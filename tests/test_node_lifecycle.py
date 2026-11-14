import os
import socket
import threading
import time

import pytest

from data_node import dirlock
from data_node.node_server import DataNodeServer, request
from data_node.storage import ChunkStorage
from metadata_server import db, queries
from shared import constants
from shared.message import recv_msg


def _setup_db(tmp_path):
    path = str(tmp_path / "m.db")
    db.initialize(path)
    return path


def _plan(conn):
    now = time.time()
    for node in ("n1", "n2", "n3"):
        queries.register_node(conn, node, "127.0.0.1", 5000, 10 ** 9, now)
    queries.create_upload_plan(conn, "file-1", "a.txt", 8,
                               [{"chunk_id": "c1", "chunk_index": 0, "chunk_size": 8, "checksum": "a" * 64}],
                               now, 3)
    queries.confirm_replica(conn, "c1", "n1")           # n1 STORED, n2/n3 still PENDING


def test_reconcile_finds_orphans_and_drops_missing_replicas(tmp_path):
    path = _setup_db(tmp_path)
    with db.connection(path) as conn:
        _plan(conn)
        orphans, missing = queries.reconcile_node(conn, "n1", ["c1", "ghost"])
        assert orphans == ["ghost"] and missing == []          # c1 known, 'ghost' has no row
        orphans, missing = queries.reconcile_node(conn, "n2", [])
        assert missing == []                                   # PENDING rows are never touched
        orphans, missing = queries.reconcile_node(conn, "n1", [])
        assert missing == ["c1"]                               # STORED row but file is gone
        left = conn.execute("SELECT COUNT(*) FROM CHUNKS_LOCATION WHERE NODE_ID='n1'").fetchone()[0]
        assert left == 0


def test_leave_node_marks_dead_immediately(tmp_path):
    path = _setup_db(tmp_path)
    with db.connection(path) as conn:
        _plan(conn)
        assert queries.leave_node(conn, "n1") is True
        assert queries.leave_node(conn, "n1") is False          # already dead
        status = conn.execute("SELECT STATUS FROM NODES WHERE NODE_ID='n1'").fetchone()[0]
        assert status == "DEAD"


def test_recover_removes_only_incomplete_writes(tmp_path):
    storage = ChunkStorage(str(tmp_path / "s"))
    storage.write_chunk("good", b"data")
    with open(os.path.join(storage.root, "half.chunk.tmp"), "wb") as f:
        f.write(b"partial")
    assert storage.recover() == 1
    assert storage.list_chunks() == ["good"]


def test_orphan_gc_respects_grace_period(tmp_path):
    storage = ChunkStorage(str(tmp_path / "s"))
    storage.write_chunk("fresh", b"x")
    assert storage.remove_if_older("fresh", 60) is False        # just written: keep (maybe in flight)
    assert storage.remove_if_older("fresh", 0) is True
    assert storage.remove_if_older("fresh", 0) is False         # already gone


def test_directory_lock_rejects_second_holder_and_frees_on_close(tmp_path):
    first = dirlock.acquire(str(tmp_path / "d"))
    with pytest.raises(RuntimeError):
        dirlock.acquire(str(tmp_path / "d"))
    first.close()
    dirlock.acquire(str(tmp_path / "d")).close()                # free again after release


def test_node_rejects_with_busy_when_pool_and_queue_are_full(tmp_path):
    server = DataNodeServer("127.0.0.1", 0, ChunkStorage(str(tmp_path / "s")), workers=1, max_pending=1)
    release = threading.Event()
    server._handle = lambda conn, addr: (release.wait(3), conn.close())    # hold the only worker
    server.start()
    try:
        held = []
        for _ in range(2):                                      # one in service, one queued
            held.append(socket.create_connection(("127.0.0.1", server.port)))
            time.sleep(0.2)
        third = socket.create_connection(("127.0.0.1", server.port))
        third.settimeout(3)
        reply, _ = recv_msg(third)
        assert reply["status"] == "error" and "busy" in reply["error"] and server.rejected == 1
    finally:
        release.set()
        server.stop()


def test_node_refuses_chunk_when_disk_reserve_would_be_violated(tmp_path, monkeypatch):
    storage = ChunkStorage(str(tmp_path / "s"))
    server = DataNodeServer("127.0.0.1", 0, storage)
    server.start()
    try:
        data = b"hello"
        header = {"cmd": "PUT_CHUNK", "chunk_id": "c1", "checksum": storage.checksum(data)}
        ok, _ = request("127.0.0.1", server.port, dict(header), data)
        assert ok["status"] == "ok"
        monkeypatch.setattr(constants, "MIN_FREE_BYTES", 1 << 62)   # pretend the disk is almost full
        full, _ = request("127.0.0.1", server.port, dict(header, chunk_id="c2"), data)
        assert full["status"] == "error" and "disk space" in full["error"]
        assert storage.list_chunks() == ["c1"]
    finally:
        server.stop()
