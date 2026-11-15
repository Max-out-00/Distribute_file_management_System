import hashlib
import os
import time

import pytest

from client import download, upload
from client.chunker import choose_chunk_size
from data_node.node_server import DataNodeServer, request
from data_node.storage import ChunkStorage
from metadata_server import db, queries
from metadata_server.handlers import handle_message
from metadata_server import placement as placement_module
from metadata_server.placement import MostFreePlacement
from shared import constants


def test_storage_enforces_capacity_counts_chunks_and_rewrites(tmp_path):
    storage = ChunkStorage(tmp_path, capacity_bytes=10)
    storage.write_chunk("a", b"12345678")
    assert storage.used_bytes() == 8
    assert storage.free_space() == 2
    with pytest.raises(OSError, match="disk full"):
        storage.write_chunk("b", b"abcdefgh")
    assert storage.list_chunks() == ["a"]
    assert not list(tmp_path.glob("*.tmp"))
    storage.write_chunk("a", b"1234567890")
    assert storage.used_bytes() == 10
    assert storage.free_space() == 0
    with pytest.raises(OSError, match="disk full"):
        storage.write_chunk("a", b"12345678901")
    assert storage.read_chunk("a") == b"1234567890"


def test_storage_ignores_temporary_files_and_env_capacity(tmp_path, monkeypatch):
    monkeypatch.setenv("NODE_CAPACITY_BYTES", "12")
    storage = ChunkStorage(tmp_path)
    (tmp_path / "broken.chunk.tmp").write_bytes(b"ignored")
    assert storage.used_bytes() == 0
    assert storage.free_space() == min(12, __import__("shutil").disk_usage(tmp_path).free)
    storage.write_chunk("one", b"12345678")
    assert storage.free_space() == 4


def test_node_put_chunk_returns_disk_full_without_partial_file(tmp_path):
    storage = ChunkStorage(tmp_path / "chunks", capacity_bytes=4)
    server = DataNodeServer("127.0.0.1", 0, storage)
    server.start()
    try:
        data = b"12345"
        response, _ = request("127.0.0.1", server.port, {
            "cmd": "PUT_CHUNK", "chunk_id": "too-big",
            "checksum": storage.checksum(data),
        }, data)
        assert response["status"] == "error"
        assert "disk full" in response["error"].lower()
        assert storage.list_chunks() == []
        assert not list((tmp_path / "chunks").glob("*.tmp"))
    finally:
        server.stop()


def test_cli_capacity_overrides_env(monkeypatch):
    from data_node import init as node_init
    captured = {}
    class Parser:
        def __init__(self, **_kwargs): pass
        def add_argument(self, *_args, **_kwargs): pass
        def parse_args(self):
            class Args:
                node_id="n"; bind_host="127.0.0.1"; advertise_host="n"; legacy_host=None
                port=1; storage_dir="unused"; capacity=5; meta_host="m"; meta_port=9
                heartbeat_interval=1
            return Args()
    class FakeLock:
        def close(self): pass
    class StopEvent:
        def wait(self, _delay): return True
    monkeypatch.setattr(node_init.argparse, "ArgumentParser", Parser)
    monkeypatch.setattr(node_init.dirlock, "acquire", lambda _path: FakeLock())
    def fake_storage(_path, capacity_bytes=None):
        captured["capacity"] = capacity_bytes
        return type("S", (), {"recover":lambda _s:0})()
    monkeypatch.setattr(node_init, "ChunkStorage", fake_storage)
    monkeypatch.setattr(node_init, "DataNodeServer", lambda *_a: type("N", (), {"start":lambda _s:None,"stop":lambda _s:None})())
    monkeypatch.setattr(node_init, "HeartbeatSender", lambda *_a: type("H", (), {"start":lambda _s:None,"stop":lambda _s:None,"leave":lambda _s:None})())
    monkeypatch.setattr(node_init.threading, "Event", StopEvent)
    monkeypatch.setattr(node_init.signal, "signal", lambda *_a:None)
    monkeypatch.setenv("NODE_CAPACITY_BYTES", "99")
    node_init.main()
    assert captured["capacity"] == 5


def _db_with_nodes(tmp_path, free_values, monkeypatch, reserve=0):
    path = str(tmp_path / "meta.sqlite")
    db.initialize(path)
    monkeypatch.setattr(constants, "RESERVE_BYTES", reserve, raising=False)
    with db.connection(path) as conn:
        for i, free in enumerate(free_values):
            queries.register_node(conn, f"n{i}", "host", 1000 + i, free, time.time())
    return path


def test_initialize_recreates_empty_integer_file_id_schema(tmp_path):
    import sqlite3

    database = tmp_path / "metadata.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """CREATE TABLE FILES (
                FILE_ID INTEGER PRIMARY KEY NOT NULL,
                FILE_NAME TEXT NOT NULL,
                SIZE INTEGER NOT NULL,
                CREATED_AT TEXT NOT NULL,
                STATUS TEXT
            )"""
        )

    db.initialize(str(database))

    with db.connection(str(database)) as conn:
        file_id = next(
            row for row in conn.execute("PRAGMA table_info(FILES)")
            if row["name"] == "FILE_ID"
        )
        assert file_id["type"] == "TEXT"
        assert file_id["pk"] == 1


def test_initialize_preserves_populated_integer_file_id_schema(tmp_path):
    import sqlite3

    database = tmp_path / "metadata.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """CREATE TABLE FILES (
                FILE_ID INTEGER PRIMARY KEY NOT NULL,
                FILE_NAME TEXT NOT NULL,
                SIZE INTEGER NOT NULL,
                CREATED_AT TEXT NOT NULL,
                STATUS TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO FILES VALUES (1, 'sample.bin', 1, 'now', 'COMPLETE')"
        )

    with pytest.raises(
        RuntimeError,
        match="legacy metadata tables contain data and need migration: FILES",
    ):
        db.initialize(str(database))

    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT FILE_ID FROM FILES").fetchone() == (1,)


def test_placement_obeys_reserve_capacity_and_distinct_replicas(tmp_path, monkeypatch):
    path = _db_with_nodes(tmp_path, [10, 10, 9], monkeypatch, reserve=2)
    with db.connection(path) as conn:
        planned, _ = queries.create_upload_plan(
            conn, "f1", "x", 6,
            [{"chunk_id":"c0", "chunk_index":0, "chunk_size":3, "checksum":"a"*64}],
            time.time(), constants.REPLICATION_FACTOR, MostFreePlacement())
        assert len(planned[0]["nodes"]) == constants.REPLICATION_FACTOR
        assert len({node["node_id"] for node in planned[0]["nodes"]}) == constants.REPLICATION_FACTOR


def test_choose_chunk_size_uses_replica_capacity_and_reserve():
    nodes = [
        {"alive": True, "free_bytes": 20},
        {"alive": True, "free_bytes": 12},
        {"alive": True, "free_bytes": 12},
        {"alive": False, "free_bytes": 5000},
    ]
    assert choose_chunk_size(
        file_size=100,
        nodes=nodes,
        replication_factor=2,
        reserve_bytes=2,
        max_chunk_size=16,
    ) == 10


def test_choose_chunk_size_rejects_insufficient_replica_capacity():
    with pytest.raises(ValueError, match="2 replicas required"):
        choose_chunk_size(
            file_size=10,
            nodes=[{"alive": True, "free_bytes": 100}],
            replication_factor=2,
            reserve_bytes=0,
            max_chunk_size=4,
        )


def test_choose_chunk_size_rejects_more_than_maximum_chunk_count():
    with pytest.raises(ValueError, match="minimum needed to stay within"):
        choose_chunk_size(
            file_size=1_000_001,
            nodes=[
                {"alive": True, "free_bytes": 99_999},
                {"alive": True, "free_bytes": 99_999},
            ],
            replication_factor=2,
            reserve_bytes=0,
            max_chunk_size=4,
            max_chunks=10,
        )


def test_plan_upload_retries_with_smaller_chunks_after_capacity_race(tmp_path, monkeypatch):
    source = tmp_path / "payload.bin"
    source.write_bytes(b"abcdefgh")
    monkeypatch.setattr(constants, "CHUNK_SIZE", 4)
    monkeypatch.setattr(constants, "REPLICATION_FACTOR", 2)
    monkeypatch.setattr(constants, "RESERVE_BYTES", 0)
    messages = []

    def fake_request(_host, _port, message):
        if message["type"] == "LIST_NODES":
            return {"status": "ok", "nodes": [
                {"node_id": "n1", "alive": True, "free_bytes": 4},
                {"node_id": "n2", "alive": True, "free_bytes": 4},
            ]}, b""
        messages.append(message)
        if len(messages) == 1:
            return {
                "status": "error",
                "error": "chunk requires 2 replicas; only 1 eligible ALIVE nodes",
            }, b""
        return {
            "status": "ok",
            "file_id": "metadata-file",
            "chunks": [
                {**descriptor, "nodes": []}
                for descriptor in message["chunks"]
            ],
        }, b""

    plan = upload.plan_upload(str(source), upload_id="upload", request=fake_request)
    assert [item["chunk_size"] for item in messages[0]["chunks"]] == [4, 4]
    assert [item["chunk_size"] for item in messages[1]["chunks"]] == [2, 2, 2, 2]
    assert plan["chunk_size"] == 2
    assert plan["file_id"] == "metadata-file"


def test_placement_rejects_full_node_or_reserve_shortfall_without_file_row(tmp_path, monkeypatch):
    path = _db_with_nodes(tmp_path, [3, 100, 0], monkeypatch, reserve=4)
    with db.connection(path) as conn:
        with pytest.raises(ValueError, match="chunk c0"):
            queries.create_upload_plan(
                conn, "f1", "x", 2,
                [{"chunk_id":"c0", "chunk_index":0, "chunk_size":2, "checksum":"a"*64}],
                time.time(), constants.REPLICATION_FACTOR)
        assert conn.execute("SELECT 1 FROM FILES WHERE FILE_ID='f1'").fetchone() is None


def test_confirm_only_marks_file_complete_after_required_replicas(tmp_path, monkeypatch):
    path = _db_with_nodes(tmp_path, [100] * 3, monkeypatch, reserve=0)
    with db.connection(path) as conn:
        planned, _ = queries.create_upload_plan(
            conn, "f1", "x", 1,
            [{"chunk_id":"c0", "chunk_index":0, "chunk_size":1, "checksum":"a"*64}],
            time.time(), constants.REPLICATION_FACTOR)
        ids = [n["node_id"] for n in planned[0]["nodes"]]
        assert queries.confirm_replica(conn, "c0", ids[0])
        assert conn.execute("SELECT STATUS FROM FILES WHERE FILE_ID='f1'").fetchone()[0] == "UPLOADING"
        assert queries.confirm_replica(conn, "c0", ids[1])
        assert conn.execute("SELECT STATUS FROM FILES WHERE FILE_ID='f1'").fetchone()[0] == "COMPLETE"
        assert queries.get_file_locations(conn, "f1")


def test_empty_file_is_complete_without_data_nodes(tmp_path):
    path = str(tmp_path / "empty.sqlite")
    db.initialize(path)
    with db.connection(path) as conn:
        planned, alive = queries.create_upload_plan(
            conn, "empty-file", "empty.bin", 0, [], time.time(),
            constants.REPLICATION_FACTOR,
        )
        assert planned == []
        assert alive == 0
        assert conn.execute(
            "SELECT STATUS FROM FILES WHERE FILE_ID='empty-file'"
        ).fetchone()[0] == "COMPLETE"
        assert queries.get_file_locations(conn, "empty-file") == []


def test_corrupt_replica_report_removes_location(tmp_path, monkeypatch):
    path = _db_with_nodes(tmp_path, [100, 100, 100], monkeypatch, reserve=0)
    with db.connection(path) as conn:
        planned, _ = queries.create_upload_plan(
            conn, "file", "x", 1,
            [{"chunk_id": "c0", "chunk_index": 0, "chunk_size": 1,
              "checksum": "a" * 64}],
            time.time(), constants.REPLICATION_FACTOR,
        )
        replica_ids = [node["node_id"] for node in planned[0]["nodes"]]
        for node_id in replica_ids:
            assert queries.confirm_replica(conn, "c0", node_id)

    response = handle_message({
        "type": "REPORT_CORRUPT", "chunk_id": "c0", "node_id": replica_ids[0],
    }, path)
    assert response == {"status": "ok", "removed": True}
    with db.connection(path) as conn:
        chunk = queries.get_file_locations(conn, "file")[0]
        assert [node["node_id"] for node in chunk["nodes"]] == [replica_ids[1]]


def test_download_skips_bad_checksum_and_uses_fallback(monkeypatch):
    data = b"correct"
    nodes = [
        {"node_id":"bad", "host":"bad", "port":1, "alive":True},
        {"node_id":"good", "host":"good", "port":2, "alive":True},
    ]
    calls = []
    reports = []
    def fake_call(host, _port, message):
        if message.get("type") == "REPORT_CORRUPT":
            reports.append((message["chunk_id"], message["node_id"]))
            return {"status": "ok", "removed": True}, b""
        calls.append(host)
        if host == "bad": return {"status":"ok"}, b"corrupt"
        return {"status":"ok"}, data
    monkeypatch.setattr(download, "call", fake_call)
    assert download.fetch_chunk({"chunk_id":"c", "chunk_index":0,
                                 "checksum":hashlib.sha256(data).hexdigest(), "nodes":nodes}) == data
    assert calls == ["bad", "good"]
    assert reports == [("c", "bad")]


def test_upload_requires_all_configured_replicas(monkeypatch, tmp_path):
    source = tmp_path / "payload.bin"
    source.write_bytes(b"x")
    monkeypatch.setattr(upload.constants, "CHUNK_SIZE", 10)
    monkeypatch.setattr(upload.constants, "RESERVE_BYTES", 0)
    monkeypatch.setattr(upload, "split_file", lambda *_: iter([(0,b"x",hashlib.sha256(b"x").hexdigest())]))
    monkeypatch.setattr(upload.constants, "REPLICATION_FACTOR", 2)
    def fake_call(_host, _port, msg, payload=b"", **_kw):
        if msg.get("type") == "LIST_NODES":
            return {"status":"ok", "nodes":[
                {"node_id":"n1", "host":"n1", "port":1, "alive":True, "free_bytes":100},
                {"node_id":"n2", "host":"n2", "port":2, "alive":True, "free_bytes":100},
            ]}, b""
        if msg.get("type") == "PLAN_UPLOAD":
            return {"status":"ok", "file_id":"f", "chunks":[{**msg["chunks"][0], "nodes":[
                {"node_id":"n1","host":"n1","port":1}, {"node_id":"n2","host":"n2","port":2}]}]}, b""
        if msg.get("cmd") == "PUT_CHUNK":
            return ({"status":"error","error":"disk full"} if msg["chunk_id"].endswith("_0") and msg.get("force") else {"status":"ok","checksum":hashlib.sha256(payload).hexdigest()}), b""
        if msg.get("type") == "CHUNK_STORED":
            return {"status":"ok"}, b""
        raise AssertionError(msg)
    # Make one planned node fail on its own endpoint.
    original_call = fake_call
    def one_full(_host, port, msg, payload=b"", **kw):
        if msg.get("cmd") == "PUT_CHUNK" and port == 2:
            return {"status":"error","error":"disk full"}, b""
        return original_call(_host, port, msg, payload, **kw)
    monkeypatch.setattr(upload, "call", one_full)
    with pytest.raises(RuntimeError, match="1 of 2 required replicas"):
        upload.upload_file(str(source))
