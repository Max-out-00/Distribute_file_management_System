import os
import sqlite3
import time

import pytest

from client.delete import delete_file
from client.upload import call, upload_file
from data_node.heartbeat import HeartbeatSender
from data_node.node_server import DataNodeServer
from data_node.storage import ChunkStorage
from dashboard import app as dashboard
from metadata_server import db
from metadata_server.server import MetadataServer
from shared import constants


def test_delete_removes_metadata_and_chunks_on_every_node(tmp_path, monkeypatch):
    database = str(tmp_path / "metadata.db")
    metadata = MetadataServer("127.0.0.1", 0, database, enable_monitor=False).start()
    monkeypatch.setattr(constants, "METADATA_PORT", metadata.port)
    monkeypatch.setattr(constants, "CHUNK_SIZE", 1024)
    stores, servers, beats = [], [], []
    try:
        for i in range(3):
            storage = ChunkStorage(str(tmp_path / f"n{i}"))
            node = DataNodeServer("127.0.0.1", 0, storage)
            node.start()
            beat = HeartbeatSender(f"n{i}", "127.0.0.1", node.port, storage,
                                   "127.0.0.1", metadata.port, interval=0.1)
            beat.start()
            stores.append(storage), servers.append(node), beats.append(beat)
        time.sleep(0.5)

        source = tmp_path / "f.bin"
        source.write_bytes(os.urandom(2500))                 # 3 chunks x 3 replicas
        file_id = upload_file(str(source))
        assert sum(s.chunk_count() for s in stores) == 3 * constants.REPLICATION_FACTOR

        reply = delete_file(file_id)
        assert reply["deleted_chunks"] == 3 and reply["unreachable_replicas"] == 0
        assert all(s.chunk_count() == 0 for s in stores)
        with db.connection(database) as conn:
            for table in ("FILES", "CHUNKS", "CHUNKS_LOCATION"):
                assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
        with pytest.raises(RuntimeError):
            delete_file(file_id)                              # second delete: file not found
    finally:
        for b in beats:
            b.stop()
        for n in servers:
            n.stop()
        metadata.stop()


def test_dashboard_delete_cleans_history_after_metadata_delete(tmp_path, monkeypatch):
    database = tmp_path / "dashboard.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    with sqlite3.connect(database) as conn:
        conn.execute(
            """INSERT INTO files
               (id, file_id, metadata_file_id, filename, size, chunks, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("history-1", None, "metadata-1", "report.txt", 5, "[]", "today"),
        )

    deleted = []
    monkeypatch.setattr(
        dashboard,
        "delete_file",
        lambda file_id: deleted.append(file_id) or {
            "deleted_chunks": 1,
            "unreachable_replicas": 0,
        },
    )

    client = dashboard.app.test_client()
    assert b'data-history-id="history-1"' in client.get("/").data
    response = client.delete("/api/files/history-1")

    assert response.status_code == 200
    assert response.json == {
        "status": "ok",
        "deleted_chunks": 1,
        "unreachable_replicas": 0,
    }
    assert deleted == ["metadata-1"]
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 0


def test_dashboard_delete_keeps_history_when_metadata_is_unreachable(
    tmp_path, monkeypatch
):
    database = tmp_path / "dashboard.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    with sqlite3.connect(database) as conn:
        conn.execute(
            """INSERT INTO files
               (id, file_id, metadata_file_id, filename, size, chunks, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("history-1", None, "metadata-1", "report.txt", 5, "[]", "today"),
        )
    def unavailable(_file_id):
        raise OSError("offline")

    monkeypatch.setattr(dashboard, "delete_file", unavailable)

    response = dashboard.app.test_client().delete("/api/files/history-1")

    assert response.status_code == 503
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
