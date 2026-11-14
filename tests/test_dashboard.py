import io
import json
import sqlite3
import threading
import time
from pathlib import Path

import client.upload as client_upload
from dashboard import app as dashboard


def test_client_upload_reports_progress_after_each_chunk(monkeypatch, tmp_path):
    source = tmp_path / "source.bin"
    source.write_bytes(b"data")
    monkeypatch.setattr(
        client_upload,
        "split_file",
        lambda *_args: iter([(0, b"data", "checksum")]),
    )

    def fake_call(_host, _port, message, _payload=b"", **_kwargs):
        if message.get("type") == "PLAN_UPLOAD":
            descriptor = message["chunks"][0]
            file_id = descriptor["chunk_id"].rsplit("_", 1)[0]
            return {
                "status": "ok",
                "file_id": file_id,
                "chunks": [
                    {
                        **descriptor,
                        "nodes": [
                            {"node_id": "node-a", "host": "node-a", "port": 5001}
                        ],
                    }
                ],
            }, b""
        if message.get("cmd") == "PUT_CHUNK":
            return {"status": "ok", "checksum": "checksum"}, b""
        if message.get("type") == "CHUNK_STORED":
            return {"status": "ok"}, b""
        raise AssertionError(f"Unexpected message: {message}")

    monkeypatch.setattr(client_upload, "call", fake_call)
    progress = []

    file_id = client_upload.upload_file(
        str(source), progress_callback=lambda completed, total: progress.append((completed, total))
    )

    assert len(file_id) == 32
    assert progress == [(1, 1)]


def test_dashboard_renders_live_node_and_chunk_sections(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    with sqlite3.connect(database) as conn:
        conn.execute(
            "INSERT INTO files (file_id, filename, size, chunks, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                "file-123",
                "original-report.txt",
                4,
                json.dumps(
                    [
                        {
                            "chunk_id": "file-123_0",
                            "chunk_index": 0,
                            "nodes": [
                                {
                                    "node_id": "node-a",
                                    "host": "node-a",
                                    "port": 5001,
                                    "alive": True,
                                },
                                {
                                    "node_id": "node-b",
                                    "host": "node-b",
                                    "port": 5002,
                                    "alive": False,
                                },
                            ],
                        }
                    ]
                ),
                "2026-11-10",
            ),
        )
    monkeypatch.setattr(
        dashboard,
        "_nodes_from_metadata",
        lambda: (
            [
                {
                    "id": "node-a",
                    "host": "node-a",
                    "port": 5001,
                    "online": True,
                    "status": "Online",
                }
            ],
            None,
        ),
    )

    response = dashboard.app.test_client().get("/")

    assert b"Live nodes" in response.data
    assert b"Node Monitoring" in response.data
    assert b"Chunk placement" in response.data
    assert b"Stored on node" in response.data
    assert b"node-a" in response.data
    assert b"node-b" in response.data
    assert b"Stored" in response.data


def test_nodes_api_returns_live_status(monkeypatch):
    node = {
        "id": "node-a",
        "host": "node-a",
        "port": 5001,
        "online": True,
        "status": "Online",
    }
    monkeypatch.setattr(dashboard, "_nodes_from_metadata", lambda: ([node], None))

    response = dashboard.app.test_client().get("/api/nodes")

    assert response.status_code == 200
    assert response.json == {"nodes": [node], "error": None, "available": True}


def test_files_api_ignores_files_missing_from_metadata(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    with sqlite3.connect(database) as conn:
        conn.execute(
            "INSERT INTO files (file_id, filename, size, chunks, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("stale-file", "old.bin", 4, json.dumps([]), "2026-11-10"),
        )
    monkeypatch.setattr(
        dashboard,
        "call",
        lambda *_args, **_kwargs: ({"status": "error", "error": "file not found"}, b""),
    )

    response = dashboard.app.test_client().get("/api/files")

    assert response.status_code == 200
    assert response.json == {"files": [], "chunk_rows": []}


def test_upload_progress_tracks_distributed_chunk_processing(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    chunk_locations = [
        {
            "chunk_id": "file-123_0",
            "chunk_index": 0,
            "nodes": [
                {"node_id": "node-a", "host": "node-a", "port": 5001, "alive": True}
            ],
        }
    ]

    def fake_call(_host, _port, message, **_kwargs):
        assert message["type"] == "GET_LOCATIONS"
        return {"status": "ok", "chunks": chunk_locations}, b""

    progress_started = threading.Event()
    finish_upload = threading.Event()

    def fake_upload(_path, progress_callback):
        progress_callback(1, 2)
        progress_started.set()
        assert finish_upload.wait(timeout=2)
        return "file-123"

    monkeypatch.setattr(dashboard, "call", fake_call)
    monkeypatch.setattr(dashboard, "upload_file", fake_upload)
    client = dashboard.app.test_client()

    response = client.post(
        "/api/uploads",
        data={"file": (io.BytesIO(b"payload"), "report.txt")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 202
    job_id = response.json["job_id"]

    try:
        assert progress_started.wait(timeout=1)
        progress = client.get(f"/api/uploads/{job_id}").json
        assert progress["state"] == "running"
        assert progress["percent"] == 53
        assert progress["phase"] == "Distributing chunks (1/2)"
    finally:
        finish_upload.set()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        progress = client.get(f"/api/uploads/{job_id}").json
        if progress["state"] != "running":
            break
        time.sleep(0.01)

    assert progress["state"] == "complete"
    assert progress["percent"] == 100
    assert progress["file_id"] == "file-123"
    placements = client.get("/api/files")
    assert placements.status_code == 200
    assert placements.json["chunk_rows"][0]["node_id"] == "node-a"


def test_download_uses_uploaded_filename(tmp_path, monkeypatch):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()

    with sqlite3.connect(database) as conn:
        conn.execute(
            "INSERT INTO files (file_id, filename, size, chunks, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("file-123", "original-report.txt", 4, json.dumps([]), "2026-11-10"),
        )

    monkeypatch.setattr(
        dashboard,
        "download_file",
        lambda _file_id, path: Path(path).write_bytes(b"data"),
    )
    client = dashboard.app.test_client()

    page = client.get("/")
    assert b'data-filename="original-report.txt"' in page.data
    assert b"downloadAnchor.download = link.dataset.filename;" in page.data
    assert b"download.bin" not in page.data

    response = client.get("/download/file-123")
    assert response.headers["Content-Disposition"] == (
        'attachment; filename=original-report.txt'
    )


def _seed_file(database):
    import sqlite3
    with sqlite3.connect(database) as conn:
        conn.execute("INSERT INTO files (file_id, filename, size, chunks, created_at) VALUES (?, ?, ?, ?, datetime('now'))",
                     ("f1", "report.txt", 5, "[]"))


def test_delete_removes_file_from_cluster_and_dashboard_list(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    _seed_file(database)
    calls = []
    monkeypatch.setattr(dashboard, "delete_file",
                        lambda file_id: calls.append(file_id) or {"deleted_chunks": 3, "unreachable_replicas": 1})
    response = dashboard.app.test_client().delete("/api/files/f1")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok", "deleted_chunks": 3, "unreachable_replicas": 1}
    assert calls == ["f1"] and dashboard._files_from_db() == []


def test_delete_of_file_already_gone_from_metadata_cleans_the_list(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    _seed_file(database)

    def gone(file_id):
        raise RuntimeError("file not found")
    monkeypatch.setattr(dashboard, "delete_file", gone)
    assert dashboard.app.test_client().delete("/api/files/f1").status_code == 200
    assert dashboard._files_from_db() == []


def test_failed_delete_keeps_the_file_listed(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    _seed_file(database)

    def broken(file_id):
        raise OSError("connection refused")
    monkeypatch.setattr(dashboard, "delete_file", broken)
    response = dashboard.app.test_client().delete("/api/files/f1")
    assert response.status_code == 503 and "unreachable" in response.get_json()["error"]
    assert len(dashboard._files_from_db()) == 1


def test_file_card_has_a_delete_button(monkeypatch, tmp_path):
    database = tmp_path / "dashboard_files.db"
    monkeypatch.setattr(dashboard, "DB_PATH", database)
    dashboard.init_db()
    _seed_file(database)
    monkeypatch.setattr(dashboard, "_nodes_from_metadata", lambda: ([], None))
    html = dashboard.app.test_client().get("/").get_data(as_text=True)
    assert 'class="feature-btn danger-btn delete-action"' in html and 'data-file-id="f1"' in html
