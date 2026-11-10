import hashlib
import os
import time

from client.download import download_file
from client.upload import call, upload_file
from data_node.heartbeat import HeartbeatSender
from data_node.node_server import DataNodeServer
from data_node.storage import ChunkStorage
from metadata_server import db, queries
from metadata_server.server import MetadataServer
from shared import constants


def test_recovery_after_node_failure_and_download(tmp_path, monkeypatch):
    database = str(tmp_path / "metadata.db")
    metadata = MetadataServer(
        "127.0.0.1", 0, database, failure_timeout=0.6,
        monitor_interval=0.05, enable_monitor=True,
    ).start()
    monkeypatch.setattr(constants, "METADATA_PORT", metadata.port)
    monkeypatch.setattr(constants, "CHUNK_SIZE", 1024)
    nodes, heartbeats = [], []
    try:
        for index in range(3):
            node_id = f"node-{index}"
            storage = ChunkStorage(str(tmp_path / node_id))
            node = DataNodeServer("127.0.0.1", 0, storage)
            node.start()
            nodes.append(node)
            heartbeat = HeartbeatSender(
                node_id, "127.0.0.1", node.port, storage,
                "127.0.0.1", metadata.port, interval=0.15,
            )
            heartbeat.start()
            heartbeats.append(heartbeat)

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            response, _ = call("127.0.0.1", metadata.port, {"type": "LIST_NODES"})
            if len(response.get("nodes", [])) == 3 and all(
                node["alive"] for node in response["nodes"]
            ):
                break
            time.sleep(0.02)
        assert len(response["nodes"]) == 3
        assert all(node["alive"] for node in response["nodes"])

        source = tmp_path / "source.bin"
        original = os.urandom(2049)
        source.write_bytes(original)
        file_id = upload_file(str(source))

        heartbeats[1].stop()
        nodes[1].stop()
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            with db.connection(database) as conn:
                nodes_state = queries.list_nodes(conn)
            dead_node = next(node for node in nodes_state if node["NODE_ID"] == "node-1")
            if dead_node["STATUS"] == "DEAD":
                break
            time.sleep(0.03)
        assert dead_node["STATUS"] == "DEAD"

        with db.connection(database) as conn:
            chunks = queries.get_file_locations(conn, file_id)
        assert len(chunks) == 3
        assert all(
            len(chunk["nodes"]) == 3 and sum(node["alive"] for node in chunk["nodes"]) >= 2
            for chunk in chunks
        ), chunks

        output = tmp_path / "download.bin"
        download_file(file_id, str(output))
        assert hashlib.sha256(output.read_bytes()).digest() == hashlib.sha256(original).digest()
    finally:
        for heartbeat in heartbeats:
            heartbeat.stop()
        for node in nodes:
            node.stop()
        metadata.stop()