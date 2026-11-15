import hashlib
import os
import time

from client.download import download_file
from client.pipeline import upload_file_parallel
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
        "127.0.0.1", 0, database, failure_timeout=4,
        monitor_interval=0.1, enable_monitor=True,
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
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with db.connection(database) as conn:
                nodes_state = queries.list_nodes(conn)
            dead_node = next(node for node in nodes_state if node["NODE_ID"] == "node-1")
            if dead_node["STATUS"] == "DEAD":
                break
            time.sleep(0.03)
        assert dead_node["STATUS"] == "DEAD"

        recovery_deadline = time.monotonic() + 5
        while time.monotonic() < recovery_deadline:
            with db.connection(database) as conn:
                chunks = queries.get_file_locations(conn, file_id)
            if chunks and all(
                sum(node["alive"] for node in chunk["nodes"]) >= constants.REPLICATION_FACTOR
                for chunk in chunks
            ):
                break
            time.sleep(0.05)
        assert len(chunks) == 3
        assert all(
            sum(node["alive"] for node in chunk["nodes"]) >= constants.REPLICATION_FACTOR
            and len({node["node_id"] for node in chunk["nodes"]}) == len(chunk["nodes"])
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


def test_adaptive_chunk_sizes_fit_capacity_and_reconstruct_in_order(tmp_path, monkeypatch):
    database = str(tmp_path / "adaptive-metadata.db")
    metadata = MetadataServer(
        "127.0.0.1", 0, database, enable_monitor=False,
    ).start()
    monkeypatch.setattr(constants, "METADATA_PORT", metadata.port)
    monkeypatch.setattr(constants, "CHUNK_SIZE", 4 * 1024 * 1024)
    monkeypatch.setattr(constants, "RESERVE_BYTES", 0)
    capacity = 3 * 1024 * 1024
    nodes, heartbeats, storage_by_id = [], [], {}
    try:
        for index in range(4):
            node_id = f"adaptive-{index}"
            storage = ChunkStorage(
                str(tmp_path / node_id), capacity_bytes=capacity,
            )
            node = DataNodeServer("127.0.0.1", 0, storage)
            node.start()
            nodes.append(node)
            storage_by_id[node_id] = storage
            heartbeat = HeartbeatSender(
                node_id, "127.0.0.1", node.port, storage,
                "127.0.0.1", metadata.port, interval=0.15,
            )
            heartbeat.start()
            heartbeats.append(heartbeat)

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            response, _ = call("127.0.0.1", metadata.port, {"type": "LIST_NODES"})
            if len(response.get("nodes", [])) == 4 and all(
                node["alive"] for node in response["nodes"]
            ):
                break
            time.sleep(0.02)
        assert len(response["nodes"]) == 4
        assert all(node["alive"] for node in response["nodes"])

        source = tmp_path / "adaptive-source.bin"
        original = os.urandom(6 * 1024 * 1024)
        source.write_bytes(original)
        file_id = upload_file_parallel(str(source), workers=2)
        locations, _ = call(
            "127.0.0.1", metadata.port,
            {"type": "GET_LOCATIONS", "file_id": file_id},
        )
        chunks = locations["chunks"]
        assert [chunk["chunk_index"] for chunk in chunks] == [0, 1]
        assert [chunk["chunk_size"] for chunk in chunks] == [capacity, capacity]
        assert all(len(chunk["nodes"]) == constants.REPLICATION_FACTOR for chunk in chunks)
        assert len({
            node["node_id"] for chunk in chunks for node in chunk["nodes"]
        }) == 4

        output = tmp_path / "adaptive-output.bin"
        download_file(file_id, str(output))
        assert output.read_bytes() == original
    finally:
        for heartbeat in heartbeats:
            heartbeat.stop()
        for node in nodes:
            node.stop()
        metadata.stop()


def test_corrupt_replica_falls_back_and_is_replicated_again(tmp_path, monkeypatch):
    database = str(tmp_path / "corrupt-metadata.db")
    metadata = MetadataServer(
        "127.0.0.1", 0, database, monitor_interval=0.1,
        enable_monitor=True,
    ).start()
    monkeypatch.setattr(constants, "METADATA_PORT", metadata.port)
    monkeypatch.setattr(constants, "CHUNK_SIZE", 1024)
    monkeypatch.setattr(constants, "RESERVE_BYTES", 0)
    nodes, heartbeats, storage_by_id = [], [], {}
    try:
        for index in range(3):
            node_id = f"integrity-{index}"
            storage = ChunkStorage(str(tmp_path / node_id), capacity_bytes=1024 * 1024)
            node = DataNodeServer("127.0.0.1", 0, storage)
            node.start()
            nodes.append(node)
            storage_by_id[node_id] = storage
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
        assert all(node["alive"] for node in response["nodes"])

        source = tmp_path / "integrity-source.bin"
        original = b"verified chunk contents"
        source.write_bytes(original)
        file_id = upload_file(str(source))
        locations, _ = call(
            "127.0.0.1", metadata.port,
            {"type": "GET_LOCATIONS", "file_id": file_id},
        )
        chunk = locations["chunks"][0]
        corrupt_node = chunk["nodes"][0]["node_id"]
        storage_by_id[corrupt_node].write_chunk(chunk["chunk_id"], b"corrupted")

        output = tmp_path / "integrity-output.bin"
        download_file(file_id, str(output))
        assert output.read_bytes() == original

        repair_deadline = time.monotonic() + 4
        while time.monotonic() < repair_deadline:
            repaired, _ = call(
                "127.0.0.1", metadata.port,
                {"type": "GET_LOCATIONS", "file_id": file_id},
            )
            repaired_nodes = repaired["chunks"][0]["nodes"]
            if len(repaired_nodes) >= constants.REPLICATION_FACTOR:
                break
            time.sleep(0.03)
        assert len(repaired_nodes) >= constants.REPLICATION_FACTOR
        assert corrupt_node not in {node["node_id"] for node in repaired_nodes}

        verified_output = tmp_path / "integrity-verified.bin"
        download_file(file_id, str(verified_output))
        assert verified_output.read_bytes() == original
    finally:
        for heartbeat in heartbeats:
            heartbeat.stop()
        for node in nodes:
            node.stop()
        metadata.stop()