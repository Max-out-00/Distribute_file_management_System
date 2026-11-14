import os
import time

import pytest

from client.delete import delete_file
from client.upload import call, upload_file
from data_node.heartbeat import HeartbeatSender
from data_node.node_server import DataNodeServer
from data_node.storage import ChunkStorage
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
        assert all(s.chunk_count() == 3 for s in stores)

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
