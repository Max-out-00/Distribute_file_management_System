from client.upload import call
from data_node.node_server import DataNodeServer
from data_node.storage import ChunkStorage
from metadata_server import db, queries
from metadata_server.server import MetadataServer
from scripts.demo import print_placement, wait_for_nodes, wait_for_recovery
from scripts.healthcheck import check


def test_healthchecks_and_demo_helpers(tmp_path, capsys):
    database = str(tmp_path / "metadata.db")
    metadata = MetadataServer(
        "127.0.0.1", 0, database, enable_monitor=False
    ).start()
    node = DataNodeServer(
        "127.0.0.1", 0, ChunkStorage(str(tmp_path / "node"))
    )
    node.start()
    try:
        check("metadata", "127.0.0.1", metadata.port)
        check("node", "127.0.0.1", node.port)
        response, _ = call("127.0.0.1", metadata.port, {
            "type": "REGISTER_NODE", "node_id": "node1", "host": "127.0.0.1",
            "port": node.port, "free_bytes": 100,
        })
        assert response["status"] == "ok"
        wait_for_nodes("127.0.0.1", metadata.port, 1, 1)

        with db.connection(database) as conn:
            chunks, _ = queries.create_upload_plan(
                conn, "file-a", "sample.bin", 1,
                [{"chunk_id": "chunk-a", "chunk_index": 0, "chunk_size": 1,
                  "checksum": "a" * 64}],
                100, 1,
            )
            queries.mark_replica(conn, "chunk-a", "node1", "STORED")
        print_placement(database, "file-a")
        assert "chunk-a: node1 (alive)" in capsys.readouterr().out
        wait_for_recovery(database, "file-a", 1, 1)
        assert chunks[0]["chunk_id"] == "chunk-a"
    finally:
        node.stop()
        metadata.stop()
