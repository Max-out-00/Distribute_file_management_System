from metadata_server import db, queries
from client.upload import call
from metadata_server.server import MetadataServer


def test_under_replicated_chunks_and_target_selection(tmp_path):
    database = tmp_path / "metadata.db"
    db.initialize(database)
    with db.connection(database) as conn:
        for node_id, free in (("node-a", 100), ("node-b", 200), ("node-c", 300)):
            queries.register_node(conn, node_id, "127.0.0.1", 1, free, 100)
        planned, alive = queries.create_upload_plan(
            conn, "file-1", "sample.bin", 8,
            [
                {"chunk_id": "chunk-a", "chunk_index": 0, "chunk_size": 4,
                 "checksum": "a" * 64},
                {"chunk_id": "chunk-b", "chunk_index": 1, "chunk_size": 4,
                 "checksum": "b" * 64},
            ],
            100, 2,
        )
        assert alive == 3
        assert len(planned[0]["nodes"]) == 2
        assert queries.confirm_replica(conn, "chunk-a", planned[0]["nodes"][0]["node_id"])
        for node in planned[1]["nodes"]:
            queries.mark_replica(conn, "chunk-b", node["node_id"], "STORED")
        under_replicated = queries.get_under_replicated_chunks(conn, 2)
        assert [chunk["chunk_id"] for chunk in under_replicated] == ["chunk-a"]
        assert len(under_replicated[0]["sources"]) == 1
        target = queries.pick_target_node(conn, "chunk-a")
        assert target is not None
        assert target["NODE_ID"] not in {
            node["node_id"] for node in planned[0]["nodes"]
        }
        queries.mark_replica(conn, "chunk-a", target["NODE_ID"], "PENDING")
        queries.remove_pending_replica(conn, "chunk-a", target["NODE_ID"])
        assert queries.pick_target_node(conn, "chunk-a")["NODE_ID"] == target["NODE_ID"]
        locations = queries.get_file_locations(conn, "file-1")
        assert locations[0]["chunk_id"] == "chunk-a"
        assert len(locations[0]["nodes"]) == 1
        assert len(queries.list_nodes(conn)) == 3


def test_invalid_metadata_request_returns_error_and_server_keeps_serving(tmp_path):
    server = MetadataServer(
        "127.0.0.1", 0, str(tmp_path / "metadata.db"), enable_monitor=False
    ).start()
    try:
        response, _ = call("127.0.0.1", server.port, {
            "type": "PLAN_UPLOAD", "file_name": "bad.bin", "file_size": 1,
            "chunks": [{"chunk_id": "../bad", "chunk_index": 0, "chunk_size": 1,
                        "checksum": "a" * 64}],
        })
        assert response["status"] == "error"
        assert "chunk_id" in response["error"]
        response, _ = call("127.0.0.1", server.port, {"type": "PING"})
        assert response["status"] == "ok"
    finally:
        server.stop()