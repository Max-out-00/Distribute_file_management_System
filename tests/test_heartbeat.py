import time

from client.upload import call
from metadata_server.server import MetadataServer

def test_silent_node_transitions_to_dead(tmp_path):
    server = MetadataServer(
        "127.0.0.1", 0, str(tmp_path / "metadata.db"),
        failure_timeout=0.15, monitor_interval=0.03,
    ).start()
    try:
        response, _ = call("127.0.0.1", server.port, {
            "type": "HEARTBEAT", "node_id": "node-a", "host": "127.0.0.1",
            "port": 1, "free_bytes": 100,
        })
        assert response["status"] == "ok"

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            response, _ = call("127.0.0.1", server.port, {"type": "LIST_NODES"})
            if response["nodes"][0]["alive"] is False:
                break
            time.sleep(0.02)
        assert response["nodes"][0]["alive"] is False
    finally:
        server.stop()
