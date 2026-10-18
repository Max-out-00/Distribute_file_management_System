"""Entry point of a data node.

Run from the project root, e.g.:
    python -m data_node.init --node-id node1 --port 5001 --storage-dir data_node/chunks/node1
    python -m data_node.init --node-id node2 --port 5002 --storage-dir data_node/chunks/node2
    python -m data_node.init --node-id node3 --port 5003 --storage-dir data_node/chunks/node3
"""
import argparse
import logging
import time

from .heartbeat import HeartbeatSender
from .node_server import DataNodeServer
from .storage import ChunkStorage


def main():
    p = argparse.ArgumentParser(description="Distributed FS data node")
    p.add_argument("--node-id", required=True)
    p.add_argument("--host", default="127.0.0.1", help="address this node advertises/binds to")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--storage-dir", default=None)
    p.add_argument("--metadata-host", default="127.0.0.1")
    p.add_argument("--metadata-port", type=int, default=5000)
    p.add_argument("--heartbeat-interval", type=float, default=3.0)
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s [{a.node_id}] %(name)s: %(message)s")

    storage = ChunkStorage(a.storage_dir or f"data_node/chunks/{a.node_id}")
    server = DataNodeServer(a.host, a.port, storage)
    heartbeat = HeartbeatSender(a.node_id, a.host, a.port, storage,
                                a.metadata_host, a.metadata_port, a.heartbeat_interval)
    server.start()
    heartbeat.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        heartbeat.stop()
        server.stop()


if __name__ == "__main__":
    main()