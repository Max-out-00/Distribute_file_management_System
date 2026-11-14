"""Entry point of a data node.

Run from the project root, e.g.:
    python -m data_node.init --node-id node1 --port 5001 --storage-dir data_node/chunks/node1
    python -m data_node.init --node-id node2 --port 5002 --storage-dir data_node/chunks/node2
    python -m data_node.init --node-id node3 --port 5003 --storage-dir data_node/chunks/node3
"""
import argparse
import logging
import signal
import sys
import threading

from shared import constants
from . import dirlock
from .heartbeat import HeartbeatSender
from .node_server import DataNodeServer
from .storage import ChunkStorage


def main():
    p = argparse.ArgumentParser(description="Distributed FS data node")
    p.add_argument("--node-id", required=True)
    p.add_argument("--bind-host", default=constants.BIND_HOST)
    p.add_argument("--advertise-host", default=None)
    p.add_argument("--host", dest="legacy_host", default=None, help=argparse.SUPPRESS)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--storage-dir", default=None)
    p.add_argument("--meta-host", "--metadata-host", default=constants.METADATA_HOST)
    p.add_argument("--meta-port", "--metadata-port", type=int,
                   default=constants.METADATA_PORT)
    p.add_argument("--heartbeat-interval", type=float, default=3.0)
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s [{a.node_id}] %(name)s: %(message)s")

    advertise_host = a.advertise_host or a.legacy_host or constants.HOST
    storage_dir = a.storage_dir or f"data_node/chunks/{a.node_id}"
    try:
        lock = dirlock.acquire(storage_dir)          # one process per storage directory
    except RuntimeError as error:
        sys.exit(f"Error: {error}")
    storage = ChunkStorage(storage_dir)
    leftovers = storage.recover()                    # crash recovery: drop half-written files
    if leftovers:
        logging.getLogger("data_node").warning("removed %d incomplete write(s) left by a crash", leftovers)
    server = DataNodeServer(a.bind_host, a.port, storage)
    heartbeat = HeartbeatSender(a.node_id, advertise_host, a.port, storage,
                                a.meta_host, a.meta_port, a.heartbeat_interval)
    server.start()
    heartbeat.start()

    stop = threading.Event()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):          # Ctrl+C, kill, Ctrl+Break
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: stop.set())
    while not stop.wait(0.5):
        pass

    log = logging.getLogger("data_node")
    log.info("shutdown signal received: leaving the cluster gracefully")
    heartbeat.stop()                                 # no more heartbeats (cannot resurrect us)
    heartbeat.leave()                                # metadata marks us DEAD now, not after 10 s
    server.stop()                                    # stop accepting, finish in-flight requests
    lock.close()


if __name__ == "__main__":
    main()