"""Registers with the metadata server and sends periodic heartbeats."""
import logging
import threading
import time

from .node_server import request

log = logging.getLogger("data_node.heartbeat")


class HeartbeatSender:
    def __init__(self, node_id, host, port, storage, meta_host, meta_port, interval=3.0):
        self.node_id, self.host, self.port = node_id, host, port
        self.storage = storage
        self.meta_host, self.meta_port = meta_host, meta_port
        self.interval = interval
        self._stop = threading.Event()

    def _payload(self, cmd):
        return {
            "cmd": cmd,
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "free_space": self.storage.free_space(),
            "chunk_count": self.storage.chunk_count(),
            "chunk_ids": self.storage.list_chunks() if cmd == "REGISTER" else None,
            "timestamp": time.time(),
        }

    def _send(self, cmd):
        resp, _ = request(self.meta_host, self.meta_port, self._payload(cmd), timeout=5)
        return resp.get("status") == "ok"

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._stop.set()

    def _run(self):
        # Register (retry until metadata server is up)
        while not self._stop.is_set():
            try:
                if self._send("REGISTER"):
                    log.info("registered with metadata server")
                    break
            except OSError as e:
                log.warning("metadata server unreachable (%s), retrying", e)
            self._stop.wait(self.interval)

        # Heartbeat loop; a missed beat is simply retried at the next interval
        while not self._stop.wait(self.interval):
            try:
                self._send("HEARTBEAT")
            except OSError as e:
                log.warning("heartbeat failed: %s", e)