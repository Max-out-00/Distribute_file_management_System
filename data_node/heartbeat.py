"""Registers with the metadata server and sends periodic heartbeats."""
import logging
import threading

from .node_server import request

log = logging.getLogger("data_node.heartbeat")


class HeartbeatSender:
    def __init__(self, node_id, host, port, storage, meta_host, meta_port, interval=3.0):
        self.node_id, self.host, self.port = node_id, host, port
        self.storage = storage
        self.meta_host, self.meta_port = meta_host, meta_port
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None

    def _payload(self, cmd):
        return {
            "type": "REGISTER_NODE" if cmd == "REGISTER" else "HEARTBEAT",
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "free_bytes": self.storage.free_space(),
            "chunk_ids": self.storage.list_chunks() if cmd == "REGISTER" else None,
        }

    def _send(self, cmd):
        resp, _ = request(self.meta_host, self.meta_port, self._payload(cmd), timeout=5)
        return resp.get("status") == "ok"

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(1, self.interval + 1))

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