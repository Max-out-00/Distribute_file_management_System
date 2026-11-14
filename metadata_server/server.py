import argparse
import logging
import os
import queue
import signal
import socket
import threading

from shared import constants
from shared.message import recv_msg, send_msg
from failure_detector.worker_pool import WorkerPool
from . import db, handlers

log = logging.getLogger("metadata_server.server")


class MetadataServer:
    def __init__(self, host=constants.BIND_HOST, port=constants.METADATA_PORT,
                 db_path=None, enable_monitor=True,
                 failure_timeout=None, monitor_interval=2,
                 workers=constants.REQUEST_WORKERS,
                 max_pending=constants.REQUEST_QUEUE_SIZE):
        self.host, self.port = host, port
        self.db_path = db_path or os.environ.get(
            "METADATA_DB", "metadata_server/metadata.db"
        )
        self.enable_monitor = enable_monitor
        self.failure_timeout, self.monitor_interval = failure_timeout, monitor_interval
        self._stop = threading.Event()
        self._sock = None
        self._thread = None
        self._monitor = None
        # Bounded FIFO of accepted connections, drained by a fixed pool of workers.
        # Replaces thread-per-connection: threads are capped and overload is rejected.
        self._requests = queue.Queue(maxsize=max_pending)
        self._pool = WorkerPool(self._requests, self._serve, workers, name="request-worker")
        self.rejected = 0

    def start(self):
        db.initialize(self.db_path)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen(64)
        listener.settimeout(0.2)
        self._sock = listener
        self._pool.start()
        self.port = listener.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        if self.enable_monitor:
            from failure_detector.monitor import FailureMonitor
            self._monitor = FailureMonitor(
                self.db_path, failure_timeout=self.failure_timeout,
                interval=self.monitor_interval,
            )
            self._monitor.start()
        log.info("metadata server listening on %s:%d", self.host, self.port)
        return self

    def stop(self):
        self._stop.set()
        self._pool.stop()
        if self._monitor:
            self._monitor.stop()
        if self._sock:
            self._sock.close()
        if self._thread:
            self._thread.join(timeout=2)
        if self._monitor:
            self._monitor.join(timeout=2)

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, address = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                self._requests.put_nowait((conn, address))   # FIFO: first accepted, first served
            except queue.Full:
                self.rejected += 1
                self._reject(conn)

    def _reject(self, conn):
        try:
            conn.settimeout(1)
            send_msg(conn, {"status": "error", "error": "server busy, retry later"})
        except OSError:
            pass
        finally:
            conn.close()

    def _serve(self, item):
        self._handle(*item)

    def _handle(self, conn, address):
        conn.settimeout(10)
        try:
            message, _ = recv_msg(conn)
            response = handlers.handle_message(message, self.db_path)
        except Exception as error:
            log.warning("request from %s failed: %s", address, error)
            response = {"status": "error", "error": str(error)}
        try:
            send_msg(conn, response)
        except OSError as error:
            log.debug("could not reply to %s: %s", address, error)
        finally:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="Metadata server")
    parser.add_argument("--bind-host", default=constants.BIND_HOST)
    parser.add_argument("--port", type=int, default=constants.METADATA_PORT)
    parser.add_argument("--db", default=os.environ.get(
        "METADATA_DB", "metadata_server/metadata.db"
    ))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    server = MetadataServer(args.bind_host, args.port, args.db).start()
    stop = threading.Event()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: stop.set())
    while not stop.wait(0.5):
        pass
    logging.getLogger(__name__).info("shutdown signal received")
    server.stop()


if __name__ == "__main__":
    main()
