import logging
import threading
import time

from metadata_server import db, queries
from shared.constants import FAILURE_TIMEOUT
from .replicator import Replicator

log = logging.getLogger("failure_detector.monitor")


class FailureMonitor:
    def __init__(self, db_path, failure_timeout=None, interval=2,
                 workers=None, retry_delay=0.2):
        self.db_path = db_path
        self.failure_timeout = (
            FAILURE_TIMEOUT if failure_timeout is None else failure_timeout
        )
        if self.failure_timeout <= 0 or interval <= 0:
            raise ValueError("failure_timeout and interval must be positive")
        self.interval = interval
        self.stop_event = threading.Event()
        self.replicator = Replicator(
            db_path, **({"workers": workers} if workers is not None else {}),
            retry_delay=retry_delay,
        )
        self.thread = None

    def start(self):
        self.replicator.start()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def scan_once(self, now=None):
        current = time.time() if now is None else now
        cutoff = current - self.failure_timeout
        with db.connection(self.db_path) as conn:
            for row in queries.dead_candidates(conn, cutoff):
                if queries.mark_dead(conn, row["NODE_ID"], cutoff):
                    log.warning("node %s transitioned ALIVE -> DEAD", row["NODE_ID"])
        self.replicator.schedule_under_replicated()

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.scan_once()
            except Exception:
                log.exception("failure detector pass failed")
            self.stop_event.wait(self.interval)

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)
        self.replicator.stop()

    def join(self, timeout=None):
        if self.thread:
            self.thread.join(timeout)