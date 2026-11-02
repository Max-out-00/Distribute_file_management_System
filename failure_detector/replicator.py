import logging
import queue
import threading

from client.upload import call
from metadata_server import db, queries
from shared import constants
from .worker_pool import WorkerPool

log = logging.getLogger("failure_detector.replicator")


class Replicator:
    def __init__(self, db_path, workers=constants.REPLICATION_WORKERS,
                 timeout=5, retry_delay=0.2):
        self.db_path = db_path
        self.timeout, self.retry_delay = timeout, retry_delay
        self.tasks = queue.Queue()
        self._lock = threading.Lock()
        self._in_flight = set()
        self._lost_chunks = set()
        self._last_alive_count = None
        self._pool = WorkerPool(self.tasks, self._replicate, workers)

    def start(self):
        self._pool.start()

    def enqueue(self, chunk_id, checksum, source, target):
        with self._lock:
            if chunk_id in self._in_flight:
                return False
            self._in_flight.add(chunk_id)
        self.tasks.put({
            "chunk_id": chunk_id, "checksum": checksum,
            "source": source, "target": target,
        })
        return True

    def schedule_under_replicated(self):
        with db.connection(self.db_path) as conn:
            nodes = queries.list_nodes(conn)
            alive_count = sum(node["STATUS"] == "ALIVE" for node in nodes)
            if alive_count != self._last_alive_count:
                self._last_alive_count = alive_count
                if alive_count < constants.REPLICATION_FACTOR:
                    log.warning(
                        "only %d alive nodes for replication factor %d; replicating as far as possible",
                        alive_count, constants.REPLICATION_FACTOR,
                    )
            chunks = queries.get_under_replicated_chunks(
                conn, constants.REPLICATION_FACTOR
            )
            for chunk in chunks:
                if not chunk["sources"]:
                    if chunk["chunk_id"] not in self._lost_chunks:
                        log.error("chunk lost: %s has no surviving replica", chunk["chunk_id"])
                        self._lost_chunks.add(chunk["chunk_id"])
                    continue
                self._lost_chunks.discard(chunk["chunk_id"])
                target = queries.pick_target_node(conn, chunk["chunk_id"])
                if target is None:
                    continue
                self.enqueue(
                    chunk["chunk_id"], chunk["checksum"],
                    chunk["sources"][0], target,
                )

    def _current_sources(self, chunk_id):
        with db.connection(self.db_path) as conn:
            for chunk in queries.get_under_replicated_chunks(
                conn, constants.REPLICATION_FACTOR
            ):
                if chunk["chunk_id"] == chunk_id:
                    return chunk["sources"]
        return []

    def _replicate(self, task):
        chunk_id = task["chunk_id"]
        failed_sources = set()
        stored = False
        try:
            for attempt in range(4):
                if self._pool.stop_event.is_set():
                    return
                sources = self._current_sources(chunk_id)
                source = next(
                    (node for node in sources if node["NODE_ID"] not in failed_sources),
                    None,
                )
                if source is None and sources:
                    failed_sources.clear()
                    source = sources[0]
                if source is None:
                    log.error("chunk lost: %s has no surviving replica", chunk_id)
                    return
                try:
                    response, _ = call(
                        source["HOST"], source["PORT"],
                        {"cmd": "REPLICATE_CHUNK", "chunk_id": chunk_id,
                         "checksum": task["checksum"],
                         "target_host": task["target"]["HOST"],
                         "target_port": task["target"]["PORT"]},
                        timeout=self.timeout,
                    )
                    if response.get("status") != "ok":
                        raise RuntimeError(response.get("error", response.get("message", "replication rejected")))
                    with db.connection(self.db_path) as conn:
                        queries.mark_replica(
                            conn, chunk_id, task["target"]["NODE_ID"], "STORED"
                        )
                    stored = True
                    log.info("replicated chunk %s to node %s", chunk_id, task["target"]["NODE_ID"])
                    return
                except Exception as error:
                    failed_sources.add(source["NODE_ID"])
                    if attempt == 3:
                        log.error("dropping replication task for %s after retries: %s", chunk_id, error)
                    else:
                        log.warning("replication attempt %d for %s failed: %s", attempt + 1, chunk_id, error)
                        self._pool.stop_event.wait(self.retry_delay * (attempt + 1))
        finally:
            if not stored:
                try:
                    with db.connection(self.db_path) as conn:
                        queries.remove_pending_replica(
                            conn, chunk_id, task["target"]["NODE_ID"]
                        )
                except Exception:
                    log.exception("could not clear pending replica for %s", chunk_id)
            with self._lock:
                self._in_flight.discard(chunk_id)

    def stop(self):
        self._pool.stop()