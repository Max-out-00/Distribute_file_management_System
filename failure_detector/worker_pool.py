import logging
import queue
import threading
import time

log = logging.getLogger("failure_detector.workers")


class WorkerPool:
    def __init__(self, work_queue, handler, workers=3):
        self.work_queue = work_queue
        self.handler = handler
        self.workers = workers
        self.stop_event = threading.Event()
        self.threads = []

    def start(self):
        for index in range(self.workers):
            thread = threading.Thread(
                target=self._work, name=f"replica-worker-{index + 1}", daemon=True
            )
            thread.start()
            self.threads.append(thread)

    def _work(self):
        while not self.stop_event.is_set():
            try:
                task = self.work_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                if task is not None:
                    self.handler(task)
            except Exception:
                log.exception("replication worker failed while processing task")
            finally:
                self.work_queue.task_done()

    def stop(self):
        self.stop_event.set()
        deadline = time.monotonic() + 6
        for thread in self.threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))