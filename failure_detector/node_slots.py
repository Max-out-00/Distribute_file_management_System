"""Per-node concurrency slots (counting semaphores) used by re-replication.

Problem solved: after a node dies, many chunks must be copied at once. Without a limit one
surviving node can be asked to run dozens of copies and its worker pool fills up with blocked
REPLICATE_CHUNK requests. A copy needs a slot on the SOURCE and on the TARGET node and holds
both (hold-and-wait), so slots are always taken in sorted node order (deadlock prevention).
"""
import contextlib
import threading
import time


class NodeSlots:
    def __init__(self, capacity=2):
        self.capacity = capacity
        self._cond = threading.Condition()
        self._free = {}

    @contextlib.contextmanager
    def holding(self, nodes, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        taken = []
        try:
            for node in sorted(set(nodes)):               # global order: no circular wait
                with self._cond:
                    self._free.setdefault(node, self.capacity)
                    while self._free[node] <= 0:
                        remaining = None if deadline is None else deadline - time.monotonic()
                        if remaining is not None and remaining <= 0:
                            raise TimeoutError("timed out waiting for a node slot")
                        self._cond.wait(remaining)
                    self._free[node] -= 1
                taken.append(node)
            yield
        finally:
            with self._cond:
                for node in taken:
                    self._free[node] += 1
                self._cond.notify_all()

    def in_use(self):
        with self._cond:
            return {n: self.capacity - f for n, f in self._free.items()}
