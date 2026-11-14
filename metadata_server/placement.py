"""Chunk placement policies: choose which nodes receive a chunk's replicas."""
import threading

from shared import constants


class RoundRobinPlacement:
    """Walk a ring of nodes with a persistent cursor.

    With nodes [n0..n4] and 3 replicas: chunk0 -> n0,n1,n2; chunk1 -> n3,n4,n0;
    chunk2 -> n1,n2,n3 ... every node gets the same share, and the 3 replicas of
    one chunk are always on distinct nodes (count <= len(nodes)).
    The cursor is shared by all request threads, so it is guarded by a lock.
    """
    name = "round_robin"

    def __init__(self):
        self._lock = threading.Lock()
        self._cursor = 0

    def choose(self, nodes, count):
        if count <= 0 or not nodes:
            return []
        count = min(count, len(nodes))
        with self._lock:
            start = self._cursor % len(nodes)
            self._cursor += count
        return [nodes[(start + i) % len(nodes)] for i in range(count)]


class MostFreePlacement:
    """Greedy: the nodes with the most free bytes (the original behaviour)."""
    name = "most_free"

    def choose(self, nodes, count):
        ranked = sorted(nodes, key=lambda n: (-n["FREE_BYTES"], n["NODE_ID"]))
        return ranked[:max(count, 0)]


_POLICIES = {"round_robin": RoundRobinPlacement, "most_free": MostFreePlacement}
_default = None
_default_lock = threading.Lock()


def make_policy(name):
    try:
        return _POLICIES[name]()
    except KeyError:
        raise ValueError(f"unknown placement policy {name!r}") from None


def get_policy():
    """Process-wide policy so the round-robin cursor survives between requests."""
    global _default
    with _default_lock:
        if _default is None:
            _default = make_policy(constants.PLACEMENT_POLICY)
        return _default
