"""In-memory chunk cache with selectable page-replacement policy.

Memory-management mapping: each cache slot is a FRAME, a chunk is a PAGE, a miss is a
PAGE FAULT (read from disk), choosing which chunk to drop is PAGE REPLACEMENT.
Policies: lru (least recently used), fifo, clock (second chance). capacity 0 disables it.
Problem solved: repeated downloads of popular chunks no longer hit the disk every time.
"""
import threading
from collections import OrderedDict

POLICIES = ("lru", "fifo", "clock")


class ChunkCache:
    def __init__(self, capacity=8, policy="lru"):
        if policy not in POLICIES:
            raise ValueError(f"unknown cache policy {policy!r}")
        self.capacity, self.policy = max(int(capacity), 0), policy
        self._lock = threading.Lock()
        self._data = OrderedDict()   # key -> bytes (order = recency for lru, arrival for fifo)
        self._ring, self._ref, self._hand = [], {}, 0   # clock state
        self.hits = self.misses = self.evictions = 0

    def get(self, key):
        if self.capacity == 0:
            return None
        with self._lock:
            if key in self._data:
                self.hits += 1
                if self.policy == "lru":
                    self._data.move_to_end(key)
                elif self.policy == "clock":
                    self._ref[key] = 1
                return self._data[key]
            self.misses += 1
            return None

    def put(self, key, data):
        if self.capacity == 0:
            return
        with self._lock:
            if key in self._data:
                self._data[key] = data
                return
            if len(self._data) >= self.capacity:
                self._evict()
            self._data[key] = data
            if self.policy == "clock":
                self._ring.append(key)
                self._ref[key] = 0

    def _evict(self):
        if self.policy == "clock":
            while True:
                victim = self._ring[self._hand]
                if self._ref.get(victim):
                    self._ref[victim] = 0                       # second chance
                    self._hand = (self._hand + 1) % len(self._ring)
                else:
                    self._ring.pop(self._hand)
                    self._ref.pop(victim, None)
                    if self._hand >= len(self._ring):
                        self._hand = 0
                    break
            del self._data[victim]
        else:
            self._data.popitem(last=False)                      # oldest (fifo) / least recent (lru)
        self.evictions += 1

    def invalidate(self, key):
        with self._lock:
            if self._data.pop(key, None) is not None and self.policy == "clock":
                self._ring.remove(key)
                self._ref.pop(key, None)
                if self._hand >= len(self._ring):
                    self._hand = 0

    def stats(self):
        with self._lock:
            total = self.hits + self.misses
            return {"policy": self.policy, "capacity": self.capacity, "cached": len(self._data),
                    "hits": self.hits, "misses": self.misses, "evictions": self.evictions,
                    "hit_ratio": round(self.hits / total, 3) if total else 0.0}
