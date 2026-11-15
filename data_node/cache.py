from collections import OrderedDict
import threading


class ChunkCache:
    def __init__(self, capacity, policy="lru"):
        if type(capacity) is not int or capacity < 0:
            raise ValueError("cache capacity must be a non-negative integer")
        policy = policy.lower()
        if policy not in ("lru", "fifo", "clock"):
            raise ValueError(f"unknown cache policy: {policy!r}")
        self.capacity = capacity
        self.policy = policy
        self._items = OrderedDict()
        self._referenced = {}
        self._clock_hand = 0
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._items:
                self._misses += 1
                return None
            self._hits += 1
            if self.policy == "lru":
                self._items.move_to_end(key)
            elif self.policy == "clock":
                self._referenced[key] = True
            return self._items[key]

    def put(self, key, value):
        if self.capacity == 0:
            return
        with self._lock:
            if key in self._items:
                self._items[key] = value
                if self.policy == "lru":
                    self._items.move_to_end(key)
                if self.policy == "clock":
                    self._referenced[key] = False
                return
            if len(self._items) >= self.capacity:
                self._evict()
            self._items[key] = value
            if self.policy == "clock":
                self._referenced[key] = False

    def _evict(self):
        if self.policy != "clock":
            self._items.popitem(last=False)
            return
        while self._items:
            keys = list(self._items)
            self._clock_hand %= len(keys)
            key = keys[self._clock_hand]
            if self._referenced.get(key, False):
                self._referenced[key] = False
                self._clock_hand = (self._clock_hand + 1) % len(keys)
                continue
            del self._items[key]
            self._referenced.pop(key, None)
            self._clock_hand %= len(self._items) if self._items else 1
            return

    def invalidate(self, key):
        with self._lock:
            self._items.pop(key, None)
            self._referenced.pop(key, None)
            if self._items:
                self._clock_hand %= len(self._items)
            else:
                self._clock_hand = 0

    def stats(self):
        with self._lock:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "size": len(self._items),
                "capacity": self.capacity,
            }
