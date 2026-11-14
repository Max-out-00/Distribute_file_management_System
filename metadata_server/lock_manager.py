"""Named exclusive locks with deadlock prevention and deadlock detection.

Why this exists: DELETE_FILE needs the file lock plus one lock per chunk, and the
replicator needs a chunk lock. Any code that holds one lock while waiting for
another can deadlock if two threads take them in opposite orders.

Two defences are implemented:
  * Prevention  - acquire_many() always takes locks in sorted key order, so no
                  circular wait (one of the four Coffman conditions) can form.
  * Detection   - acquire() keeps a wait-for graph (owner -> key it waits for ->
                  owner holding that key). If waiting would close a cycle it
                  raises DeadlockError instead of blocking forever.
A timeout is the last line of defence (LockTimeout).
"""
import contextlib
import threading
import time


class DeadlockError(RuntimeError):
    """Waiting for this lock would create a cycle in the wait-for graph."""


class LockTimeout(TimeoutError):
    """The lock could not be acquired within the timeout."""


class LockManager:
    def __init__(self):
        self._cond = threading.Condition()
        self._owner = {}      # key -> owner currently holding it
        self._waiting = {}    # owner -> key it is blocked on

    def _would_deadlock(self, owner, key):
        """Follow holder -> awaited key -> holder ...; a path back to owner is a cycle."""
        seen = {owner}
        current = key
        while True:
            holder = self._owner.get(current)
            if holder is None:
                return False
            if holder in seen:
                return True
            seen.add(holder)
            current = self._waiting.get(holder)
            if current is None:
                return False

    def acquire(self, key, owner=None, timeout=None, detect=True):
        owner = threading.get_ident() if owner is None else owner
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            try:
                while self._owner.get(key, owner) != owner:
                    if detect and self._would_deadlock(owner, key):
                        raise DeadlockError(f"waiting for {key!r} would deadlock")
                    self._waiting[owner] = key
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise LockTimeout(f"timed out waiting for lock {key!r}")
                    self._cond.wait(remaining)
                self._owner[key] = owner
            finally:
                self._waiting.pop(owner, None)

    def release(self, key, owner=None):
        owner = threading.get_ident() if owner is None else owner
        with self._cond:
            if self._owner.get(key) == owner:
                del self._owner[key]
                self._cond.notify_all()

    def acquire_many(self, keys, owner=None, timeout=None):
        """Deadlock prevention: take every lock in one global (sorted) order."""
        owner = threading.get_ident() if owner is None else owner
        taken = []
        try:
            for key in sorted(set(keys)):
                self.acquire(key, owner, timeout)
                taken.append(key)
        except BaseException:
            for key in reversed(taken):
                self.release(key, owner)
            raise
        return taken

    def release_many(self, keys, owner=None):
        for key in reversed(sorted(set(keys))):
            self.release(key, owner)

    @contextlib.contextmanager
    def holding(self, keys, timeout=None):
        owner = threading.get_ident()
        taken = self.acquire_many(keys, owner, timeout)
        try:
            yield
        finally:
            self.release_many(taken, owner)

    def held(self):
        with self._cond:
            return dict(self._owner)


LOCKS = LockManager()   # one process-wide instance shared by handlers and replicator
