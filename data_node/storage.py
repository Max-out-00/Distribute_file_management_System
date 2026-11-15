"""Local chunk storage for a data node: just stores and retrieves blobs of bytes."""
import hashlib
import os
import re
import shutil
import threading

from shared import constants
from .cache import ChunkCache

CHUNK_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


class ChunkStorage:
    def __init__(self, root_dir, cache=None, capacity_bytes=None):
        self.cache = cache if cache is not None else ChunkCache(
            constants.CHUNK_CACHE_CHUNKS, constants.CHUNK_CACHE_POLICY
        )
        self.root = os.path.abspath(root_dir)
        os.makedirs(self.root, exist_ok=True)
        if capacity_bytes is None:
            configured = os.environ.get("NODE_CAPACITY_BYTES")
            capacity_bytes = int(configured) if configured is not None else None
        if capacity_bytes is not None and (type(capacity_bytes) is not int or capacity_bytes < 0):
            raise ValueError("capacity_bytes must be a non-negative integer")
        self.capacity_bytes = capacity_bytes
        self._lock = threading.Lock()

    def _path(self, chunk_id):
        if not isinstance(chunk_id, str) or not CHUNK_ID.fullmatch(chunk_id):
            raise ValueError(f"invalid chunk id: {chunk_id!r}")
        return os.path.join(self.root, chunk_id + ".chunk")

    @staticmethod
    def checksum(data):
        return hashlib.sha256(data).hexdigest()

    def write_chunk(self, chunk_id, data):
        """Atomically replace a chunk after checking configured and physical capacity."""
        path = self._path(chunk_id)
        tmp = path + ".tmp"
        with self._lock:
            previous_size = os.path.getsize(path) if os.path.exists(path) else 0
            delta = len(data) - previous_size
            if self.capacity_bytes is not None and self.used_bytes() + delta > self.capacity_bytes:
                raise OSError("disk full: configured node capacity exceeded")
            if len(data) > shutil.disk_usage(self.root).free:
                raise OSError("disk full: insufficient physical disk space")
            try:
                with open(tmp, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
            except OSError as error:
                try:
                    os.remove(tmp)
                except FileNotFoundError:
                    pass
                if getattr(error, "errno", None) == 28:
                    raise OSError("disk full: insufficient physical disk space") from error
                raise
            self.cache.invalidate(chunk_id)
        return self.checksum(data)

    def read_chunk(self, chunk_id):
        path = self._path(chunk_id)
        cached = self.cache.get(chunk_id)
        if cached is not None:
            return cached
        if not os.path.exists(path):
            raise FileNotFoundError(chunk_id)
        with open(path, "rb") as f:
            data = f.read()
        self.cache.put(chunk_id, data)
        return data

    def has_chunk(self, chunk_id):
        return os.path.exists(self._path(chunk_id))

    def delete_chunk(self, chunk_id):
        path = self._path(chunk_id)
        with self._lock:
            if os.path.exists(path):
                os.remove(path)
                self.cache.invalidate(chunk_id)
                return True
        return False

    def list_chunks(self):
        return [n[:-len(".chunk")] for n in os.listdir(self.root) if n.endswith(".chunk")]

    def chunk_count(self):
        return len(self.list_chunks())

    def used_bytes(self):
        return sum(os.path.getsize(os.path.join(self.root, name))
                   for name in os.listdir(self.root) if name.endswith(".chunk"))

    def free_space(self):
        physical_free = shutil.disk_usage(self.root).free
        if self.capacity_bytes is None:
            return physical_free
        return max(0, min(self.capacity_bytes - self.used_bytes(), physical_free))

    def recover(self):
        removed = 0
        for name in os.listdir(self.root):
            if name.endswith(".tmp"):
                os.remove(os.path.join(self.root, name))
                removed += 1
        return removed
