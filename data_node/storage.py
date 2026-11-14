"""Local chunk storage for a data node: just stores and retrieves blobs of bytes."""
import hashlib
import os
import re
import shutil
import threading
import time

from shared import constants
from .cache import ChunkCache

CHUNK_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


class ChunkStorage:
    def __init__(self, root_dir, cache=None):
        self.cache = cache if cache is not None else ChunkCache(
            constants.CHUNK_CACHE_CHUNKS, constants.CHUNK_CACHE_POLICY)
        self.root = os.path.abspath(root_dir)
        os.makedirs(self.root, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, chunk_id):
        if not isinstance(chunk_id, str) or not CHUNK_ID.fullmatch(chunk_id):
            raise ValueError(f"invalid chunk id: {chunk_id!r}")
        return os.path.join(self.root, chunk_id + ".chunk")

    @staticmethod
    def checksum(data):
        return hashlib.sha256(data).hexdigest()

    def write_chunk(self, chunk_id, data):
        """Atomic write: temp file + fsync + rename, so a crash never leaves a half chunk."""
        path = self._path(chunk_id)
        tmp = path + ".tmp"
        with self._lock:
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            self.cache.invalidate(chunk_id)   # never serve a stale copy
        return self.checksum(data)

    def read_chunk(self, chunk_id):
        path = self._path(chunk_id)
        cached = self.cache.get(chunk_id)          # memory hit: no disk read
        if cached is not None:
            return cached
        if not os.path.exists(path):
            raise FileNotFoundError(chunk_id)
        with open(path, "rb") as f:                # page fault: read from disk
            data = f.read()
        self.cache.put(chunk_id, data)
        return data

    def has_chunk(self, chunk_id):
        return os.path.exists(self._path(chunk_id))

    def delete_chunk(self, chunk_id):
        path = self._path(chunk_id)
        with self._lock:
            self.cache.invalidate(chunk_id)
            if os.path.exists(path):
                os.remove(path)
                return True
        return False

    def recover(self):
        """Crash recovery at startup: remove half-written *.tmp files from interrupted writes."""
        removed = 0
        with self._lock:
            for name in os.listdir(self.root):
                if name.endswith(".tmp"):
                    os.remove(os.path.join(self.root, name))
                    removed += 1
        return removed

    def remove_if_older(self, chunk_id, min_age):
        """Garbage-collect an orphan chunk, but never a file written in the last min_age seconds."""
        path = self._path(chunk_id)
        with self._lock:
            try:
                if time.time() - os.path.getmtime(path) < min_age:
                    return False
            except OSError:
                return False
            self.cache.invalidate(chunk_id)
            os.remove(path)
            return True

    def list_chunks(self):
        return [n[:-len(".chunk")] for n in os.listdir(self.root) if n.endswith(".chunk")]

    def chunk_count(self):
        return len(self.list_chunks())

    def used_bytes(self):
        return sum(os.path.getsize(os.path.join(self.root, n))
                   for n in os.listdir(self.root) if n.endswith(".chunk"))

    def free_space(self):
        return shutil.disk_usage(self.root).free