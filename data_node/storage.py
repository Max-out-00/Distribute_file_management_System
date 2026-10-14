"""Local chunk storage for a data node: just stores and retrieves blobs of bytes."""
import hashlib
import os
import shutil
import threading


class ChunkStorage:
    def __init__(self, root_dir):
        self.root = os.path.abspath(root_dir)
        os.makedirs(self.root, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, chunk_id):
        # Block path traversal such as "../../etc/passwd"
        if not chunk_id or os.path.basename(chunk_id) != chunk_id:
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
        return self.checksum(data)

    def read_chunk(self, chunk_id):
        path = self._path(chunk_id)
        if not os.path.exists(path):
            raise FileNotFoundError(chunk_id)
        with open(path, "rb") as f:
            return f.read()

    def has_chunk(self, chunk_id):
        return os.path.exists(self._path(chunk_id))

    def delete_chunk(self, chunk_id):
        path = self._path(chunk_id)
        with self._lock:
            if os.path.exists(path):
                os.remove(path)
                return True
        return False

    def list_chunks(self):
        return [n[:-len(".chunk")] for n in os.listdir(self.root) if n.endswith(".chunk")]

    def chunk_count(self):
        return len(self.list_chunks())

    def used_bytes(self):
        return sum(os.path.getsize(os.path.join(self.root, n))
                   for n in os.listdir(self.root) if n.endswith(".chunk"))

    def free_space(self):
        return shutil.disk_usage(self.root).free