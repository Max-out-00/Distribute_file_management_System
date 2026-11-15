import os


class DirectoryLock:
    def __init__(self, file):
        self._file = file

    def close(self):
        if self._file.closed:
            return
        if os.name == "nt":
            import msvcrt

            self._file.seek(0)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        self._file.close()


def acquire(storage_dir):
    os.makedirs(storage_dir, exist_ok=True)
    lock_file = open(os.path.join(storage_dir, ".node.lock"), "a+b")
    try:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        lock_file.close()
        raise RuntimeError(f"storage directory is already in use: {storage_dir}") from error
    return DirectoryLock(lock_file)
