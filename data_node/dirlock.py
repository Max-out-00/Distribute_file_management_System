"""Exclusive OS file lock on a data node's storage directory.

Problem solved: starting a second node process on the same folder (typo, copy-pasted command)
makes two processes write the same chunk files and corrupt each other. The lock is held for the
life of the process and released by the OS automatically even if the process crashes.
POSIX: fcntl.flock.  Windows: msvcrt.locking on the first byte.
"""
import os


def acquire(directory):
    os.makedirs(directory, exist_ok=True)
    handle = open(os.path.join(directory, ".node.lock"), "a+")
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError(f"storage directory {directory} is already used by another data node process")
    handle.seek(1)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle
