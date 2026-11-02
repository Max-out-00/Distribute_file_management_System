import sqlite3
from contextlib import contextmanager

from . import queries


@contextmanager
def connection(path):
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    queries.configure(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize(path):
    with connection(path) as conn:
        queries.initialize(conn)
