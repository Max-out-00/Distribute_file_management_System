import socket
import threading
import time

import pytest

from metadata_server.lock_manager import DeadlockError, LockManager, LockTimeout
from metadata_server.placement import MostFreePlacement, RoundRobinPlacement
from metadata_server.server import MetadataServer
from shared.message import recv_msg, send_msg


def nodes(n, free=100):
    return [{"NODE_ID": f"n{i}", "FREE_BYTES": free} for i in range(n)]


def test_round_robin_rotates_and_never_repeats_a_node_within_a_chunk():
    policy, ring = RoundRobinPlacement(), nodes(5)
    picks = [[n["NODE_ID"] for n in policy.choose(ring, 3)] for _ in range(3)]
    assert picks == [["n0", "n1", "n2"], ["n3", "n4", "n0"], ["n1", "n2", "n3"]]
    assert all(len(set(p)) == 3 for p in picks)


def test_most_free_is_greedy():
    ring = nodes(3)
    ring[2]["FREE_BYTES"] = 999
    assert MostFreePlacement().choose(ring, 1)[0]["NODE_ID"] == "n2"


def test_ordered_acquisition_prevents_deadlock():
    locks, results = LockManager(), []

    def worker(keys):
        with locks.holding(keys, timeout=3):
            time.sleep(0.05)
            results.append("ok")

    threads = [threading.Thread(target=worker, args=(k,)) for k in (["a", "b"], ["b", "a"])]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert results == ["ok", "ok"]


def test_wait_for_cycle_is_detected():
    locks, barrier, errors = LockManager(), threading.Barrier(2), []

    def worker(first, second):
        locks.acquire(first)
        barrier.wait()
        try:
            locks.acquire(second, timeout=3)
            locks.release(second)
        except DeadlockError as error:
            errors.append(error)
        finally:
            locks.release(first)

    threads = [threading.Thread(target=worker, args=p) for p in (("a", "b"), ("b", "a"))]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(errors) >= 1


def test_lock_timeout():
    locks = LockManager()
    locks.acquire("x", owner="A")
    with pytest.raises(LockTimeout):
        locks.acquire("x", owner="B", timeout=0.1)


def test_full_fifo_queue_rejects_with_server_busy(tmp_path):
    server = MetadataServer(port=0, db_path=str(tmp_path / "m.db"), enable_monitor=False,
                            workers=1, max_pending=1)
    release = threading.Event()
    server._serve = lambda item: (release.wait(3), item[0].close())   # hold the lone worker
    server._pool.handler = server._serve
    server.start()
    try:
        held = []
        for _ in range(2):                     # one in service, one waiting in the queue
            held.append(socket.create_connection(("127.0.0.1", server.port)))
            time.sleep(0.2)
        third = socket.create_connection(("127.0.0.1", server.port))
        third.settimeout(3)
        reply, _ = recv_msg(third)
        assert reply["status"] == "error" and "busy" in reply["error"]
        assert server.rejected == 1
    finally:
        release.set()
        server.stop()
