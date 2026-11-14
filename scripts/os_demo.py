"""Live demos of the OS algorithms:  python scripts/os_demo.py [placement|deadlock|fifo|all]"""
import sys
import threading
import time

sys.path.insert(0, ".")
from metadata_server.lock_manager import DeadlockError, LockManager
from metadata_server.placement import MostFreePlacement, RoundRobinPlacement


def placement():
    print("== Round-robin vs most-free: 10 chunks, 5 nodes, 3 replicas each")
    for policy in (MostFreePlacement(), RoundRobinPlacement()):
        nodes = [{"NODE_ID": f"n{i}", "FREE_BYTES": 1000 - i} for i in range(5)]
        load = {n["NODE_ID"]: 0 for n in nodes}
        for _ in range(10):
            for n in policy.choose(nodes, 3):
                load[n["NODE_ID"]] += 1
        print(f"{policy.name:12s} chunks per node: {load}")


def deadlock():
    print("== Two threads lock A,B in opposite order")
    locks, gate, out = LockManager(), threading.Barrier(2), []

    def naive(first, second):
        locks.acquire(first)
        gate.wait()
        try:
            locks.acquire(second, timeout=2)
        except DeadlockError as e:
            out.append(f"detected by wait-for graph: {e}")
        finally:
            locks.release(first)

    ts = [threading.Thread(target=naive, args=a) for a in (("A", "B"), ("B", "A"))]
    [t.start() for t in ts]; [t.join() for t in ts]
    print("naive order   ->", out[0] if out else "no deadlock")

    def ordered(keys):
        with locks.holding(keys, timeout=2):
            time.sleep(0.05)
    ts = [threading.Thread(target=ordered, args=(k,)) for k in (["A", "B"], ["B", "A"])]
    [t.start() for t in ts]; [t.join() for t in ts]
    print("sorted order  -> both finished, no deadlock possible")


def fifo():
    import queue
    print("== FIFO bounded queue: capacity 3, 5 arrivals")
    q = queue.Queue(maxsize=3)
    for i in range(1, 6):
        try:
            q.put_nowait(f"request-{i}")
            print(f"request-{i} queued")
        except queue.Full:
            print(f"request-{i} REJECTED (server busy)")
    print("served in order:", [q.get() for _ in range(q.qsize())])


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    for name, fn in (("placement", placement), ("deadlock", deadlock), ("fifo", fifo)):
        if which in (name, "all"):
            fn()
