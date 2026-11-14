import os
import threading
import time

import pytest

from client.delete import delete_file
from client.download import download_file, read_range
from client.pipeline import BoundedBuffer, upload_file_parallel
from data_node.cache import ChunkCache
from data_node.heartbeat import HeartbeatSender
from data_node.node_server import DataNodeServer
from data_node.storage import ChunkStorage
from failure_detector.node_slots import NodeSlots
from metadata_server.server import MetadataServer
from shared import constants


# ---------- chunk cache = page replacement ----------
def test_lru_evicts_least_recently_used():
    cache = ChunkCache(2, "lru")
    cache.put("a", b"1"); cache.put("b", b"2")
    cache.get("a")                      # a is now most recent
    cache.put("c", b"3")                # evicts b
    assert cache.get("b") is None and cache.get("a") == b"1" and cache.get("c") == b"3"


def test_fifo_ignores_recent_use():
    cache = ChunkCache(2, "fifo")
    cache.put("a", b"1"); cache.put("b", b"2")
    cache.get("a")
    cache.put("c", b"3")                # evicts a (oldest), even though it was just used
    assert cache.get("a") is None and cache.get("b") == b"2"


def test_clock_gives_second_chance():
    cache = ChunkCache(2, "clock")
    cache.put("a", b"1"); cache.put("b", b"2")
    cache.get("a")                      # reference bit set on a
    cache.put("c", b"3")                # a gets a second chance, b is evicted
    assert cache.get("b") is None and cache.get("a") == b"1"


def test_cache_disabled_and_invalidate_and_stats():
    off = ChunkCache(0)
    off.put("a", b"1")
    assert off.get("a") is None
    cache = ChunkCache(2)
    cache.put("a", b"1"); cache.get("a"); cache.get("zzz"); cache.invalidate("a")
    stats = cache.stats()
    assert stats["hits"] == 1 and stats["misses"] == 1 and cache.get("a") is None


def test_storage_serves_second_read_from_memory_and_delete_invalidates(tmp_path):
    storage = ChunkStorage(str(tmp_path), cache=ChunkCache(4))
    storage.write_chunk("c1", b"hello")
    assert storage.read_chunk("c1") == b"hello"
    assert storage.read_chunk("c1") == b"hello"
    assert storage.cache.stats()["hits"] == 1
    storage.delete_chunk("c1")
    with pytest.raises(FileNotFoundError):
        storage.read_chunk("c1")
    storage.write_chunk("c1", b"new")
    assert storage.read_chunk("c1") == b"new"          # a rewrite never serves the stale copy


# ---------- node slots (counting semaphores) ----------
def _run_concurrently(slots, jobs, timeout=5):
    active, peak, lock, errors = [0], [0], threading.Lock(), []

    def worker(nodes):
        try:
            with slots.holding(nodes, timeout=timeout):
                with lock:
                    active[0] += 1; peak[0] = max(peak[0], active[0])
                time.sleep(0.05)
                with lock:
                    active[0] -= 1
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(j,)) for j in jobs]
    [t.start() for t in threads]; [t.join() for t in threads]
    return peak[0], errors


def test_slots_limit_concurrency_per_node():
    peak, errors = _run_concurrently(NodeSlots(capacity=2), [["n1"]] * 6)
    assert errors == [] and peak == 2


def test_opposite_order_requests_do_not_deadlock():
    jobs = [["n1", "n2"], ["n2", "n1"]] * 3
    peak, errors = _run_concurrently(NodeSlots(capacity=1), jobs, timeout=10)
    assert errors == []


# ---------- bounded buffer (producer-consumer with semaphores) ----------
def test_bounded_buffer_blocks_producer_when_full():
    buf = BoundedBuffer(2)
    assert buf.put(1) and buf.put(2)
    assert buf.put(3, timeout=0.1) is False        # full: producer must wait
    assert buf.get() == 1                           # FIFO order
    assert buf.put(3, timeout=0.1) is True


# ---------- end to end: parallel upload + direct access + delete on real nodes ----------
def test_parallel_upload_range_read_and_cache_end_to_end(tmp_path, monkeypatch):
    metadata = MetadataServer("127.0.0.1", 0, str(tmp_path / "m.db"), enable_monitor=False).start()
    monkeypatch.setattr(constants, "METADATA_PORT", metadata.port)
    monkeypatch.setattr(constants, "CHUNK_SIZE", 1024)
    stores, servers, beats = [], [], []
    try:
        for i in range(4):
            storage = ChunkStorage(str(tmp_path / f"n{i}"), cache=ChunkCache(8))
            node = DataNodeServer("127.0.0.1", 0, storage)
            node.start()
            beat = HeartbeatSender(f"n{i}", "127.0.0.1", node.port, storage,
                                   "127.0.0.1", metadata.port, interval=0.1)
            beat.start()
            stores.append(storage), servers.append(node), beats.append(beat)
        time.sleep(0.5)
        data = os.urandom(5000)                                  # 5 chunks of 1024 (last is 904)
        source = tmp_path / "f.bin"
        source.write_bytes(data)
        progress = []
        file_id = upload_file_parallel(str(source), workers=3,
                                       progress_callback=lambda d, t: progress.append((d, t)))
        assert len(progress) == 5 and progress[-1] == (5, 5)
        assert sum(s.chunk_count() for s in stores) == 15        # 5 chunks x 3 replicas

        out = tmp_path / "out.bin"
        download_file(file_id, str(out))
        assert out.read_bytes() == data

        for offset, length in [(0, 10), (1020, 10), (2048, 1024), (4990, 100), (5000, 5), (9999, 5), (0, 0)]:
            assert read_range(file_id, offset, length) == data[offset:offset + length]
        with pytest.raises(ValueError):
            read_range(file_id, -1, 5)

        for _ in range(2):
            read_range(file_id, 0, 5000)
        assert sum(s.cache.stats()["hits"] for s in stores) > 0   # second pass came from memory

        reply = delete_file(file_id)
        assert reply["deleted_chunks"] == 5 and all(s.chunk_count() == 0 for s in stores)
    finally:
        for b in beats:
            b.stop()
        for n in servers:
            n.stop()
        metadata.stop()
