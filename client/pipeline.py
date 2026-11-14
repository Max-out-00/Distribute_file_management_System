"""Parallel upload built on the classic bounded-buffer (producer-consumer) problem.

One producer thread reads chunks from disk; several consumer threads send them to the data
nodes. Two counting semaphores and a mutex coordinate them:
    empty = free slots in the buffer   (producer waits when the buffer is full)
    full  = filled slots               (consumers wait when it is empty)
    mutex = protects the deque itself
Problem solved: sequential upload waits for each network round trip; the bounded buffer lets
several chunks travel at once while memory stays limited to about (buffer + workers) chunks.
"""
import collections
import os
import queue
import threading
import uuid

from client.chunker import split_file
from client.upload import call
from shared import constants


class BoundedBuffer:
    def __init__(self, capacity):
        self._items = collections.deque()
        self._mutex = threading.Lock()
        self._empty = threading.Semaphore(capacity)
        self._full = threading.Semaphore(0)

    def put(self, item, timeout=None):
        if not self._empty.acquire(timeout=timeout):
            return False
        with self._mutex:
            self._items.append(item)
        self._full.release()
        return True

    def get(self, timeout=None):
        if not self._full.acquire(timeout=timeout):
            raise queue.Empty
        with self._mutex:
            item = self._items.popleft()
        self._empty.release()
        return item


def _store_chunk(meta_host, index, chunk, data, checksum):
    """Send one chunk to every planned node and confirm each replica to the metadata server."""
    stored, last_error = 0, "no alive nodes were planned"
    for node in chunk.get("nodes", []):
        try:
            reply, _ = call(node["host"], node["port"],
                            {"cmd": "PUT_CHUNK", "chunk_id": chunk["chunk_id"],
                             "checksum": checksum}, data)
            if reply.get("status") != "ok":
                last_error = reply.get("error", reply.get("message", "node rejected chunk"))
                continue
            if reply.get("checksum") != checksum:
                last_error = "node returned a different checksum"
                continue
            ack, _ = call(meta_host, constants.METADATA_PORT,
                          {"type": "CHUNK_STORED", "chunk_id": chunk["chunk_id"],
                           "node_id": node["node_id"]})
            if ack.get("status") != "ok":
                raise RuntimeError(ack.get("error", "metadata did not confirm replica"))
            stored += 1
        except OSError as error:
            last_error = str(error)
    if stored == 0:
        raise RuntimeError(f"all planned nodes failed for chunk {index}: {last_error}")


def upload_file_parallel(path, workers=4, buffer_chunks=None, progress_callback=None):
    workers = max(int(workers), 1)
    buffer_chunks = buffer_chunks or workers
    file_id = uuid.uuid4().hex
    descriptors = [
        {"chunk_id": f"{file_id}_{i}", "chunk_index": i, "chunk_size": len(d), "checksum": c}
        for i, d, c in split_file(path, constants.CHUNK_SIZE)
    ]
    meta_host = getattr(constants, "METADATA_HOST", constants.HOST)
    response, _ = call(meta_host, constants.METADATA_PORT, {
        "type": "PLAN_UPLOAD", "file_name": os.path.basename(path),
        "file_size": os.path.getsize(path), "chunks": descriptors})
    if response.get("status") != "ok":
        raise RuntimeError(response.get("error", "metadata server rejected upload plan"))
    planned = response.get("chunks", [])
    if len(planned) != len(descriptors):
        raise RuntimeError("metadata upload plan is incomplete")

    buffer, abort, errors = BoundedBuffer(buffer_chunks), threading.Event(), []
    done, done_lock = [0], threading.Lock()

    def put_or_abort(item):
        while not abort.is_set():
            if buffer.put(item, timeout=0.2):
                return

    def producer():
        try:
            for index, data, checksum in split_file(path, constants.CHUNK_SIZE):
                put_or_abort((index, data, checksum))
        except Exception as error:
            errors.append(error)
            abort.set()
        finally:
            for _ in range(workers):
                put_or_abort(None)                      # one stop marker per consumer

    def consumer():
        while not abort.is_set():
            try:
                item = buffer.get(timeout=0.2)
            except queue.Empty:
                continue
            if item is None:
                return
            index, data, checksum = item
            try:
                if planned[index]["chunk_id"] != descriptors[index]["chunk_id"]:
                    raise RuntimeError("metadata upload plan does not match chunk order")
                _store_chunk(meta_host, index, planned[index], data, checksum)
            except Exception as error:
                errors.append(error)
                abort.set()
                return
            with done_lock:
                done[0] += 1
                if progress_callback is not None:
                    progress_callback(done[0], len(planned))

    threads = [threading.Thread(target=producer, name="upload-producer")]
    threads += [threading.Thread(target=consumer, name=f"upload-consumer-{i + 1}")
                for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return response["file_id"]
