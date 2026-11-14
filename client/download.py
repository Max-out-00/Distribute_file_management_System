import hashlib
from client.upload import call          # your call(host, port, msg, payload) helper
from shared import constants as C

def fetch_chunk(chunk):
    """Try each node holding this chunk until one returns valid data."""
    for node in chunk["nodes"]:
        if not node.get("alive", True):
            continue                     # skip nodes the metadata server says are dead
        try:
            resp, data = call(node["host"], node["port"],
                              {"cmd": "GET_CHUNK", "chunk_id": chunk["chunk_id"]})
        except (OSError, ValueError):
            continue                     # node unreachable, try the next copy
        if resp.get("status") != "ok":
            continue
        if hashlib.sha256(data).hexdigest() != chunk["checksum"]:
            continue                     # corrupted copy, try the next one
        return data
    raise RuntimeError(f"all copies of chunk {chunk['chunk_index']} failed")

def download_file(file_id, out_path):
    meta_host = getattr(C, "METADATA_HOST", C.HOST)
    resp, _ = call(meta_host, C.METADATA_PORT,
                   {"type": "GET_LOCATIONS", "file_id": file_id})
    if resp["status"] != "ok":
        raise RuntimeError(resp["error"])

    chunks = sorted(resp["chunks"], key=lambda c: c["chunk_index"])

    # sanity check: no gaps in the sequence 0..n-1
    if [c["chunk_index"] for c in chunks] != list(range(len(chunks))):
        raise RuntimeError("missing chunk indexes, file is incomplete")

    with open(out_path, "wb") as out:
        for chunk in chunks:             # one chunk in memory at a time
            out.write(fetch_chunk(chunk))

def read_range(file_id, offset, length):
    """Direct (random) access: fetch only the chunks that cover [offset, offset+length).

    Same idea as paging: chunk number = offset // chunk_size, displacement = offset % chunk_size.
    Problem solved: reading 100 bytes from a 1 GB file no longer downloads the whole file.
    """
    if offset < 0 or length < 0:
        raise ValueError("offset and length must be non-negative")
    meta_host = getattr(C, "METADATA_HOST", C.HOST)
    resp, _ = call(meta_host, C.METADATA_PORT, {"type": "GET_LOCATIONS", "file_id": file_id})
    if resp["status"] != "ok":
        raise RuntimeError(resp["error"])
    chunks = sorted(resp["chunks"], key=lambda c: c["chunk_index"])
    if [c["chunk_index"] for c in chunks] != list(range(len(chunks))):
        raise RuntimeError("missing chunk indexes, file is incomplete")
    sizes = [c.get("chunk_size", C.CHUNK_SIZE) for c in chunks]
    end = min(offset + length, sum(sizes))
    out, position = bytearray(), 0
    for chunk, size in zip(chunks, sizes):
        start, stop = position, position + size
        if start >= end:
            break
        if stop > offset:                       # this chunk overlaps the requested range
            data = fetch_chunk(chunk)
            out += data[max(offset - start, 0):min(end, stop) - start]
        position = stop
    return bytes(out)
