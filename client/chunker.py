import hashlib

MAX_CHUNKS_PER_FILE = 100_000


def choose_chunk_size(file_size, nodes, replication_factor, reserve_bytes,
                      max_chunk_size, max_chunks=MAX_CHUNKS_PER_FILE):
    """Choose a chunk size that fits on enough live nodes without excessive chunk counts."""
    for name, value in (
        ("file_size", file_size),
        ("replication_factor", replication_factor),
        ("reserve_bytes", reserve_bytes),
        ("max_chunk_size", max_chunk_size),
        ("max_chunks", max_chunks),
    ):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    if replication_factor == 0 or max_chunk_size == 0 or max_chunks == 0:
        raise ValueError("replication_factor, max_chunk_size, and max_chunks must be positive")
    if file_size == 0:
        return max_chunk_size

    capacities = []
    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError("each reported data node must be an object")
        if node.get("alive") is not True:
            continue
        free_bytes = node.get("free_bytes")
        if type(free_bytes) is not int or free_bytes < 0:
            raise ValueError("each ALIVE node must report a non-negative free_bytes value")
        capacities.append(max(0, free_bytes - reserve_bytes))
    if len(capacities) < replication_factor:
        raise ValueError(
            f"only {len(capacities)} ALIVE nodes available; "
            f"{replication_factor} replicas required"
        )

    capacities.sort(reverse=True)
    per_node_capacity = min(max_chunk_size, capacities[replication_factor - 1])
    minimum_size = (file_size + max_chunks - 1) // max_chunks
    if per_node_capacity < minimum_size:
        raise ValueError(
            f"available per-node capacity is {per_node_capacity} bytes, below the "
            f"{minimum_size}-byte minimum needed to stay within {max_chunks} chunks"
        )
    return min(file_size, per_node_capacity)


def split_file(path, chunk_size):
    """Yield (index, data, checksum) one chunk at a time."""
    with open(path, "rb") as f:
        index = 0
        while True:
            data = f.read(chunk_size)
            if not data:           # empty bytes = end of file
                break
            yield index, data, hashlib.sha256(data).hexdigest()
            index += 1

def reassemble_file(chunks, out_path):
    """chunks: iterable of (index, data). Writes them in index order."""
    with open(out_path, "wb") as out:
        for _, data in sorted(chunks, key=lambda c: c[0]):
            out.write(data)