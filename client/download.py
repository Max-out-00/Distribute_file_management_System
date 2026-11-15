import hashlib
import logging
from client.upload import call          # your call(host, port, msg, payload) helper
from shared import constants as C

log = logging.getLogger("client.download")


def report_corrupt(chunk_id, node_id, metadata_request=None):
    message = {
        "type": "REPORT_CORRUPT",
        "chunk_id": chunk_id,
        "node_id": node_id,
    }
    if metadata_request is None:
        host = getattr(C, "METADATA_HOST", C.HOST)
        response, _ = call(host, C.METADATA_PORT, message)
    else:
        response, _ = metadata_request(message)
    if response.get("status") != "ok":
        raise RuntimeError(response.get("error", "metadata rejected corrupt replica report"))


def fetch_chunk(chunk):
    """Try each node holding this chunk until one returns valid data."""
    expected_size = chunk.get("chunk_size")
    if expected_size is not None and (type(expected_size) is not int or expected_size < 0):
        raise RuntimeError("metadata returned an invalid chunk size")
    candidates = [node for node in chunk["nodes"] if node.get("alive", True)]
    if candidates:
        offset = chunk.get("chunk_index", 0) % len(candidates)
        candidates = candidates[offset:] + candidates[:offset]
    for node in candidates:
        try:
            resp, data = call(node["host"], node["port"],
                              {"cmd": "GET_CHUNK", "chunk_id": chunk["chunk_id"]})
        except (OSError, ValueError):
            continue                     # node unreachable, try the next copy
        if resp.get("status") != "ok":
            continue
        if hashlib.sha256(data).hexdigest() != chunk["checksum"]:
            try:
                report_corrupt(chunk["chunk_id"], node["node_id"])
            except (OSError, ValueError, RuntimeError) as error:
                log.warning("could not report corrupt replica %s on %s: %s",
                            chunk["chunk_id"], node["node_id"], error)
            continue
        if expected_size is not None and len(data) != expected_size:
            continue
        return data
    raise RuntimeError(
        f"no valid replica could provide chunk {chunk['chunk_index']} "
        f"({chunk.get('chunk_id', 'unknown')}): all available copies failed connection or SHA-256 verification"
    )

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