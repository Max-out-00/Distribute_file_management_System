import os
import socket
import uuid
from collections.abc import Callable

from client.chunker import split_file
from shared import constants
from shared.message import send_msg, recv_msg


def call(host, port, msg, payload=b"", timeout=5):
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        send_msg(s, msg, payload)
        return recv_msg(s)


def upload_file(path, progress_callback: Callable[[int, int], None] | None = None):
    file_id = uuid.uuid4().hex
    descriptors = []
    for index, data, checksum in split_file(path, constants.CHUNK_SIZE):
        descriptors.append({
            "chunk_id": f"{file_id}_{index}", "chunk_index": index,
            "chunk_size": len(data), "checksum": checksum,
        })
    host = getattr(constants, "METADATA_HOST", constants.HOST)
    response, _ = call(host, constants.METADATA_PORT, {
        "type": "PLAN_UPLOAD", "file_name": os.path.basename(path),
        "file_size": os.path.getsize(path), "chunks": descriptors,
    })
    if response.get("status") != "ok":
        raise RuntimeError(response.get("error", "metadata server rejected upload plan"))

    planned = response.get("chunks", [])
    total_chunks = len(planned)
    for index, data, checksum in split_file(path, constants.CHUNK_SIZE):
        if index >= len(planned):
            raise RuntimeError("metadata upload plan is incomplete")
        chunk = planned[index]
        if chunk.get("chunk_id") != descriptors[index]["chunk_id"]:
            raise RuntimeError("metadata upload plan does not match chunk order")
        stored = 0
        last_error = "no alive nodes were planned"
        for node in chunk.get("nodes", []):
            try:
                reply, _ = call(
                    node["host"], node["port"],
                    {"cmd": "PUT_CHUNK", "chunk_id": chunk["chunk_id"],
                     "checksum": checksum}, data,
                )
                if reply.get("status") != "ok":
                    last_error = reply.get("error", reply.get("message", "node rejected chunk"))
                    continue
                if reply.get("checksum") != checksum:
                    last_error = "node returned a different checksum"
                    continue
                acknowledged, _ = call(host, constants.METADATA_PORT, {
                    "type": "CHUNK_STORED", "chunk_id": chunk["chunk_id"],
                    "node_id": node["node_id"],
                })
                if acknowledged.get("status") != "ok":
                    raise RuntimeError(acknowledged.get("error", "metadata did not confirm replica"))
                stored += 1
            except OSError as error:
                last_error = str(error)
        if stored == 0:
            raise RuntimeError(f"all planned nodes failed for chunk {index}: {last_error}")
        if progress_callback is not None:
            progress_callback(index + 1, total_chunks)
    return response["file_id"]