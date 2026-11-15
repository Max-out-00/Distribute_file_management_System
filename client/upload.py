import os
import socket
import uuid

from client.chunker import MAX_CHUNKS_PER_FILE, choose_chunk_size, split_file
from shared import constants
from shared.message import send_msg, recv_msg


def call(host, port, msg, payload=b"", timeout=5):
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        send_msg(s, msg, payload)
        return recv_msg(s)


def plan_upload(path, upload_id=None, request=None):
    """Choose capacity-aware chunks and request a complete replica plan."""
    file_size = os.path.getsize(path)
    upload_id = upload_id or uuid.uuid4().hex
    host = getattr(constants, "METADATA_HOST", constants.HOST)
    requester = request or call
    chunk_size = constants.CHUNK_SIZE
    if file_size:
        nodes_response, _ = requester(
            host, constants.METADATA_PORT, {"type": "LIST_NODES"}
        )
        if not isinstance(nodes_response, dict):
            raise RuntimeError("metadata server returned an invalid node response")
        if nodes_response.get("status") != "ok":
            raise RuntimeError(
                nodes_response.get("error", "metadata server could not list data nodes")
            )
        nodes = nodes_response.get("nodes")
        if not isinstance(nodes, list):
            raise RuntimeError("metadata server returned an invalid node list")
        chunk_size = choose_chunk_size(
            file_size, nodes, int(constants.REPLICATION_FACTOR),
            int(constants.RESERVE_BYTES), int(constants.CHUNK_SIZE),
        )

    while True:
        descriptors = [{
            "chunk_id": f"{upload_id}_{index}",
            "chunk_index": index,
            "chunk_size": len(data),
            "checksum": checksum,
        } for index, data, checksum in split_file(path, chunk_size)]
        response, _ = requester(host, constants.METADATA_PORT, {
            "type": "PLAN_UPLOAD", "file_name": os.path.basename(path),
            "file_size": file_size, "chunks": descriptors,
        })
        if not isinstance(response, dict):
            raise RuntimeError("metadata server returned an invalid upload plan response")
        if response.get("status") == "ok":
            planned = response.get("chunks")
            metadata_file_id = response.get("file_id")
            if (not isinstance(metadata_file_id, str) or not metadata_file_id
                    or not isinstance(planned, list)):
                raise RuntimeError("metadata returned an incomplete upload plan")
            if len(planned) != len(descriptors):
                raise RuntimeError("metadata upload plan is incomplete")
            for descriptor, chunk in zip(descriptors, planned):
                if not isinstance(chunk, dict) or any(
                    chunk.get(key) != descriptor[key]
                    for key in ("chunk_id", "chunk_index", "chunk_size", "checksum")
                ):
                    raise RuntimeError("metadata upload plan does not match chunk descriptors")
            return {
                "metadata_host": host,
                "upload_id": upload_id,
                "chunk_size": chunk_size,
                "descriptors": descriptors,
                "planned": planned,
                "file_id": metadata_file_id,
            }

        error = response.get("error")
        if not isinstance(error, str) or not error:
            error = "metadata server rejected upload plan"
        minimum_size = (
            (file_size + MAX_CHUNKS_PER_FILE - 1) // MAX_CHUNKS_PER_FILE
            if file_size else 1
        )
        smaller_size = max(minimum_size, chunk_size // 2)
        if "eligible ALIVE nodes" not in error or smaller_size >= chunk_size:
            raise RuntimeError(error)
        chunk_size = smaller_size


def upload_file(path, progress_callback=None):
    plan = plan_upload(path, request=call)
    host = plan["metadata_host"]
    descriptors = plan["descriptors"]
    planned = plan["planned"]
    total_chunks = len(planned)
    uploaded_chunks = 0
    for index, data, checksum in split_file(path, plan["chunk_size"]):
        if index >= len(planned):
            raise RuntimeError("metadata upload plan is incomplete")
        chunk = planned[index]
        descriptor = descriptors[index]
        if (len(data) != descriptor["chunk_size"]
                or checksum != descriptor["checksum"]):
            raise RuntimeError("source file changed after upload planning")
        stored_nodes = set()
        last_error = "no alive nodes were planned"
        nodes = chunk.get("nodes", [])
        required = int(constants.REPLICATION_FACTOR)
        for node in nodes:
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
                stored_nodes.add(node["node_id"])
            except OSError as error:
                last_error = str(error)
        if len(stored_nodes) != required:
            raise RuntimeError(
                f"chunk {index} has {len(stored_nodes)} of {required} required replicas; "
                f"upload cannot complete: {last_error}"
            )
        uploaded_chunks += 1
        if progress_callback is not None:
            progress_callback(index + 1, total_chunks)
    if uploaded_chunks != total_chunks:
        raise RuntimeError("source file changed after upload planning")
    return plan["file_id"]