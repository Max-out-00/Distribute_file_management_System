import logging
import re
import time
import uuid

from shared import constants
from . import db, queries

log = logging.getLogger("metadata_server.handlers")
CHUNK_ID = re.compile(r"^[A-Za-z0-9_.-]+$")
CHECKSUM = re.compile(r"^[a-fA-F0-9]{64}$")


def _required(data, key, kind):
    value = data.get(key)
    if kind is int and type(value) is not int:
        raise ValueError(f"{key} must be an integer")
    if kind is str and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"{key} must be a non-empty string")
    return value


def handle_message(message, db_path):
    message_type = _required(message, "type", str)
    if message_type == "PING":
        return {"status": "ok"}

    with db.connection(db_path) as conn:
        if message_type in ("REGISTER_NODE", "HEARTBEAT"):
            node_id = _required(message, "node_id", str)
            host = _required(message, "host", str)
            port = _required(message, "port", int)
            free_bytes = _required(message, "free_bytes", int)
            if not 0 < port < 65536 or free_bytes < 0:
                raise ValueError("port or free_bytes is outside its valid range")
            returned = queries.register_node(
                conn, node_id, host, port, free_bytes, time.time()
            )
            if returned:
                log.info("node %s transitioned DEAD -> ALIVE", node_id)
            return {"status": "ok"}

        if message_type == "LIST_NODES":
            nodes = queries.list_nodes(conn)
            return {"status": "ok", "nodes": [
                {"node_id": n["NODE_ID"], "host": n["HOST"], "port": n["PORT"],
                 "alive": n["STATUS"] == "ALIVE"} for n in nodes
            ]}

        if message_type == "PLAN_UPLOAD":
            file_name = _required(message, "file_name", str)
            size = _required(message, "file_size", int)
            items = message.get("chunks")
            if size < 0 or not isinstance(items, list):
                raise ValueError("size must be non-negative and chunks must be a list")
            chunks = []
            seen = set()
            indexes = set()
            for item in items:
                if not isinstance(item, dict):
                    raise ValueError("each chunk descriptor must be an object")
                cid = _required(item, "chunk_id", str)
                index = _required(item, "chunk_index", int)
                chunk_size = _required(item, "chunk_size", int)
                checksum = _required(item, "checksum", str)
                if not CHUNK_ID.fullmatch(cid) or cid in seen or index in indexes:
                    raise ValueError(f"invalid or duplicate chunk_id/index for {cid}")
                if index < 0 or chunk_size < 0 or not CHECKSUM.fullmatch(checksum):
                    raise ValueError(f"invalid chunk descriptor for {cid}")
                seen.add(cid)
                indexes.add(index)
                chunks.append({"chunk_id": cid, "chunk_index": index,
                               "chunk_size": chunk_size, "checksum": checksum})
            if indexes != set(range(len(chunks))) or sum(
                item["chunk_size"] for item in chunks
            ) != size:
                raise ValueError("chunk indexes or sizes do not match file_size")
            file_id = uuid.uuid4().hex
            planned, alive = queries.create_upload_plan(
                conn, file_id, file_name, size, chunks, time.time(),
                constants.REPLICATION_FACTOR,
            )
            if alive < constants.REPLICATION_FACTOR:
                log.warning("only %d alive nodes; upload replication is limited", alive)
            return {"status": "ok", "file_id": file_id, "chunks": planned}

        if message_type == "CHUNK_STORED":
            cid = _required(message, "chunk_id", str)
            node_id = _required(message, "node_id", str)
            if not queries.confirm_replica(conn, cid, node_id):
                raise ValueError("chunk replica was not planned")
            return {"status": "ok"}

        if message_type == "GET_LOCATIONS":
            file_id = _required(message, "file_id", str)
            chunks = queries.get_file_locations(conn, file_id)
            if chunks is None:
                raise ValueError("file not found")
            return {"status": "ok", "chunks": chunks}

    raise ValueError(f"unknown message type {message_type!r}")
