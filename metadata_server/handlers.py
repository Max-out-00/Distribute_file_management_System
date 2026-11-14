import logging
import re
import time
import uuid

from client.upload import call
from shared import constants
from . import db, queries
from .lock_manager import LOCKS

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


def _delete_file(message, db_path):
    file_id = _required(message, "file_id", str)
    with db.connection(db_path) as conn:
        chunk_ids = queries.chunk_ids_for_file(conn, file_id)
    if chunk_ids is None:
        raise ValueError("file not found")
    # File lock + every chunk lock, taken in sorted order (deadlock prevention).
    # Holding the chunk locks makes an in-flight re-replication finish first, so its
    # new replica is visible to us and no orphan copy is left behind.
    keys = [f"file:{file_id}"] + [f"chunk:{cid}" for cid in chunk_ids]
    with LOCKS.holding(keys, timeout=constants.LOCK_TIMEOUT):
        with db.connection(db_path) as conn:
            replicas = queries.delete_file(conn, file_id)   # commits on exit
        if replicas is None:
            raise ValueError("file not found")
        unreachable = 0
        for chunk_id, host, port in replicas:    # metadata is gone; now free the bytes
            try:
                reply, _ = call(host, port, {"cmd": "DELETE_CHUNK", "chunk_id": chunk_id},
                                timeout=3)
                unreachable += reply.get("status") != "ok"
            except (OSError, ValueError):
                unreachable += 1
    return {"status": "ok", "deleted_chunks": len(chunk_ids),
            "unreachable_replicas": unreachable}


def handle_message(message, db_path):
    message_type = _required(message, "type", str)
    if message_type == "PING":
        return {"status": "ok"}
    if message_type == "DELETE_FILE":
        return _delete_file(message, db_path)

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
            reply = {"status": "ok"}
            reported = message.get("chunk_ids")
            if (message_type == "REGISTER_NODE" and isinstance(reported, list)
                    and all(isinstance(c, str) for c in reported)):
                orphans, missing = queries.reconcile_node(conn, node_id, reported)
                if missing:
                    log.warning("node %s lost %d replica(s); dropped them so they are re-replicated",
                                node_id, len(missing))
                reply.update(orphans=orphans, removed_replicas=len(missing))
            return reply

        if message_type == "LEAVE_NODE":
            node_id = _required(message, "node_id", str)
            if queries.leave_node(conn, node_id):
                log.info("node %s left gracefully", node_id)
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
