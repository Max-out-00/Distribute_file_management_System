
from metadata_server.db import get_connection


def register_file(file_id, file_name, size, status="UPLOADING"):
    """Save a file's metadata in the files table."""

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO files (file_id, file_name, size, status)
            VALUES (?, ?, ?, ?)
            """,
            (file_id, file_name, size, status),
        )

        connection.commit()

    finally:
        connection.close()


def get_file(file_id):
    """Retrieve one file using its ID."""

    connection = get_connection()

    try:
        row = connection.execute(
            "SELECT * FROM files WHERE file_id = ?",
            (file_id,),
        ).fetchone()

        return dict(row) if row is not None else None

    finally:
        connection.close()


def update_file_status(file_id, status):
    """Update a file's status."""

    connection = get_connection()

    try:
        cursor = connection.execute(
            "UPDATE files SET status = ? WHERE file_id = ?",
            (status, file_id),
        )

        connection.commit()

        return cursor.rowcount > 0

    finally:
        connection.close()


def register_chunk(
    chunk_id, file_id, chunk_index, size, checksum
):
    """Save metadata for one chunk."""

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO chunks
                (chunk_id, file_id, chunk_index, size, checksum)
            VALUES (?, ?, ?, ?, ?)
            """,
            (chunk_id, file_id, chunk_index, size, checksum),
        )

        connection.commit()

    finally:
        connection.close()


def get_chunks_for_file(file_id):
    """Retrieve all chunks belonging to a file."""

    connection = get_connection()

    try:
        rows = connection.execute(
            """
            SELECT *
            FROM chunks
            WHERE file_id = ?
            ORDER BY chunk_index
            """,
            (file_id,),
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        connection.close()


def register_node(node_id, address, disk_used, disk_total):
    """Register a storage node."""

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO nodes
                (node_id, address, disk_used, disk_total, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (node_id, address, disk_used, disk_total, "ALIVE"),
        )

        connection.commit()

    finally:
        connection.close()


def update_node_heartbeat(node_id, disk_used, disk_total):
    """Record a node heartbeat and its current disk usage."""

    connection = get_connection()

    try:
        cursor = connection.execute(
            """
            UPDATE nodes
            SET last_heartbeat = CURRENT_TIMESTAMP,
                disk_used = ?,
                disk_total = ?,
                status = ?
            WHERE node_id = ?
            """,
            (disk_used, disk_total, "ALIVE", node_id),
        )

        connection.commit()

        return cursor.rowcount > 0

    finally:
        connection.close()


def get_live_nodes():
    """Return nodes currently marked ALIVE."""

    connection = get_connection()

    try:
        rows = connection.execute(
            "SELECT * FROM nodes WHERE status = ?",
            ("ALIVE",),
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        connection.close()


def register_chunk_location(chunk_id, node_id, status="AVAILABLE"):
    """Record that a storage node holds a chunk."""

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO chunk_location (chunk_id, node_id, status)
            VALUES (?, ?, ?)
            """,
            (chunk_id, node_id, status),
        )

        connection.commit()

    finally:
        connection.close()


def get_nodes_for_chunk(chunk_id):
    """Find the storage nodes holding a chunk."""

    connection = get_connection()

    try:
        rows = connection.execute(
            """
            SELECT n.*
            FROM nodes AS n
            JOIN chunk_location AS cl
                ON n.node_id = cl.node_id
            WHERE cl.chunk_id = ?
              AND cl.status = ?
              AND n.status = ?
            """,
            (chunk_id, "AVAILABLE", "ALIVE"),
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        connection.close()
