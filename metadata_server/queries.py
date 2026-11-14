from shared import constants
from . import placement

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS NODES (
 NODE_ID TEXT PRIMARY KEY NOT NULL, HOST TEXT NOT NULL, PORT INTEGER NOT NULL,
 FREE_BYTES INTEGER NOT NULL DEFAULT 0, LAST_HEARTBEAT REAL NOT NULL,
 STATUS TEXT NOT NULL CHECK (STATUS IN ('ALIVE', 'DEAD')));
CREATE TABLE IF NOT EXISTS FILES (FILE_ID TEXT PRIMARY KEY NOT NULL,
 FILE_NAME TEXT NOT NULL, SIZE INTEGER NOT NULL, CREATED_AT REAL NOT NULL);
CREATE TABLE IF NOT EXISTS CHUNKS (CHUNK_ID TEXT PRIMARY KEY NOT NULL,
 FILE_ID TEXT NOT NULL, CHUNK_INDEX INTEGER NOT NULL, CHUNK_SIZE INTEGER NOT NULL,
 CHECKSUM TEXT NOT NULL, FOREIGN KEY (FILE_ID) REFERENCES FILES(FILE_ID));
CREATE TABLE IF NOT EXISTS CHUNKS_LOCATION (
 CHUNK_ID TEXT NOT NULL, NODE_ID TEXT NOT NULL,
 STATUS TEXT NOT NULL CHECK (STATUS IN ('PENDING', 'STORED')),
 PRIMARY KEY (CHUNK_ID, NODE_ID),
 FOREIGN KEY (CHUNK_ID) REFERENCES CHUNKS(CHUNK_ID),
 FOREIGN KEY (NODE_ID) REFERENCES NODES(NODE_ID));
"""


def initialize(conn):
    expected = {
        "NODES": {"NODE_ID", "HOST", "PORT", "FREE_BYTES", "LAST_HEARTBEAT", "STATUS"},
        "FILES": {"FILE_ID", "FILE_NAME", "SIZE", "CREATED_AT"},
        "CHUNKS": {"CHUNK_ID", "FILE_ID", "CHUNK_INDEX", "CHUNK_SIZE", "CHECKSUM"},
        "CHUNKS_LOCATION": {"CHUNK_ID", "NODE_ID", "STATUS"},
    }
    incompatible = []
    table_info = {}
    for table, columns in expected.items():
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        table_info[table] = {row["name"]: row for row in rows}
        existing = set(table_info[table])
        if existing and not columns.issubset(existing):
            incompatible.append(table)
    file_id = table_info["FILES"].get("FILE_ID")
    if file_id and (file_id["type"].upper() != "TEXT" or file_id["pk"] != 1):
        if "FILES" not in incompatible:
            incompatible.append("FILES")
    if "FILES" in incompatible:
        for table in ("CHUNKS", "CHUNKS_LOCATION"):
            if table_info[table] and table not in incompatible:
                incompatible.append(table)
    populated = [
        table for table in incompatible
        if conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
    ]
    if populated:
        raise RuntimeError(
            "legacy metadata tables contain data and need migration: " + ", ".join(populated)
        )
    for table in ("CHUNKS_LOCATION", "CHUNKS", "FILES", "NODES"):
        if table in incompatible:
            conn.execute(f"DROP TABLE {table}")
    conn.executescript(SCHEMA_SQL)


def configure(conn):
    conn.execute("PRAGMA foreign_keys = ON")


def register_node(conn, node_id, host, port, free_bytes, now):
    old = conn.execute("SELECT STATUS FROM NODES WHERE NODE_ID = ?", (node_id,)).fetchone()
    conn.execute(
        """INSERT INTO NODES (NODE_ID, HOST, PORT, FREE_BYTES, LAST_HEARTBEAT, STATUS)
           VALUES (?, ?, ?, ?, ?, 'ALIVE')
           ON CONFLICT(NODE_ID) DO UPDATE SET HOST=excluded.HOST,
           PORT=excluded.PORT, FREE_BYTES=excluded.FREE_BYTES,
           LAST_HEARTBEAT=excluded.LAST_HEARTBEAT, STATUS='ALIVE'""",
        (node_id, host, port, free_bytes, now),
    )
    return bool(old and old["STATUS"] == "DEAD")


def mark_dead(conn, node_id, cutoff):
    sql = "UPDATE NODES SET STATUS='DEAD' WHERE NODE_ID=? AND STATUS='ALIVE' AND LAST_HEARTBEAT<?"
    return conn.execute(sql, (node_id, cutoff)).rowcount == 1


def dead_candidates(conn, cutoff):
    return conn.execute(
        "SELECT NODE_ID FROM NODES WHERE STATUS='ALIVE' AND LAST_HEARTBEAT<?", (cutoff,)
    ).fetchall()


def list_nodes(conn):
    sql = "SELECT NODE_ID, HOST, PORT, FREE_BYTES, STATUS FROM NODES ORDER BY NODE_ID"
    return [dict(row) for row in conn.execute(sql)]


def get_under_replicated_chunks(conn, k):
    rows = conn.execute(
        """SELECT c.CHUNK_ID, c.CHECKSUM, COUNT(n.NODE_ID) AS STORED_COUNT
           FROM CHUNKS c LEFT JOIN CHUNKS_LOCATION l
             ON l.CHUNK_ID=c.CHUNK_ID AND l.STATUS='STORED'
           LEFT JOIN NODES n ON n.NODE_ID=l.NODE_ID AND n.STATUS='ALIVE'
           GROUP BY c.CHUNK_ID HAVING COUNT(n.NODE_ID) < ?""", (k,)
    ).fetchall()
    result = []
    for row in rows:
        sources = conn.execute(
            """SELECT n.NODE_ID, n.HOST, n.PORT, n.FREE_BYTES FROM CHUNKS_LOCATION l
               JOIN NODES n ON n.NODE_ID=l.NODE_ID
               WHERE l.CHUNK_ID=? AND l.STATUS='STORED' AND n.STATUS='ALIVE'
               ORDER BY n.FREE_BYTES DESC""", (row["CHUNK_ID"],)
        ).fetchall()
        result.append({
            "chunk_id": row["CHUNK_ID"], "checksum": row["CHECKSUM"],
            "stored_count": row["STORED_COUNT"], "sources": [dict(s) for s in sources]})
    return result


def get_replication_issues(conn, required=None):
    minimum = constants.REPLICATION_FACTOR if required is None else int(required)
    rows = conn.execute(
        """SELECT f.FILE_ID, f.FILE_NAME, c.CHUNK_ID, c.CHUNK_INDEX,
                  COUNT(DISTINCT CASE WHEN n.STATUS='ALIVE' AND l.STATUS='STORED'
                      THEN n.NODE_ID END) AS ALIVE_COPIES
           FROM FILES f
           JOIN CHUNKS c ON c.FILE_ID=f.FILE_ID
           LEFT JOIN CHUNKS_LOCATION l ON l.CHUNK_ID=c.CHUNK_ID AND l.STATUS='STORED'
           LEFT JOIN NODES n ON n.NODE_ID=l.NODE_ID
           GROUP BY f.FILE_ID, f.FILE_NAME, c.CHUNK_ID, c.CHUNK_INDEX
           HAVING ALIVE_COPIES < ?
           ORDER BY f.FILE_NAME, c.CHUNK_INDEX""", (minimum,)
    ).fetchall()
    return [
        {
            "file_id": row["FILE_ID"],
            "file_name": row["FILE_NAME"],
            "chunk_id": row["CHUNK_ID"],
            "chunk_index": row["CHUNK_INDEX"],
            "alive_copies": row["ALIVE_COPIES"],
            "required": minimum,
        }
        for row in rows
    ]


def pick_target_node(conn, chunk_id):
    row = conn.execute(
        """SELECT n.NODE_ID, n.HOST, n.PORT, n.FREE_BYTES FROM NODES n
           WHERE n.STATUS='ALIVE' AND NOT EXISTS (
             SELECT 1 FROM CHUNKS_LOCATION l
             WHERE l.CHUNK_ID=? AND l.NODE_ID=n.NODE_ID)
           ORDER BY n.FREE_BYTES DESC, n.NODE_ID LIMIT 1""", (chunk_id,)
    ).fetchone()
    return dict(row) if row else None


def mark_replica(conn, chunk_id, node_id, status):
    if status not in ("PENDING", "STORED"):
        raise ValueError("replica status must be PENDING or STORED")
    conn.execute(
        """INSERT INTO CHUNKS_LOCATION (CHUNK_ID, NODE_ID, STATUS) VALUES (?, ?, ?)
           ON CONFLICT(CHUNK_ID, NODE_ID) DO UPDATE SET STATUS=excluded.STATUS""",
        (chunk_id, node_id, status),
    )


def remove_pending_replica(conn, chunk_id, node_id):
    sql = "DELETE FROM CHUNKS_LOCATION WHERE CHUNK_ID=? AND NODE_ID=? AND STATUS='PENDING'"
    conn.execute(sql, (chunk_id, node_id))


def alive_nodes(conn):
    sql = "SELECT NODE_ID, HOST, PORT, FREE_BYTES FROM NODES WHERE STATUS='ALIVE' ORDER BY NODE_ID"
    return [dict(row) for row in conn.execute(sql)]


def create_upload_plan(conn, file_id, file_name, size, chunks, now, replica_count,
                       policy=None):
    policy = policy or placement.get_policy()
    replica_count = max(int(replica_count), constants.REPLICATION_FACTOR)
    pool = alive_nodes(conn)
    alive = len(pool)
    if alive == 0:
        raise ValueError("no alive data nodes")
    conn.execute(
        "INSERT INTO FILES (FILE_ID, FILE_NAME, SIZE, CREATED_AT) VALUES (?, ?, ?, ?)",
        (file_id, file_name, size, now),
    )
    planned = []
    for item in chunks:
        conn.execute(
            """INSERT INTO CHUNKS (CHUNK_ID, FILE_ID, CHUNK_INDEX, CHUNK_SIZE, CHECKSUM)
               VALUES (?, ?, ?, ?, ?)""",
            (item["chunk_id"], file_id, item["chunk_index"], item["chunk_size"], item["checksum"]),
        )
        eligible = [n for n in pool if n["FREE_BYTES"] >= item["chunk_size"]]
        if not eligible:
            raise ValueError("no alive data node has enough free space")
        targets = []
        for node in policy.choose(eligible, min(replica_count, len(eligible))):
            mark_replica(conn, item["chunk_id"], node["NODE_ID"], "PENDING")
            node["FREE_BYTES"] -= item["chunk_size"]   # track space used inside this plan
            targets.append({"node_id": node["NODE_ID"], "host": node["HOST"],
                            "port": node["PORT"]})
        planned.append({**item, "nodes": targets})
    return planned, alive


def confirm_replica(conn, chunk_id, node_id):
    cursor = conn.execute(
        """UPDATE CHUNKS_LOCATION SET STATUS='STORED'
           WHERE CHUNK_ID=? AND NODE_ID=?""", (chunk_id, node_id)
    )
    return cursor.rowcount == 1


def get_file_locations(conn, file_id):
    if not conn.execute("SELECT 1 FROM FILES WHERE FILE_ID=?", (file_id,)).fetchone():
        return None
    rows = conn.execute(
        """SELECT c.CHUNK_ID, c.CHUNK_INDEX, c.CHUNK_SIZE, c.CHECKSUM, n.NODE_ID, n.HOST, n.PORT,
                  n.STATUS AS NODE_STATUS, l.STATUS AS LOCATION_STATUS
           FROM CHUNKS c LEFT JOIN CHUNKS_LOCATION l ON l.CHUNK_ID=c.CHUNK_ID
           LEFT JOIN NODES n ON n.NODE_ID=l.NODE_ID
           WHERE c.FILE_ID=? ORDER BY c.CHUNK_INDEX, n.NODE_ID""", (file_id,)
    ).fetchall()
    chunks = {}
    for row in rows:
        item = chunks.setdefault(row["CHUNK_ID"], {
            "chunk_id": row["CHUNK_ID"], "chunk_index": row["CHUNK_INDEX"],
            "chunk_size": row["CHUNK_SIZE"], "checksum": row["CHECKSUM"], "nodes": []})
        if row["NODE_ID"] and row["LOCATION_STATUS"] == "STORED":
            item["nodes"].append({
                "node_id": row["NODE_ID"], "host": row["HOST"], "port": row["PORT"],
                "alive": row["NODE_STATUS"] == "ALIVE",
            })
    return list(chunks.values())

def reconcile_node(conn, node_id, reported):
    """Compare what a node says it stores with what the database believes.

    orphans: files on the node with no location row for it (e.g. file deleted while the node
             was down) - the node may delete them after a grace period.
    missing: STORED rows whose file is gone from the node - dropped so recovery re-replicates.
    """
    reported = set(reported)
    rows = conn.execute("SELECT CHUNK_ID, STATUS FROM CHUNKS_LOCATION WHERE NODE_ID=?",
                        (node_id,)).fetchall()
    known = {row["CHUNK_ID"]: row["STATUS"] for row in rows}
    orphans = sorted(c for c in reported if c not in known)
    missing = sorted(c for c, status in known.items() if status == "STORED" and c not in reported)
    for chunk_id in missing:
        conn.execute("DELETE FROM CHUNKS_LOCATION WHERE CHUNK_ID=? AND NODE_ID=?", (chunk_id, node_id))
    return orphans, missing


def leave_node(conn, node_id):
    """A node announced a clean shutdown: mark it DEAD at once. True if it was ALIVE."""
    cursor = conn.execute("UPDATE NODES SET STATUS='DEAD' WHERE NODE_ID=? AND STATUS='ALIVE'", (node_id,))
    return cursor.rowcount > 0


def has_pending_replica(conn, chunk_id):
    row = conn.execute("SELECT 1 FROM CHUNKS_LOCATION WHERE CHUNK_ID=? AND STATUS='PENDING'", (chunk_id,))
    return row.fetchone() is not None


def chunk_exists(conn, chunk_id):
    return conn.execute("SELECT 1 FROM CHUNKS WHERE CHUNK_ID=?", (chunk_id,)).fetchone() is not None


def chunk_ids_for_file(conn, file_id):
    """Chunk IDs of a file, or None if the file is unknown."""
    if not conn.execute("SELECT 1 FROM FILES WHERE FILE_ID=?", (file_id,)).fetchone():
        return None
    rows = conn.execute("SELECT CHUNK_ID FROM CHUNKS WHERE FILE_ID=?", (file_id,)).fetchall()
    return [row["CHUNK_ID"] for row in rows]


def delete_file(conn, file_id):
    """Remove a file's metadata (children first, because foreign keys are ON).

    Returns the replica endpoints [(chunk_id, host, port)] so the caller can delete
    the bytes after the transaction commits, or None if the file does not exist.
    """
    if not conn.execute("SELECT 1 FROM FILES WHERE FILE_ID=?", (file_id,)).fetchone():
        return None
    replicas = [
        (r["CHUNK_ID"], r["HOST"], r["PORT"]) for r in conn.execute(
            """SELECT l.CHUNK_ID, n.HOST, n.PORT FROM CHUNKS c
               JOIN CHUNKS_LOCATION l ON l.CHUNK_ID=c.CHUNK_ID
               JOIN NODES n ON n.NODE_ID=l.NODE_ID WHERE c.FILE_ID=?""", (file_id,))
    ]
    conn.execute("DELETE FROM CHUNKS_LOCATION WHERE CHUNK_ID IN "
                 "(SELECT CHUNK_ID FROM CHUNKS WHERE FILE_ID=?)", (file_id,))
    conn.execute("DELETE FROM CHUNKS WHERE FILE_ID=?", (file_id,))
    conn.execute("DELETE FROM FILES WHERE FILE_ID=?", (file_id,))
    return replicas
