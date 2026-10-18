#For server user. [Stored for when we can make server work. I realized that I needed to make 5001, 5002 and so on servers for it to work]
# import hashlib
# import json
# import sqlite3
# import sys
# import uuid

# from pathlib import Path

# from flask import (
#     Flask,
#     render_template,
#     request,
#     redirect,
#     url_for,
#     flash,
# )
# from werkzeug.utils import secure_filename

# app = Flask(__name__)
# app.secret_key = "local-dashboard-dev-key"

# # Project paths
# BASE_DIR = Path(__file__).resolve().parent
# PROJECT_ROOT = BASE_DIR.parent
# DATABASE = BASE_DIR / "piyush_files.db"
# UPLOAD_DIR = BASE_DIR / "uploads"

# # Import existing team modules without modifying them.
# if str(PROJECT_ROOT) not in sys.path:
#     sys.path.insert(0, str(PROJECT_ROOT))

# from client.chunker import split_file
# from client.upload import call


# # Match the existing team's manual_roundtrip.py configuration.
# CHUNK_SIZE = 4096

# NODES = [
#     {"id": "node1", "host": "127.0.0.1", "port": 5001},
#     {"id": "node2", "host": "127.0.0.1", "port": 5002},
#     {"id": "node3", "host": "127.0.0.1", "port": 5003},
# ]


# def get_db():
#     connection = sqlite3.connect(DATABASE)
#     connection.row_factory = sqlite3.Row
#     return connection


# def init_db():
#     with get_db() as connection:
#         connection.execute("""
#             CREATE TABLE IF NOT EXISTS files (
#                 id TEXT PRIMARY KEY,
#                 filename TEXT NOT NULL,
#                 size INTEGER NOT NULL,
#                 checksum TEXT NOT NULL,
#                 chunks TEXT NOT NULL,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         """)


# @app.route("/")
# @app.route("/home")
# def home():
#     return render_template("piyush_home.html")


# @app.route("/upload", methods=["POST"])
# def upload():
#     uploaded_file = request.files.get("file")

#     if uploaded_file is None or not uploaded_file.filename:
#         flash("Please choose a file first.", "error")
#         return redirect(url_for("home"))

#     filename = secure_filename(uploaded_file.filename)

#     if not filename:
#         flash("The filename is invalid.", "error")
#         return redirect(url_for("home"))

#     UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

#     file_id = uuid.uuid4().hex
#     temporary_path = UPLOAD_DIR / f"{file_id}_{filename}"
#     placements = []

#     try:
#         uploaded_file.save(temporary_path)

#         file_size = temporary_path.stat().st_size
#         file_digest = hashlib.sha256()

#         # Calculate the checksum of the complete original file.
#         with temporary_path.open("rb") as source:
#             for block in iter(lambda: source.read(65536), b""):
#                 file_digest.update(block)

#         file_checksum = file_digest.hexdigest()

#         # Split and distribute chunks across the configured nodes.
#         for index, data, checksum in split_file(
#             str(temporary_path), CHUNK_SIZE
#         ):
#             node = NODES[index % len(NODES)]
#             chunk_id = f"{file_id}_{index}"

#             response, _ = call(
#                 node["host"],
#                 node["port"],
#                 {
#                     "cmd": "PUT_CHUNK",
#                     "chunk_id": chunk_id,
#                     "checksum": checksum,
#                 },
#                 data,
#                 timeout=10,
#             )

#             if response.get("status") != "ok":
#                 raise RuntimeError(
#                     f"Node {node['id']} rejected chunk {index}: "
#                     f"{response}"
#                 )

#             if response.get("checksum") != checksum:
#                 raise RuntimeError(
#                     f"Checksum verification failed for chunk {index}."
#                 )

#             placements.append({
#                 "index": index,
#                 "chunk_id": chunk_id,
#                 "node_id": node["id"],
#                 "host": node["host"],
#                 "port": node["port"],
#                 "checksum": checksum,
#                 "size": len(data),
#             })

#         # Record metadata only after all chunks upload successfully.
#         with get_db() as connection:
#             connection.execute(
#                 """
#                 INSERT INTO files
#                     (id, filename, size, checksum, chunks)
#                 VALUES (?, ?, ?, ?, ?)
#                 """,
#                 (
#                     file_id,
#                     filename,
#                     file_size,
#                     file_checksum,
#                     json.dumps(placements),
#                 ),
#             )

#         flash(
#             f"Uploaded '{filename}' successfully. "
#             f"Chunks stored: {len(placements)}.",
#             "success",
#         )

#     except (OSError, sqlite3.Error, RuntimeError, ValueError) as error:
#         app.logger.exception("File upload failed")
#         flash(f"Upload failed: {error}", "error")

#     finally:
#         if temporary_path.exists():
#             temporary_path.unlink()

#     return redirect(url_for("home"))


# @app.route("/api/files")
# def list_files():
#     with get_db() as connection:
#         rows = connection.execute(
#             """
#             SELECT id, filename, size, checksum, chunks, created_at
#             FROM files
#             ORDER BY created_at DESC
#             """
#         ).fetchall()

#     return {
#         "files": [
#             {
#                 "id": row["id"],
#                 "filename": row["filename"],
#                 "size": row["size"],
#                 "checksum": row["checksum"],
#                 "chunks": json.loads(row["chunks"]),
#                 "created_at": row["created_at"],
#             }
#             for row in rows
#         ]
#     }


# @app.route("/api/nodes")
# def node_status():
#     results = []

#     for node in NODES:
#         try:
#             # A lightweight connection check. This does not replace
#             # the project's heartbeat or failure detector.
#             import socket

#             with socket.create_connection(
#                 (node["host"], node["port"]), timeout=1
#             ):
#                 online = True

#         except OSError:
#             online = False

#         results.append({
#             "id": node["id"],
#             "host": node["host"],
#             "port": node["port"],
#             "status": "online" if online else "offline",
#         })

#     return {"nodes": results}


# init_db()


# if __name__ == "__main__":
#     app.run(debug=True)



import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

# Make project packages importable when launched from the repository root
# or directly from the dashboard directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.chunker import split_file
from client.upload import call

app = Flask(__name__)
app.secret_key = "piyush-dashboard-dev-key"
app.config["MAX_CONTENT_LENGTH"] = 512 * 1024 * 1024

BASE_DIR = Path(__file__).resolve().parent
DATABASE = BASE_DIR / "piyush_files.db"
CHUNK_SIZE = 4096

NODES = [
    {"id": "node1", "host": "127.0.0.1", "port": 5001},
    {"id": "node2", "host": "127.0.0.1", "port": 5002},
    {"id": "node3", "host": "127.0.0.1", "port": 5003},
]


def get_db():
    connection = sqlite3.connect(DATABASE, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                checksum TEXT NOT NULL DEFAULT '',
                chunks TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        columns = {
            row["name"]
            for row in db.execute("PRAGMA table_info(files)").fetchall()
        }

        migrations = {
            "stored_name": "TEXT",
            "size": "INTEGER NOT NULL DEFAULT 0",
            "checksum": "TEXT NOT NULL DEFAULT ''",
            "chunks": "TEXT NOT NULL DEFAULT '[]'",
            "created_at": "TIMESTAMP",
        }

        for column, definition in migrations.items():
            if column not in columns:
                db.execute(
                    f"ALTER TABLE files ADD COLUMN {column} {definition}"
                )


def node_request(node, message, payload=b"", timeout=5):
    return call(
        node["host"],
        node["port"],
        message,
        payload,
        timeout=timeout,
    )


def node_is_online(node):
    try:
        response, _ = node_request(
            node, {"cmd": "PING"}, timeout=1.5
        )
        return response.get("status") == "ok"
    except (OSError, ValueError, TimeoutError, ConnectionError):
        return False


def get_node_status():
    statuses = []

    for node in NODES:
        online = node_is_online(node)
        statuses.append({
            "id": node["id"],
            "host": node["host"],
            "port": node["port"],
            "online": online,
            "status": "Online" if online else "Offline",
        })

    return statuses


def get_files():
    with get_db() as db:
        rows = db.execute("""
            SELECT id, filename, size, checksum, chunks, created_at
            FROM files
            ORDER BY created_at DESC
        """).fetchall()

    files = []
    for row in rows:
        item = dict(row)
        try:
            item["chunks"] = json.loads(item["chunks"] or "[]")
        except (TypeError, json.JSONDecodeError):
            item["chunks"] = []

        item["chunk_count"] = len(item["chunks"])
        files.append(item)

    return files


@app.route("/")
@app.route("/home")
def home():
    files = get_files()
    nodes = get_node_status()

    return render_template(
        "piyush_home.html",
        files=files,
        total_files=len(files),
        nodes=nodes,
        online_nodes=sum(node["online"] for node in nodes),
        total_nodes=len(nodes),
    )


@app.route("/upload", methods=["POST"])
def upload():
    uploaded = request.files.get("file")

    if uploaded is None or not uploaded.filename:
        flash("Please choose a file first.", "error")
        return redirect(url_for("home"))

    filename = secure_filename(uploaded.filename)
    if not filename:
        flash("The filename is invalid.", "error")
        return redirect(url_for("home"))

    online_nodes = [node for node in NODES if node_is_online(node)]

    if not online_nodes:
        flash("Upload failed: no data nodes are online.", "error")
        return redirect(url_for("home"))

    file_id = uuid.uuid4().hex
    temp_path = None
    placed_chunks = []
    file_inserted = False

    try:
        # Keep the original file only temporarily on the dashboard.
        with tempfile.NamedTemporaryFile(
            prefix="distributed_upload_", delete=False
        ) as temp:
            temp_path = temp.name
            uploaded.save(temp)

        file_size = os.path.getsize(temp_path)

        whole_hash = hashlib.sha256()
        with open(temp_path, "rb") as source:
            for block in iter(lambda: source.read(65536), b""):
                whole_hash.update(block)

        file_checksum = whole_hash.hexdigest()
        chunk_records = []

        for index, data, checksum in split_file(temp_path, CHUNK_SIZE):
            # Spread successive chunks across the online nodes.
            node = online_nodes[index % len(online_nodes)]
            chunk_id = f"{file_id}_{index}"

            response, _ = node_request(
                node,
                {
                    "cmd": "PUT_CHUNK",
                    "chunk_id": chunk_id,
                    "checksum": checksum,
                },
                data,
                timeout=20,
            )

            if (
                response.get("status") != "ok"
                or response.get("checksum") != checksum
            ):
                raise RuntimeError(
                    response.get(
                        "message",
                        f"{node['id']} did not verify chunk {index}.",
                    )
                )

            placed_chunks.append((node, chunk_id))
            chunk_records.append({
                "index": index,
                "chunk_id": chunk_id,
                "node_id": node["id"],
                "host": node["host"],
                "port": node["port"],
                "size": len(data),
                "checksum": checksum,
            })

        # Record metadata only after all chunks have been accepted.
        with get_db() as db:
            db.execute("""
                INSERT INTO files
                    (id, filename, size, checksum, chunks, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                file_id,
                filename,
                file_size,
                file_checksum,
                json.dumps(chunk_records),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))

        file_inserted = True
        flash(
            f"'{filename}' distributed successfully in "
            f"{len(chunk_records)} chunk(s)!",
            "success",
        )

    except Exception as error:
        app.logger.exception("Distributed upload failed")

        # Remove chunks from nodes if the upload failed partway through.
        for node, chunk_id in placed_chunks:
            try:
                node_request(
                    node,
                    {"cmd": "DELETE_CHUNK", "chunk_id": chunk_id},
                    timeout=2,
                )
            except Exception:
                app.logger.warning(
                    "Could not clean up chunk %s on %s",
                    chunk_id,
                    node["id"],
                )

        if file_inserted:
            with get_db() as db:
                db.execute("DELETE FROM files WHERE id = ?", (file_id,))

        flash(f"Distributed upload failed: {error}", "error")

    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                app.logger.exception("Could not remove temporary upload")

    return redirect(url_for("home"))


@app.route("/api/files")
def api_files():
    return jsonify({"files": get_files()})


@app.route("/api/nodes")
def api_nodes():
    nodes = get_node_status()
    return jsonify({
        "nodes": nodes,
        "online_nodes": sum(node["online"] for node in nodes),
        "total_nodes": len(nodes),
    })


@app.errorhandler(413)
def file_too_large(_error):
    flash("File too large. Maximum upload size is 512 MB.", "error")
    return redirect(url_for("home"))


init_db()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=False)
