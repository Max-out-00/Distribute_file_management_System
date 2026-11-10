<<<<<<< HEAD
import hashlib
=======
>>>>>>> 7705a94da38dbc1d6ca4308c4dc77434284e87ba
import json
import os
import sqlite3
import sys
import tempfile
<<<<<<< HEAD
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask,
    abort,
    jsonify,
    render_template,
    request,
    send_file,
    url_for,
)
=======
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
>>>>>>> 7705a94da38dbc1d6ca4308c4dc77434284e87ba
from werkzeug.utils import secure_filename

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

<<<<<<< HEAD
from client.chunker import split_file
from client.upload import call
from shared import constants

app = Flask(__name__)
app.secret_key = os.environ.get("DASHBOARD_SECRET_KEY", "local-dashboard-dev-key")
app.config["MAX_CONTENT_LENGTH"] = int(
    os.environ.get("DASHBOARD_MAX_UPLOAD_BYTES", str(512 * 1024 * 1024))
)

BASE_DIR = Path(__file__).resolve().parent
DATABASE = Path(os.environ.get("DASHBOARD_DATABASE", str(BASE_DIR / "piyush_files.db")))
CHUNK_SIZE = constants.CHUNK_SIZE
MAX_FILE_BYTES = app.config["MAX_CONTENT_LENGTH"]
METADATA_HOST = os.environ.get("METADATA_HOST", "localhost")
METADATA_PORT = int(os.environ.get("METADATA_PORT", "5000"))
METADATA_TIMEOUT = float(os.environ.get("DASHBOARD_METADATA_TIMEOUT", "8"))
NODE_IDS = ("node1", "node2", "node3")
CURRENT_BACKEND_REPLICATION_FACTOR = constants.REPLICATION_FACTOR
APPROVED_REFERENCE_REPLICATION_FACTOR = 3

_jobs = {}
_jobs_lock = threading.Lock()


class DatabaseConnection:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10)
        self.connection.row_factory = sqlite3.Row

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        try:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
        finally:
            self.connection.close()
        return False


def get_db():
    return DatabaseConnection(DATABASE)


def init_db():
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    with get_db() as db:
        db.execute("""
=======
from client.download import download_file
from client.upload import call, upload_file
from shared import constants

app = Flask(__name__)
app.secret_key = 'distributed-fs-ui'
app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'dashboard_files.db'
METADATA_HOST = os.environ.get('METADATA_HOST', '127.0.0.1')
METADATA_PORT = int(os.environ.get('METADATA_PORT', '9999'))


def resolve_metadata():
    host = os.environ.get('METADATA_HOST', METADATA_HOST)
    port = int(os.environ.get('METADATA_PORT', str(METADATA_PORT)))
    constants.METADATA_HOST = host
    constants.METADATA_PORT = port
    return host, port


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute(
            '''
>>>>>>> 7705a94da38dbc1d6ca4308c4dc77434284e87ba
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL,
                chunks TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL
            )
<<<<<<< HEAD
        """)
        columns = {
            row["name"] for row in db.execute("PRAGMA table_info(files)").fetchall()
        }
        additions = {
            "stored_name": "TEXT",
            "metadata_file_id": "TEXT",
            "upload_status": "TEXT",
            "replica_counts": "TEXT",
            "integrity_status": "TEXT",
        }
        for name, definition in additions.items():
            if name not in columns:
                db.execute(f"ALTER TABLE files ADD COLUMN {name} {definition}")
        db.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_activity (
                id TEXT PRIMARY KEY,
                operation TEXT NOT NULL,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
        """)


def metadata_request(message, payload=b"", timeout=None):
    return call(
        METADATA_HOST,
        METADATA_PORT,
        message,
        payload,
        timeout=METADATA_TIMEOUT if timeout is None else timeout,
    )


def data_node_request(node, message, payload=b"", timeout=15):
    return call(node["host"], int(node["port"]), message, payload, timeout=timeout)


def _now():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _set_job(job_id, **updates):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is not None:
            job.update(updates)


def _create_job(job):
    with _jobs_lock:
        _jobs[job["id"]] = job


def _job_snapshot(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
        return dict(job) if job else None


def _insert_activity(activity_id, operation, filename, size, status, message=""):
    with get_db() as db:
        db.execute("""
            INSERT INTO dashboard_activity
                (id, operation, filename, size, status, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (activity_id, operation, filename, size, status, message, _now()))


def _update_activity(activity_id, status, message=""):
    with get_db() as db:
        db.execute(
            "UPDATE dashboard_activity SET status=?, message=? WHERE id=?",
            (status, message[:1000], activity_id),
        )


def _get_history():
    with get_db() as db:
        rows = db.execute("""
            SELECT id, filename, size, checksum, chunks, created_at,
                   metadata_file_id, upload_status, replica_counts, integrity_status
            FROM files ORDER BY created_at DESC, rowid DESC
        """).fetchall()
    files = []
=======
            '''
        )


def _nodes_from_metadata():
    meta_host, meta_port = resolve_metadata()
    try:
        response, _ = call(meta_host, meta_port, {'type': 'LIST_NODES'}, timeout=3)
        nodes = response.get('nodes', []) if isinstance(response, dict) else []
    except Exception:
        nodes = []
    results = []
    for node in nodes:
        results.append({
            'id': node.get('node_id', 'unknown'),
            'host': node.get('host', 'unknown'),
            'port': node.get('port', 0),
            'online': bool(node.get('alive', False)),
            'status': 'Online' if node.get('alive', False) else 'Offline',
        })
    return results


def build_recovery_notice(nodes):
    offline = [n for n in nodes if not n.get('online', False)]
    if not offline:
        return None
    node_names = ', '.join(node['id'] for node in offline)
    return f"Recovery notice: {node_names} are offline. The failure detector is re-replicating affected chunks and the dashboard will refresh with the latest metadata."


def _files_from_db():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT file_id, filename, size, chunks, created_at FROM files ORDER BY created_at DESC'
        ).fetchall()
    out = []
>>>>>>> 7705a94da38dbc1d6ca4308c4dc77434284e87ba
    for row in rows:
        chunks = []
        try:
<<<<<<< HEAD
            item["chunks"] = json.loads(item["chunks"] or "[]")
        except (TypeError, json.JSONDecodeError):
            item["chunks"] = []
        try:
            item["replica_counts"] = json.loads(item["replica_counts"] or "[]")
        except (TypeError, json.JSONDecodeError):
            item["replica_counts"] = []
        item["upload_status"] = item["upload_status"] or "Legacy local history"
        item["download_available"] = bool(item["metadata_file_id"])
        item["chunk_count"] = len(item["chunks"])
        files.append(item)
    return files


def _get_activity():
    with get_db() as db:
        rows = db.execute("""
            SELECT id, operation, filename, size, status, message, created_at
            FROM dashboard_activity ORDER BY created_at DESC LIMIT 20
        """).fetchall()
    return [dict(row) for row in rows]


def _get_nodes():
    result = {node_id: {
        "id": node_id,
        "host": os.environ.get(f"DASHBOARD_{node_id.upper()}_HOST", node_id),
        "port": int(os.environ.get(f"DASHBOARD_{node_id.upper()}_PORT", str(5000 + int(node_id[-1])))),
        "state": "unknown",
        "status": "Unknown",
        "free_bytes": None,
        "last_heartbeat": None,
    } for node_id in NODE_IDS}
    error = None
    try:
        response, _ = metadata_request({"type": "LIST_NODES"})
        if response.get("status") != "ok" or not isinstance(response.get("nodes"), list):
            raise RuntimeError(response.get("error", "Metadata returned an invalid node list"))
        for reported in response["nodes"]:
            node_id = reported.get("node_id")
            if node_id not in result:
                continue
            alive = reported.get("alive")
            result[node_id].update({
                "host": reported.get("host") or result[node_id]["host"],
                "port": reported.get("port") or result[node_id]["port"],
                "state": "alive" if alive is True else "dead" if alive is False else "unknown",
                "status": "Alive" if alive is True else "Dead" if alive is False else "Unknown",
            })
    except (OSError, ValueError, TypeError, RuntimeError, TimeoutError) as exc:
        error = str(exc)
    return list(result.values()), error


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _run_upload(job_id, temp_path, filename, size, history_id):
    metadata_file_id = None
    try:
        descriptors = []
        for index, data, checksum in split_file(temp_path, CHUNK_SIZE):
            descriptors.append({
                "chunk_id": f"{history_id}_{index}",
                "chunk_index": index,
                "chunk_size": len(data),
                "checksum": checksum,
            })
        response, _ = metadata_request({
            "type": "PLAN_UPLOAD",
            "file_name": filename,
            "file_size": size,
            "chunks": descriptors,
        })
        if response.get("status") != "ok":
            raise RuntimeError(response.get("error", "Metadata server rejected the upload plan"))
        metadata_file_id = response.get("file_id")
        planned = response.get("chunks")
        if not isinstance(metadata_file_id, str) or not isinstance(planned, list):
            raise RuntimeError("Metadata returned an incomplete upload plan")
        if len(planned) != len(descriptors):
            raise RuntimeError("Metadata upload plan does not match the chunk count")

        counts = []
        total_work = 0
        for index, chunk in enumerate(planned):
            if chunk.get("chunk_id") != descriptors[index]["chunk_id"]:
                raise RuntimeError("Metadata upload plan does not match the submitted chunk order")
            nodes = chunk.get("nodes")
            if not isinstance(nodes, list) or not nodes:
                raise RuntimeError(f"No replica targets were assigned for chunk {index}")
            counts.append(len(nodes))
            total_work += descriptors[index]["chunk_size"] * len(nodes)
        _set_job(
            job_id,
            phase="storing chunks",
            metadata_file_id=metadata_file_id,
            replicas_min=min(counts, default=0),
            replicas_max=max(counts, default=0),
            assigned_bytes=total_work,
            progress_bytes=0,
        )

        completed_bytes = 0
        completed_chunks = []
        errors = []
        for index, data, checksum in split_file(temp_path, CHUNK_SIZE):
            plan = planned[index]
            confirmed_nodes = []
            target_errors = []
            for node in plan["nodes"]:
                try:
                    if not isinstance(node.get("node_id"), str):
                        raise RuntimeError("Upload plan contains a target without a node ID")
                    reply, _ = data_node_request(node, {
                        "cmd": "PUT_CHUNK",
                        "chunk_id": plan["chunk_id"],
                        "checksum": checksum,
                        "size": len(data),
                    }, data, timeout=30)
                    if reply.get("status") != "ok" or reply.get("checksum") != checksum:
                        raise RuntimeError(reply.get("error", reply.get("message", "Node did not verify the chunk checksum")))
                    acknowledgement, _ = metadata_request({
                        "type": "CHUNK_STORED",
                        "chunk_id": plan["chunk_id"],
                        "node_id": node["node_id"],
                    })
                    if acknowledgement.get("status") != "ok":
                        raise RuntimeError(acknowledgement.get("error", "Metadata did not confirm the stored replica"))
                    confirmed_nodes.append(node["node_id"])
                    completed_bytes += len(data)
                    _set_job(job_id, progress_bytes=completed_bytes)
                except (OSError, ValueError, TypeError, RuntimeError, TimeoutError) as exc:
                    target_errors.append(f"{node.get('node_id', 'target')}: {exc}")
            completed_chunks.append({
                "index": index,
                "chunk_id": plan["chunk_id"],
                "size": len(data),
                "checksum": checksum,
                "assigned_nodes": [node.get("node_id") for node in plan["nodes"]],
                "confirmed_nodes": confirmed_nodes,
                "assigned_replicas": len(plan["nodes"]),
                "confirmed_replicas": len(confirmed_nodes),
            })
            if len(confirmed_nodes) != len(plan["nodes"]):
                errors.append(f"Chunk {index}: confirmed {len(confirmed_nodes)} of {len(plan['nodes'])} assigned replicas. " + "; ".join(target_errors))

        if errors:
            message = "Upload was not fully replicated. " + " ".join(errors)
            with get_db() as db:
                db.execute("""
                    UPDATE files SET metadata_file_id=?, chunks=?, replica_counts=?,
                        upload_status=?, integrity_status=? WHERE id=?
                """, (metadata_file_id, json.dumps(completed_chunks), json.dumps(counts),
                      "Partial", "Not yet verified", history_id))
            _update_activity(job_id, "Partial", message)
            _set_job(job_id, status="failed", phase="incomplete replicas", error=message,
                     chunks=completed_chunks, metadata_file_id=metadata_file_id)
            return

        with get_db() as db:
            db.execute("""
                UPDATE files SET metadata_file_id=?, chunks=?, replica_counts=?,
                    upload_status=?, integrity_status=? WHERE id=?
            """, (metadata_file_id, json.dumps(completed_chunks), json.dumps(counts),
                  "Uploaded", "Verified on node write", history_id))
        _update_activity(job_id, "Success", f"Metadata confirmed all {sum(counts)} planned chunk replicas.")
        _set_job(job_id, status="success", phase="complete", progress_bytes=total_work,
                 chunks=completed_chunks, metadata_file_id=metadata_file_id)
    except (OSError, ValueError, TypeError, RuntimeError, TimeoutError) as exc:
        message = str(exc)
        if metadata_file_id:
            with get_db() as db:
                db.execute("UPDATE files SET metadata_file_id=?, upload_status=? WHERE id=?",
                           (metadata_file_id, "Partial", history_id))
        else:
            with get_db() as db:
                db.execute("UPDATE files SET upload_status=? WHERE id=?", ("Failed", history_id))
        _update_activity(job_id, "Failed", message)
        _set_job(job_id, status="failed", phase="failed", error=message,
                 metadata_file_id=metadata_file_id)
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            app.logger.warning("Could not remove temporary upload file %s", temp_path)


def _run_download(job_id, metadata_file_id, filename, expected_size, expected_checksum, activity_id):
    output_path = None
    try:
        response, _ = metadata_request({"type": "GET_LOCATIONS", "file_id": metadata_file_id})
        if response.get("status") != "ok":
            raise RuntimeError(response.get("error", "Metadata server could not locate this file"))
        chunks = response.get("chunks")
        if not isinstance(chunks, list):
            raise RuntimeError("Metadata returned an invalid chunk list")
        chunks = sorted(chunks, key=lambda item: item.get("chunk_index", -1))
        if [item.get("chunk_index") for item in chunks] != list(range(len(chunks))):
            raise RuntimeError("Metadata locations contain missing or invalid chunk indexes")
        total_bytes = expected_size
        _set_job(job_id, phase="retrieving and verifying chunks", total_bytes=total_bytes,
                 progress_bytes=0)
        output = tempfile.NamedTemporaryFile(prefix="dfs-dashboard-download-", delete=False)
        output_path = output.name
        written = 0
        with output:
            for chunk in chunks:
                expected_chunk_checksum = chunk.get("checksum")
                nodes = chunk.get("nodes")
                if not isinstance(nodes, list):
                    raise RuntimeError(f"Metadata returned invalid replica locations for chunk {chunk.get('chunk_index')}")
                chunk_data = None
                failures = []
                for node in nodes:
                    if node.get("alive") is False:
                        continue
                    try:
                        reply, data = data_node_request(node, {
                            "cmd": "GET_CHUNK", "chunk_id": chunk["chunk_id"]
                        }, timeout=30)
                        if reply.get("status") != "ok":
                            raise RuntimeError(reply.get("error", "Node rejected chunk read"))
                        if hashlib.sha256(data).hexdigest() != expected_chunk_checksum:
                            raise RuntimeError("SHA-256 verification failed")
                        chunk_data = data
                        break
                    except (OSError, ValueError, TypeError, RuntimeError, TimeoutError) as exc:
                        failures.append(f"{node.get('node_id', 'replica')}: {exc}")
                if chunk_data is None:
                    raise RuntimeError(
                        f"No live replica returned a valid copy of chunk {chunk.get('chunk_index')}. "
                        + "; ".join(failures)
                    )
                output.write(chunk_data)
                written += len(chunk_data)
                _set_job(job_id, progress_bytes=written)
        if written != expected_size:
            raise RuntimeError(f"Reconstructed size {written} does not match recorded size {expected_size}")
        _set_job(job_id, phase="verifying file checksum")
        digest = _sha256_file(output_path)
        if digest != expected_checksum:
            raise RuntimeError(
                "Reconstructed file SHA-256 does not match the original uploaded file."
            )
        _update_activity(activity_id, "Success", f"All chunks and the complete file passed SHA-256 verification; file checksum {digest}.")
        _set_job(job_id, status="success", phase="complete", progress_bytes=written,
                 output_path=output_path, checksum=digest,
                 download_url=f"/api/downloads/{job_id}/file")
        output_path = None
    except (OSError, ValueError, TypeError, RuntimeError, TimeoutError) as exc:
        _update_activity(activity_id, "Failed", str(exc))
        _set_job(job_id, status="failed", phase="failed", error=str(exc))
    finally:
        if output_path:
            try:
                os.remove(output_path)
            except OSError:
                app.logger.warning("Could not remove incomplete download %s", output_path)


@app.route("/")
@app.route("/home")
def home():
    files = _get_history()
    nodes, node_error = _get_nodes()
    activities = _get_activity()
    return render_template(
        "index.html",
        files=files,
        activities=activities,
        nodes=nodes,
        node_error=node_error,
        online_nodes=sum(node["state"] == "alive" for node in nodes),
        dead_nodes=sum(node["state"] == "dead" for node in nodes),
        unknown_nodes=sum(node["state"] == "unknown" for node in nodes),
        local_file_count=len(files),
        local_bytes=sum(item["size"] or 0 for item in files),
        current_replication_factor=CURRENT_BACKEND_REPLICATION_FACTOR,
        reference_replication_factor=APPROVED_REFERENCE_REPLICATION_FACTOR,
    )


@app.route("/api/nodes")
def api_nodes():
    nodes, error = _get_nodes()
    return jsonify({"nodes": nodes, "error": error,
                    "online_nodes": sum(node["state"] == "alive" for node in nodes),
                    "total_configured": len(NODE_IDS)})


@app.route("/api/files")
def api_files():
    return jsonify({"source": "Local Dashboard History; not a metadata-server listing",
                    "files": _get_history()})


@app.route("/api/uploads", methods=["POST"])
def start_upload():
    uploaded = request.files.get("file")
    if uploaded is None or not uploaded.filename:
        return jsonify({"error": "Choose a file to upload."}), 400
    filename = secure_filename(uploaded.filename)
    if not filename:
        return jsonify({"error": "The filename is invalid."}), 400

    history_id = uuid.uuid4().hex
    activity_id = history_id
    temp = tempfile.NamedTemporaryFile(prefix="dfs-dashboard-upload-", delete=False)
    temp_path = temp.name
    try:
        uploaded.save(temp)
        temp.close()
        size = os.path.getsize(temp_path)
        if size > MAX_FILE_BYTES:
            raise ValueError("File exceeds the configured upload size limit.")
        file_checksum = _sha256_file(temp_path)
        with get_db() as db:
            db.execute("""
                INSERT INTO files (id, filename, size, checksum, chunks, created_at,
                    stored_name, upload_status, integrity_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (history_id, filename, size, file_checksum, "[]", _now(),
                  filename, "Queued", "Not yet verified"))
        _insert_activity(activity_id, "Upload", filename, size, "Queued")
        _create_job({"id": history_id, "operation": "upload", "status": "running",
                     "phase": "queued for metadata placement", "progress_bytes": 0,
                     "assigned_bytes": 0, "filename": filename, "size": size})
        thread = threading.Thread(
            target=_run_upload,
            args=(history_id, temp_path, filename, size, history_id),
            daemon=True,
            name=f"dashboard-upload-{history_id[:8]}",
        )
        thread.start()
        return jsonify({"job_id": history_id, "status": "running"}), 202
    except (OSError, sqlite3.Error, ValueError) as exc:
        try:
            temp.close()
        except OSError:
            pass
        try:
            os.remove(temp_path)
        except OSError:
            pass
        return jsonify({"error": str(exc)}), 400


@app.route("/api/downloads", methods=["POST"])
def start_download():
    data = request.get_json(silent=True) or {}
    history_id = data.get("history_id")
    if not isinstance(history_id, str):
        return jsonify({"error": "A local history ID is required."}), 400
    with get_db() as db:
        row = db.execute("""
            SELECT id, filename, size, checksum, metadata_file_id, upload_status
            FROM files WHERE id=?
        """, (history_id,)).fetchone()
    if row is None:
        return jsonify({"error": "Dashboard history record was not found."}), 404
    if not row["metadata_file_id"] or row["upload_status"] != "Uploaded":
        return jsonify({"error": "This local history entry has no completed metadata-server upload and cannot be downloaded here."}), 409

    job_id = uuid.uuid4().hex
    filename = row["filename"]
    _insert_activity(job_id, "Download", filename, row["size"], "Queued")
    _create_job({"id": job_id, "operation": "download", "status": "running",
                 "phase": "requesting chunk locations", "progress_bytes": 0,
                 "total_bytes": row["size"], "filename": filename,
                 "size": row["size"]})
    thread = threading.Thread(
        target=_run_download,
        args=(job_id, row["metadata_file_id"], filename, row["size"], row["checksum"], job_id),
        daemon=True,
        name=f"dashboard-download-{job_id[:8]}",
    )
    thread.start()
    return jsonify({"job_id": job_id, "status": "running"}), 202


@app.route("/api/jobs/<job_id>")
def job_status(job_id):
    job = _job_snapshot(job_id)
    if job is None:
        return jsonify({"error": "Job not found or dashboard process restarted."}), 404
    if job.get("operation") == "upload":
        denominator = job.get("assigned_bytes", 0)
    else:
        denominator = job.get("total_bytes", 0)
    job["progress_percent"] = (
        min(100, int(job.get("progress_bytes", 0) * 100 / denominator))
        if denominator else (100 if job.get("status") == "success" else 0)
    )
    return jsonify(job)


@app.route("/api/downloads/<job_id>/file")
def download_result(job_id):
    job = _job_snapshot(job_id)
    if not job or job.get("operation") != "download" or job.get("status") != "success":
        abort(404)
    output_path = job.get("output_path")
    if not output_path or not os.path.isfile(output_path):
        abort(404)
    response = send_file(output_path, as_attachment=True,
                         download_name=job["filename"], conditional=True)
    response.call_on_close(lambda: _remove_download_file(output_path, job_id))
    return response


def _remove_download_file(path, job_id):
    try:
        os.remove(path)
    except OSError:
        app.logger.warning("Could not remove completed download %s", path)
    _set_job(job_id, output_path=None)


@app.errorhandler(413)
def request_too_large(_error):
    return jsonify({"error": "Upload exceeds the configured request size limit."}), 413
=======
            chunks = json.loads(row['chunks'] or '[]')
        except json.JSONDecodeError:
            chunks = []
        out.append({
            'file_id': row['file_id'],
            'filename': row['filename'],
            'size': row['size'],
            'chunks': chunks,
            'chunk_count': len(chunks),
            'created_at': row['created_at'],
        })
    return out


@app.route('/')
@app.route('/home')
def home():
    files = _files_from_db()
    nodes = _nodes_from_metadata()
    recovery_notice = build_recovery_notice(nodes)
    return render_template(
        'index.html',
        files=files,
        nodes=nodes,
        total_files=len(files),
        online_nodes=sum(1 for n in nodes if n['online']),
        total_nodes=len(nodes),
        recovery_notice=recovery_notice,
    )


@app.route('/upload', methods=['POST'])
def upload():
    uploaded = request.files.get('file')
    if uploaded is None or not uploaded.filename:
        flash('Please choose a file first.', 'error')
        return redirect(url_for('home'))

    filename = secure_filename(uploaded.filename)
    if not filename:
        flash('The filename is invalid.', 'error')
        return redirect(url_for('home'))

    tmp = None
    meta_host, meta_port = resolve_metadata()
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as handle:
            uploaded.save(handle.name)
            tmp = handle.name
        file_id = upload_file(tmp)

        response, _ = call(meta_host, meta_port, {'type': 'GET_LOCATIONS', 'file_id': file_id}, timeout=5)
        chunk_data = response.get('chunks', []) if isinstance(response, dict) else []

        with get_db() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO files (file_id, filename, size, chunks, created_at) VALUES (?, ?, ?, ?, datetime("now"))',
                (file_id, filename, os.path.getsize(tmp), json.dumps(chunk_data)),
            )
        flash(f'Uploaded and distributed: {filename}', 'success')
    except Exception as exc:
        flash(f'Upload failed: {exc}', 'error')
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    return redirect(url_for('home'))


def _lookup_name(file_id):
    with get_db() as conn:
        row = conn.execute('SELECT filename FROM files WHERE file_id = ?', (file_id,)).fetchone()
    if row is None:
        return f'{file_id}.bin'
    return row['filename']


@app.route('/download/<file_id>')
def download(file_id):
    resolve_metadata()
    tmp = tempfile.NamedTemporaryFile(delete=False).name
    try:
        download_file(file_id, tmp)
        return send_file(tmp, as_attachment=True, download_name=_lookup_name(file_id))
    except Exception as exc:
        flash(f'Download failed: {exc}', 'error')
        return redirect(url_for('home'))


@app.route('/api/files')
def api_files():
    return jsonify({'files': _files_from_db()})


@app.route('/api/nodes')
def api_nodes():
    return jsonify({'nodes': _nodes_from_metadata()})


@app.errorhandler(413)
def too_large(_error):
    flash('File too large. Maximum upload size is 512 MB.', 'error')
    return redirect(url_for('home'))
>>>>>>> 7705a94da38dbc1d6ca4308c4dc77434284e87ba


init_db()

<<<<<<< HEAD
if __name__ == "__main__":
    app.run(
        host=os.environ.get("DASHBOARD_BIND_HOST", "127.0.0.1"),
        port=int(os.environ.get("DASHBOARD_PORT", "8000")),
        debug=False,
    )
=======
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=False)
>>>>>>> 7705a94da38dbc1d6ca4308c4dc77434284e87ba
