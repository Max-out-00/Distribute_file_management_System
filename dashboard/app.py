import json
import os
import sqlite3
import sys
import tempfile
import threading
import uuid
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
upload_jobs = {}
upload_jobs_lock = threading.Lock()


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
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL,
                chunks TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL
            )
            '''
        )


def _nodes_from_metadata():
    meta_host, meta_port = resolve_metadata()
    try:
        response, _ = call(meta_host, meta_port, {'type': 'LIST_NODES'}, timeout=3)
        if response.get('status') != 'ok':
            return [], response.get('error', 'Metadata server rejected the node request.')
        nodes = response.get('nodes', [])
    except (OSError, ValueError, KeyError) as exc:
        return [], str(exc)
    results = []
    for node in nodes:
        results.append({
            'id': node.get('node_id', 'unknown'),
            'host': node.get('host', 'unknown'),
            'port': node.get('port', 0),
            'online': bool(node.get('alive', False)),
            'status': 'Online' if node.get('alive', False) else 'Offline',
        })
    return results, None


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
    for row in rows:
        chunks = []
        try:
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


def _chunk_rows(files):
    rows = []
    for file in files:
        for chunk in file['chunks']:
            nodes = chunk.get('nodes', [])
            if not nodes:
                rows.append({
                    'filename': file['filename'],
                    'chunk_index': chunk.get('chunk_index', '—'),
                    'chunk_id': chunk.get('chunk_id', '—'),
                    'node_id': None,
                    'endpoint': None,
                    'status': 'No replica',
                    'online': False,
                })
                continue
            for node in nodes:
                online = bool(node.get('alive', True))
                rows.append({
                    'filename': file['filename'],
                    'chunk_index': chunk.get('chunk_index', '—'),
                    'chunk_id': chunk.get('chunk_id', '—'),
                    'node_id': node.get('node_id', 'unknown'),
                    'endpoint': f"{node.get('host', 'unknown')}:{node.get('port', 0)}",
                    'status': 'Stored · online' if online else 'Stored · node offline',
                    'online': online,
                })
    return rows


def _update_upload_job(job_id, **changes):
    with upload_jobs_lock:
        upload_jobs[job_id].update(changes)


def _process_upload(job_id, temp_path, filename, file_size):
    try:
        _update_upload_job(job_id, state='running', phase='Distributing chunks', percent=15)

        def update_progress(completed, total):
            percent = 15 + round(75 * completed / total) if total else 90
            _update_upload_job(
                job_id,
                phase=f'Distributing chunks ({completed}/{total})',
                percent=percent,
            )

        file_id = upload_file(temp_path, progress_callback=update_progress)
        _update_upload_job(job_id, phase='Loading chunk locations', percent=92)

        meta_host, meta_port = resolve_metadata()
        response, _ = call(
            meta_host,
            meta_port,
            {'type': 'GET_LOCATIONS', 'file_id': file_id},
            timeout=5,
        )
        if response.get('status') != 'ok':
            raise RuntimeError(response.get('error', 'Could not load stored chunk locations.'))
        chunk_data = response.get('chunks', [])
        with get_db() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO files (file_id, filename, size, chunks, created_at) '
                'VALUES (?, ?, ?, ?, datetime("now"))',
                (file_id, filename, file_size, json.dumps(chunk_data)),
            )
        _update_upload_job(
            job_id,
            state='complete',
            phase='Upload complete',
            percent=100,
            file_id=file_id,
        )
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        _update_upload_job(job_id, state='error', phase='Upload failed', error=str(exc))
    finally:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            _update_upload_job(job_id, cleanup_error=str(exc))


@app.route('/')
@app.route('/home')
def home():
    files = _files_from_db()
    nodes, node_error = _nodes_from_metadata()
    recovery_notice = build_recovery_notice(nodes)
    return render_template(
        'index.html',
        files=files,
        chunk_rows=_chunk_rows(files),
        nodes=nodes,
        node_error=node_error,
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


@app.route('/api/uploads', methods=['POST'])
def start_upload():
    uploaded = request.files.get('file')
    if uploaded is None or not uploaded.filename:
        return jsonify({'error': 'Please choose a file first.'}), 400

    filename = secure_filename(uploaded.filename)
    if not filename:
        return jsonify({'error': 'The filename is invalid.'}), 400

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(filename)[1]
    ) as handle:
        temp_path = handle.name
    uploaded.save(temp_path)
    job_id = uuid.uuid4().hex
    with upload_jobs_lock:
        upload_jobs[job_id] = {
            'state': 'queued',
            'phase': 'Waiting to distribute chunks',
            'percent': 10,
            'filename': filename,
        }
    worker = threading.Thread(
        target=_process_upload,
        args=(job_id, temp_path, filename, os.path.getsize(temp_path)),
        daemon=True,
    )
    worker.start()
    return jsonify({'job_id': job_id}), 202


@app.route('/api/uploads/<job_id>')
def upload_status(job_id):
    with upload_jobs_lock:
        job = upload_jobs.get(job_id)
        if job is None:
            return jsonify({'error': 'Upload job not found.'}), 404
        return jsonify(job)


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
    files = _files_from_db()
    refreshed_files = []
    meta_host, meta_port = resolve_metadata()
    try:
        for file in files:
            response, _ = call(
                meta_host,
                meta_port,
                {'type': 'GET_LOCATIONS', 'file_id': file['file_id']},
                timeout=3,
            )
            if response.get('status') != 'ok':
                if response.get('error') == 'file not found':
                    continue
                raise RuntimeError(response.get('error', 'Could not refresh chunk locations.'))
            file['chunks'] = response.get('chunks', [])
            refreshed_files.append(file)
    except (OSError, RuntimeError, ValueError, KeyError) as exc:
        return jsonify({
            'files': files,
            'chunk_rows': _chunk_rows(files),
            'error': str(exc),
        }), 503
    return jsonify({
        'files': refreshed_files,
        'chunk_rows': _chunk_rows(refreshed_files),
    })


@app.route('/api/nodes')
def api_nodes():
    nodes, error = _nodes_from_metadata()
    return jsonify({'nodes': nodes, 'error': error, 'available': error is None})


@app.errorhandler(413)
def too_large(_error):
    flash('File too large. Maximum upload size is 512 MB.', 'error')
    return redirect(url_for('home'))


init_db()

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=False)
