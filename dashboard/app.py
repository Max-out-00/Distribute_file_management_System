import json
import os
import sqlite3
import sys
import tempfile
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


init_db()

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=False)
