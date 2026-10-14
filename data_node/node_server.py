"""TCP server of a data node.

Wire format (both directions):
    [4-byte big-endian header length][JSON header][optional payload of header["size"] bytes]

Commands handled:
    PUT_CHUNK        {chunk_id, size, checksum?} + payload  -> {status}
    GET_CHUNK        {chunk_id}                             -> {status, size, checksum} + payload
    DELETE_CHUNK     {chunk_id}                             -> {status}
    HAS_CHUNK        {chunk_id}                             -> {status, exists}
    REPLICATE_CHUNK  {chunk_id, target_host, target_port}   -> {status}
                     (sent by metadata server / failure detector to a surviving replica holder)
    PING                                                    -> {status}
"""
import json
import logging
import socket
import struct
import threading

log = logging.getLogger("data_node.server")
MAX_HEADER = 1024 * 1024


# ---------- framing helpers (shared with heartbeat.py) ----------
def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("connection closed")
        buf.extend(part)
    return bytes(buf)


def send_msg(sock, header, payload=b""):
    header = dict(header)
    if payload:
        header["size"] = len(payload)
    raw = json.dumps(header).encode()
    sock.sendall(struct.pack(">I", len(raw)) + raw + payload)


def recv_msg(sock):
    (hlen,) = struct.unpack(">I", recv_exact(sock, 4))
    if hlen > MAX_HEADER:
        raise ValueError("header too large")
    header = json.loads(recv_exact(sock, hlen).decode())
    payload = recv_exact(sock, header["size"]) if header.get("size") else b""
    return header, payload


def request(host, port, header, payload=b"", timeout=10):
    """One-shot request/response helper."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        send_msg(s, header, payload)
        return recv_msg(s)


# ---------- server ----------
class DataNodeServer:
    def __init__(self, host, port, storage):
        self.host, self.port, self.storage = host, port, storage
        self._sock = None
        self._running = False

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self._running = True
        log.info("data node listening on %s:%d", self.host, self.port)
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._sock.accept()
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def _handle(self, conn, addr):
        conn.settimeout(30)
        try:
            while True:
                try:
                    header, payload = recv_msg(conn)
                except ConnectionError:
                    break
                try:
                    self._dispatch(conn, header, payload)
                except Exception as e:  # report error but keep serving
                    log.exception("error handling %s from %s", header.get("cmd"), addr)
                    send_msg(conn, {"status": "error", "message": str(e)})
        except (OSError, ValueError) as e:
            log.warning("connection %s dropped: %s", addr, e)
        finally:
            conn.close()

    def _dispatch(self, conn, h, payload):
        cmd, cid = h.get("cmd"), h.get("chunk_id")

        if cmd == "PING":
            send_msg(conn, {"status": "ok"})

        elif cmd == "PUT_CHUNK":
            expected = h.get("checksum")
            if expected and self.storage.checksum(payload) != expected:
                send_msg(conn, {"status": "error", "message": "checksum mismatch"})
                return
            digest = self.storage.write_chunk(cid, payload)
            log.info("stored chunk %s (%d bytes)", cid, len(payload))
            send_msg(conn, {"status": "ok", "checksum": digest})

        elif cmd == "GET_CHUNK":
            try:
                data = self.storage.read_chunk(cid)
            except FileNotFoundError:
                send_msg(conn, {"status": "error", "message": "chunk not found"})
                return
            send_msg(conn, {"status": "ok", "checksum": self.storage.checksum(data)}, data)

        elif cmd == "DELETE_CHUNK":
            send_msg(conn, {"status": "ok", "deleted": self.storage.delete_chunk(cid)})

        elif cmd == "HAS_CHUNK":
            send_msg(conn, {"status": "ok", "exists": self.storage.has_chunk(cid)})

        elif cmd == "REPLICATE_CHUNK":
            data = self.storage.read_chunk(cid)
            resp, _ = request(h["target_host"], int(h["target_port"]),
                              {"cmd": "PUT_CHUNK", "chunk_id": cid,
                               "checksum": self.storage.checksum(data)}, data)
            log.info("replicated %s -> %s:%s (%s)", cid, h["target_host"], h["target_port"], resp.get("status"))
            send_msg(conn, {"status": resp.get("status", "error"),
                            "message": resp.get("message", "")})

        else:
            send_msg(conn, {"status": "error", "message": f"unknown command {cmd!r}"})