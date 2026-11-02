"""TCP server of a data node.

Wire format (both directions):
    [4-byte big-endian header length][JSON header][optional payload of header["size"] bytes]

Commands handled:
    PUT_CHUNK        {chunk_id, checksum, size} + payload   -> {status, checksum}
    GET_CHUNK        {chunk_id}                             -> {status, size, checksum} + payload
    DELETE_CHUNK     {chunk_id}                             -> {status}
    HAS_CHUNK        {chunk_id}                             -> {status, exists}
    REPLICATE_CHUNK  {chunk_id, checksum, target_host, target_port} -> {status}
                     (sent by metadata server / failure detector to a surviving replica holder)
    PING                                                    -> {status}
"""
import logging
import socket
import threading

from shared.message import recv_msg, send_msg

log = logging.getLogger("data_node.server")


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
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
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
            except socket.timeout:
                continue
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
                except (OSError, ValueError, TypeError) as e:
                    log.warning("invalid request from %s: %s", addr, e)
                    send_msg(conn, {"status": "error", "error": str(e)})
                    break
                try:
                    self._dispatch(conn, header, payload)
                except Exception as e:  # report error but keep serving
                    log.exception("error handling %s from %s", header.get("cmd"), addr)
                    send_msg(conn, {"status": "error", "error": str(e), "message": str(e)})
        except (OSError, ValueError) as e:
            log.warning("connection %s dropped: %s", addr, e)
        finally:
            conn.close()

    def _dispatch(self, conn, h, payload):
        if not isinstance(h, dict):
            raise ValueError("message header must be a JSON object")
        cmd, cid = h.get("cmd"), h.get("chunk_id")

        if cmd == "PING":
            send_msg(conn, {"status": "ok"})

        elif cmd == "PUT_CHUNK":
            expected = h.get("checksum")
            if not isinstance(expected, str):
                raise ValueError("checksum is required")
            if type(h.get("size")) is not int or h["size"] != len(payload):
                raise ValueError("payload size is missing or does not match")
            if self.storage.checksum(payload) != expected:
                send_msg(conn, {"status": "error", "error": "checksum mismatch",
                                "message": "checksum mismatch"})
                return
            digest = self.storage.write_chunk(cid, payload)
            log.info("stored chunk %s (%d bytes)", cid, len(payload))
            send_msg(conn, {"status": "ok", "checksum": digest})

        elif cmd == "GET_CHUNK":
            try:
                data = self.storage.read_chunk(cid)
            except FileNotFoundError:
                send_msg(conn, {"status": "error", "error": "chunk not found",
                                "message": "chunk not found"})
                return
            send_msg(conn, {"status": "ok", "checksum": self.storage.checksum(data)}, data)

        elif cmd == "DELETE_CHUNK":
            send_msg(conn, {"status": "ok", "deleted": self.storage.delete_chunk(cid)})

        elif cmd == "HAS_CHUNK":
            send_msg(conn, {"status": "ok", "exists": self.storage.has_chunk(cid)})

        elif cmd == "REPLICATE_CHUNK":
            expected = h.get("checksum")
            target_host = h.get("target_host")
            target_port = h.get("target_port")
            if not isinstance(expected, str) or not isinstance(target_host, str):
                raise ValueError("checksum and target_host are required")
            if type(target_port) is not int or not 0 < target_port < 65536:
                raise ValueError("target_port must be a valid port")
            data = self.storage.read_chunk(cid)
            if self.storage.checksum(data) != expected:
                raise ValueError("source chunk checksum mismatch")
            resp, _ = request(target_host, target_port,
                              {"cmd": "PUT_CHUNK", "chunk_id": cid,
                               "checksum": expected}, data)
            log.info("replicated %s -> %s:%s (%s)", cid, target_host, target_port, resp.get("status"))
            send_msg(conn, {"status": resp.get("status", "error"),
                            "error": resp.get("error", resp.get("message", "")),
                            "message": resp.get("message", "")})

        else:
            send_msg(conn, {"status": "error", "error": f"unknown command {cmd!r}",
                            "message": f"unknown command {cmd!r}"})