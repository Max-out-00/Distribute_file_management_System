"""JSON-header and binary-payload framing shared by clients and data nodes."""
import json
import socket
import struct


MAX_HEADER = 1024 * 1024


def _recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise ConnectionError("socket closed while receiving a message")
        data.extend(part)
    return bytes(data)


def send_msg(sock, header, payload=b""):
    header = dict(header)
    if payload:
        header["size"] = len(payload)
    encoded_header = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack(">I", len(encoded_header)) + encoded_header + payload)


def recv_msg(sock):
    (header_size,) = struct.unpack(">I", _recv_exact(sock, 4))
    if header_size > MAX_HEADER:
        raise ValueError("header too large")

    header = json.loads(_recv_exact(sock, header_size).decode("utf-8"))
    payload_size = header.get("size", 0)
    if not isinstance(payload_size, int) or payload_size < 0:
        raise ValueError("invalid payload size")
    return header, _recv_exact(sock, payload_size) if payload_size else b""
