
import json
import struct


def _read_exact(connection, size):
    """Read exactly size bytes, unless the connection closes."""
    data = bytearray()

    while len(data) < size:
        part = connection.recv(size - len(data))

        if not part:
            raise ConnectionError("Connection closed unexpectedly.")

        data.extend(part)

    return bytes(data)


def send_msg(connection, message, payload=b""):
    """Send a JSON message followed by an optional binary payload."""
    if not isinstance(message, dict):
        raise TypeError("message must be a dictionary.")

    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes.")

    header = json.dumps({
        "message": message,
        "payload_size": len(payload),
    }).encode("utf-8")

    connection.sendall(struct.pack("!I", len(header)))
    connection.sendall(header)

    if payload:
        connection.sendall(payload)


def recv_msg(connection):
    """Receive a message and its optional binary payload."""
    header_size = struct.unpack(
        "!I", _read_exact(connection, 4)
    )[0]

    if header_size > 1024 * 1024:
        raise ValueError("Message header is too large.")

    header = json.loads(
        _read_exact(connection, header_size).decode("utf-8")
    )

    payload_size = header.get("payload_size", 0)

    if not isinstance(payload_size, int) or payload_size < 0:
        raise ValueError("Invalid payload size.")

    message = header["message"]
    payload = _read_exact(connection, payload_size)

    return message, payload
