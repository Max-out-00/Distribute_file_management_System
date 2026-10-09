import socket
from shared.message import send_msg, recv_msg

def call(host, port, msg, payload=b"", timeout=5):
    with socket.create_connection((host, port), timeout=timeout) as s:
        send_msg(s, msg, payload)
        return recv_msg(s)