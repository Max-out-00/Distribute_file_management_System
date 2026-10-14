import socket

def send_msg(sock, msg, payload=b""):
    msg_bytes = msg.encode('utf-8')
    msg_length = len(msg_bytes)
    payload_length = len(payload)
    total_length = msg_length + payload_length

    # Send the length of the message and payload
    sock.sendall(total_length.to_bytes(4, byteorder='big'))
    # Send the message
    sock.sendall(msg_bytes)
    # Send the payload if it exists
    if payload_length > 0:
        sock.sendall(payload)

def recv_msg(sock):
    # Receive the length of the incoming message and payload
    length_bytes = sock.recv(4)
    if not length_bytes:
        raise ConnectionError("Socket closed while waiting for message length.")
    total_length = int.from_bytes(length_bytes, byteorder='big')

    # Receive the message
    msg_bytes = sock.recv(total_length)
    if not msg_bytes:
        raise ConnectionError("Socket closed while waiting for message.")
    
    # Decode the message
    msg = msg_bytes.decode('utf-8')
    
    return msg