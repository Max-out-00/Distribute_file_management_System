import socket
import threading

from shared.message import recv_msg, send_msg


def test_json_header_and_binary_payload_round_trip():
    sender, receiver = socket.socketpair()
    expected_header = {
        "cmd": "PUT_CHUNK",
        "chunk_id": "example_0",
        "checksum": "abc123",
    }
    expected_payload = b"\x00\x01chunk data\xff"

    def send():
        with sender:
            send_msg(sender, expected_header, expected_payload)

    thread = threading.Thread(target=send)
    thread.start()
    try:
        actual_header, actual_payload = recv_msg(receiver)
    finally:
        receiver.close()
    thread.join()

    assert actual_header == {**expected_header, "size": len(expected_payload)}
    assert actual_payload == expected_payload
