import argparse
import socket
import sys

from shared.message import recv_msg, send_msg


def check(service, host, port, timeout=2):
    message = {"type": "PING"} if service == "metadata" else {"cmd": "PING"}
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        send_msg(sock, message)
        response, _ = recv_msg(sock)
    if response.get("status") != "ok":
        raise RuntimeError(response.get("error", "PING failed"))


def main():
    parser = argparse.ArgumentParser(description="Check a DFS service with PING")
    parser.add_argument("service", choices=("metadata", "node"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    try:
        check(args.service, args.host, args.port)
    except (OSError, ValueError, RuntimeError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
