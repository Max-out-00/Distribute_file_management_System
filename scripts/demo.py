import argparse
import os
import sys
import time

from client.upload import call
from metadata_server import db, queries
from shared import constants


def wait_for_nodes(host, port, count, timeout):
    deadline = time.monotonic() + timeout
    expected = {f"node{index}" for index in range(1, count + 1)}
    while time.monotonic() < deadline:
        try:
            response, _ = call(host, port, {"type": "LIST_NODES"}, timeout=1)
            nodes = response.get("nodes", [])
            alive = {node["node_id"] for node in nodes if node["alive"]}
            if response.get("status") == "ok" and expected.issubset(alive):
                return
        except OSError:
            pass
        time.sleep(0.25)
    raise TimeoutError(f"{count} alive nodes did not register before timeout")


def print_placement(db_path, file_id):
    with db.connection(db_path) as conn:
        chunks = queries.get_file_locations(conn, file_id)
    if chunks is None:
        raise ValueError(f"unknown file id: {file_id}")
    for chunk in chunks:
        placements = ", ".join(
            f"{node['node_id']} ({'alive' if node['alive'] else 'dead'})"
            for node in chunk["nodes"]
        ) or "no stored replicas"
        print(f"{chunk['chunk_id']}: {placements}")


def wait_for_recovery(db_path, file_id, replicas, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with db.connection(db_path) as conn:
            chunks = queries.get_file_locations(conn, file_id)
        if chunks is None:
            raise ValueError(f"unknown file id: {file_id}")
        if all(sum(node["alive"] for node in c["nodes"]) >= replicas for c in chunks):
            return
        time.sleep(0.25)
    raise TimeoutError(f"chunks did not recover to {replicas} live replicas")


def main():
    parser = argparse.ArgumentParser(description="Support the Docker recovery demo")
    commands = parser.add_subparsers(dest="command", required=True)
    nodes = commands.add_parser("wait-nodes")
    nodes.add_argument("--count", type=int, default=3)
    nodes.add_argument("--timeout", type=float, default=60)
    nodes.add_argument("--host", default=constants.METADATA_HOST)
    nodes.add_argument("--port", type=int, default=constants.METADATA_PORT)
    placement = commands.add_parser("placement")
    placement.add_argument("file_id")
    recovery = commands.add_parser("wait-recovery")
    recovery.add_argument("file_id")
    recovery.add_argument("--replicas", type=int, default=constants.REPLICATION_FACTOR)
    recovery.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    db_path = os.environ.get("METADATA_DB", "metadata_server/metadata.db")
    try:
        if args.command == "wait-nodes":
            wait_for_nodes(args.host, args.port, args.count, args.timeout)
        elif args.command == "placement":
            print_placement(db_path, args.file_id)
        else:
            wait_for_recovery(db_path, args.file_id, args.replicas, args.timeout)
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
