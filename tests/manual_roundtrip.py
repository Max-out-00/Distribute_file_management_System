"""Run a local end-to-end chunk upload/download demo against three data nodes.

From the repository root:
    python -m tests.manual_roundtrip
    python -m tests.manual_roundtrip path\\to\\input.bin --output restored.bin
"""
import argparse
import hashlib
import uuid
from pathlib import Path

from client.chunker import reassemble_file, split_file
from client.upload import call
from data_node.node_server import DataNodeServer
from data_node.storage import ChunkStorage


CHUNK_SIZE = 4096
NODE_PORTS = (5001, 5002, 5003)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(64 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run_demo(source_path, output_path, chunk_size):
    source_path = Path(source_path).resolve()
    output_path = Path(output_path).resolve()
    if source_path == output_path:
        raise ValueError("input and output files must be different")
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if chunk_size <= 0:
        raise ValueError("chunk size must be greater than zero")

    root = Path.cwd()
    node_specs = [
        (f"node{index}", port, root / "data_node" / "storage" / f"node{index}")
        for index, port in enumerate(NODE_PORTS, start=1)
    ]
    servers = []
    placements = []
    demo_id = uuid.uuid4().hex[:12]

    try:
        for node_id, port, storage_path in node_specs:
            server = DataNodeServer("127.0.0.1", port, ChunkStorage(storage_path))
            server.start()
            servers.append(server)
            print(f"Started {node_id} on 127.0.0.1:{port}; storage: {storage_path}")

        for index, data, checksum in split_file(str(source_path), chunk_size):
            node_id, port, storage_path = node_specs[index % len(node_specs)]
            chunk_id = f"demo_{demo_id}_{index}"
            response, _ = call(
                "127.0.0.1",
                port,
                {"cmd": "PUT_CHUNK", "chunk_id": chunk_id, "checksum": checksum},
                data,
            )
            if response.get("status") != "ok":
                raise RuntimeError(f"upload of chunk {index} failed: {response}")
            if response.get("checksum") != checksum:
                raise RuntimeError(f"server checksum mismatch for chunk {index}")

            placements.append((index, chunk_id, node_id, port, storage_path))
            print(
                f"Uploaded chunk {index} ({len(data)} bytes) -> {node_id}; "
                f"stored as {storage_path / (chunk_id + '.chunk')}"
            )

        downloaded = []
        for index, chunk_id, node_id, port, _ in placements:
            response, data = call(
                "127.0.0.1", port, {"cmd": "GET_CHUNK", "chunk_id": chunk_id}
            )
            actual_checksum = hashlib.sha256(data).hexdigest()
            if response.get("status") != "ok":
                raise RuntimeError(f"download of chunk {index} failed: {response}")
            if actual_checksum != response.get("checksum"):
                raise RuntimeError(f"download checksum mismatch for chunk {index}")
            downloaded.append((index, data))
            print(f"Downloaded and verified chunk {index} from {node_id}")

        reassemble_file(downloaded, str(output_path))
        if sha256(source_path) != sha256(output_path):
            raise RuntimeError("reassembled file does not match the input")
        print(f"Reassembled file: {output_path}")
        print("SHA-256 comparison: MATCH")
        print(f"Chunks uploaded: {len(placements)}")
    finally:
        for server in reversed(servers):
            server.stop()


def main():
    parser = argparse.ArgumentParser(description="Demonstrate chunked file storage")
    parser.add_argument("source", nargs="?", default="sample.bin", help="file to upload")
    parser.add_argument("--output", default="sample_out.bin", help="reassembled output")
    parser.add_argument(
        "--chunk-size", type=int, default=CHUNK_SIZE, help="chunk size in bytes"
    )
    args = parser.parse_args()
    run_demo(args.source, args.output, args.chunk_size)


if __name__ == "__main__":
    main()
