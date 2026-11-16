# Distribute_file_management_System

A distributed file system project organized by service ownership.

## Project Structure

```text
distributed-fs/
├── docs/                 # Protocol, schema, architecture, and report
├── shared/               # Constants and message framing
├── metadata_server/      # Metadata TCP server, database, and placement
├── data_node/            # Chunk storage, node server, and heartbeats
├── client/               # Chunking, upload, download, and CLI
├── failure_detector/     # Monitoring and re-replication
├── dashboard/            # Optional web dashboard
├── tests/                # Unit, manual, and integration tests
└── scripts/              # Development and demo launchers
```

## Quick start (local run)

From the repository root, start the metadata server in one terminal:

```powershell
python -m metadata_server.server --port 9999
```

Then start three data nodes in separate terminals:

```powershell
python -m data_node.init --node-id node1 --bind-host 127.0.0.1 --advertise-host 127.0.0.1 --port 5001 --storage-dir data_node/storage/node1 --meta-host 127.0.0.1 --meta-port 9999
python -m data_node.init --node-id node2 --bind-host 127.0.0.1 --advertise-host 127.0.0.1 --port 5002 --storage-dir data_node/storage/node2 --meta-host 127.0.0.1 --meta-port 9999
python -m data_node.init --node-id node3 --bind-host 127.0.0.1 --advertise-host 127.0.0.1 --port 5003 --storage-dir data_node/storage/node3 --meta-host 127.0.0.1 --meta-port 9999
```

Start the web dashboard in another terminal:

```powershell
python -m dashboard.app
```

Then open the browser at:

```text
http://127.0.0.1:8000
```

Use the UI to upload, download, and delete stored files. Chunks are at most 4 MiB by default; clients reduce the per-file chunk size when the live node capacities cannot fit a 4 MiB chunk on the required number of distinct nodes. Each chunk is stored with the configured replication factor (2 by default), and uploads fail rather than silently reducing that factor when capacity is insufficient. The dashboard uses the live metadata server and data nodes directly, shows a green progress bar for uploads/downloads, surfaces node recovery notices when data nodes drop offline, and keeps a simple data log of each uploaded file and its timestamp.

Command-line alternative:

```powershell
python -m client.cli upload .\sample.bin
python -m client.cli download <FILE_ID> .\restored.bin
python -m client.cli delete <FILE_ID>
```

You can also run the built-in recovery demo script:

```powershell
./scripts/run_demo.ps1
```

or, on bash-based systems:

```bash
./scripts/run_demo.sh
```

## Show the upload, chunking, and storage flow

From PowerShell in the repository root, run:

```powershell
python -m tests.manual_roundtrip
```

The demo starts three local data-node TCP servers, reads `sample.bin` in
4,096-byte pieces, and sends the pieces round-robin to the nodes using the
`PUT_CHUNK` protocol. It then fetches each chunk with `GET_CHUNK`, checks its
SHA-256 checksum, reassembles the output as `sample_out.bin`, and compares the
input and output checksums. It does not modify `sample.bin`.

The console output includes each chunk's destination and on-disk path. You can
also inspect the stored pieces in PowerShell:

```powershell
Get-ChildItem .\data_node\storage\node*\demo_*.chunk
```

For a visible whole-file comparison after the demo:

```powershell
Get-FileHash .\sample.bin
Get-FileHash .\sample_out.bin
```

To use another file or make the split more visible:

```powershell
python -m tests.manual_roundtrip .\path\to\your-file.bin --output .\restored.bin --chunk-size 1024
```

This is a local demonstration of splitting, TCP upload/download, per-chunk
integrity checks, reassembly, and storage across three node directories. It
does not demonstrate metadata-based placement, automatic replication, or
failure recovery.

## Demonstrate adaptive capacity and replica repair

Run the isolated integration demonstrations from the repository root:

```powershell
python -m pytest -q tests/test_integration.py::test_adaptive_chunk_sizes_fit_capacity_and_reconstruct_in_order
python -m pytest -q tests/test_integration.py::test_corrupt_replica_falls_back_and_is_replicated_again
```

The first starts four 3 MiB nodes and uploads a 6 MiB file. It verifies two
3 MiB chunks, two distinct initial replicas per chunk, ordered reconstruction,
and matching bytes. The second corrupts one stored copy, verifies that download
falls back to the other checksum-valid replica, and waits for automatic
re-replication. The dashboard also displays each node's latest reported free
space and the active replication factor (2 by default); its reference value of
3 is informational, not the current configuration.


To show the evidence while presenting:

Get-ChildItem .\data_node\storage\node*\demo_*.chunk
Get-FileHash .\sample.bin
Get-FileHash .\sample_out.bin

## Running with Docker

Start the metadata server and three data nodes:

```sh
docker compose up --build -d
```

Upload and download through the optional client profile:

```sh
docker compose run --rm client python -m client.cli upload sample.bin
docker compose run --rm client python -m client.cli download YOUR_FILE_ID downloaded.bin
docker compose run --rm client python -m client.cli delete YOUR_FILE_ID
```

Run the end-to-end failure-recovery demo (PowerShell users can run
`.\scripts\run_demo.ps1`):

```sh
./scripts/run_demo.sh
```

The demo waits for all nodes, uploads `sample.bin`, shows replica placement,
stops `node2`, waits for recovery, downloads the file, and compares SHA-256
hashes. To stop the stack:

```sh
docker compose down
```