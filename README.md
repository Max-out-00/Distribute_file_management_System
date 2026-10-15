# Distributed File System

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

Run the metadata server from the project root with
`python -m metadata_server.server`.

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

# Distribute_file_management_System
