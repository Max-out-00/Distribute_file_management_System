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
# Distribute_file_management_System
