# AGENTS.md
 
Read this file and `agents/HANDOFF.md` before making any change.
Implement ONLY the task you were given. Planned features live in `docs/roadmap.md`; build only the roadmap item named in your task, never several at once.
 
## Project summary
- A simplified GFS/HDFS-style distributed file storage system written in Python 3.
- One metadata server stores file, chunk, and node metadata in SQLite.
- Data nodes store chunk bytes locally and report liveness with heartbeats.
- A failure detector (a background thread inside the metadata server process) marks silent nodes dead and re-replicates their chunks to restore the replication factor.
- Clients get placement from the metadata server, then exchange chunk bytes directly with data nodes.
- Each node runs as a separate Docker container (or a separate machine) on a shared network.
- Wire format uses length-prefixed JSON framing and SHA-256 chunk checksums.
## Architecture invariants (never violate)
- The metadata server stores information ABOUT data. It never stores or relays file data.
- The client asks the metadata server where chunks go, then sends or fetches chunk bytes DIRECTLY to or from the nodes.
- Wire format: `[4-byte big-endian header length][JSON header][optional raw payload]`, via `send_msg` / `recv_msg` in `shared/message.py`. Reuse these; never hand-roll socket framing.
- Replication factor is 2 (`shared/constants.py`). Every chunk has a SHA-256 checksum.
- Each replica of a chunk lives on a DIFFERENT node.
- Standard library only, except pytest (tests) and Flask (optional dashboard).
- Run everything from the repo root with `python -m package.module`. Package folders use underscores.
- Tables and status values:
  - `NODES(NODE_ID, HOST, PORT, FREE_BYTES, LAST_HEARTBEAT, STATUS)`; `NODES.STATUS` is `ALIVE` or `DEAD`.
  - `FILES(FILE_ID, FILE_NAME, SIZE, NUM_CHUNKS, CREATED_AT, STATUS)`; `FILES.STATUS` is `UPLOADING` or `COMPLETE`. Only `COMPLETE` files can be downloaded.
  - `CHUNKS(CHUNK_ID, FILE_ID, CHUNK_INDEX, CHUNK_SIZE, CHECK_SUM)`.
  - `CHUNKS_LOCATION(CHUNK_ID, NODE_ID, STATUS)`; `STATUS` is `PENDING`, `STORED`, or `CORRUPT`.
  - `EVENTS(EVENT_ID, TIMESTAMP, KIND, NODE_ID, CHUNK_ID, MESSAGE)` for notifications (added when roadmap item 5 is built).
- All SQL uses `?` placeholders and lives only in `metadata_server/queries.py`.
- FILE_ID and CHUNK_ID values are uuid-based text, never reused.
## Ownership map
Each agent edits only its own area unless the task explicitly says otherwise.
- `metadata_server/`: metadata/DB agent.
- `data_node/`: data node agent.
- `failure_detector/`: failure-handling agent.
- `client/`: client agent.
- `dashboard/`: dashboard agent (optional component).
- `shared/`, `docs/protocol.md`, `docs/schema.sql`: CONTRACT files. Before changing one, add a note to `agents/HANDOFF.md`, and update every dependent component in the same change.
- `Dockerfile`, `docker-compose.yml`, `scripts/`: infra agent.
- `docs/roadmap.md`: read-only for agents, except to update the status of the item you completed.
## Protocol rule
- Every message type is documented in `docs/protocol.md` with its fields, an example, the reply, and its error cases.
- Add or change the documentation BEFORE the code.
- Never invent an undocumented message type.
- Replies are `{"status": "ok", ...}` or `{"status": "error", "error": "..."}`.
## Coding rules
- Use plain functions for logic and one `Handler` class per server. No custom socket classes.
- Every socket has a timeout. Never use a bare `except:`.
- Servers must never crash on bad input; return an error reply instead.
- Validate required message fields and return a clear error naming the missing field.
- Use a fresh SQLite connection per request or per thread. Never share a connection across threads.
- Guard shared in-memory state with a lock.
- Write chunks to a `.tmp` file, then `os.replace`. Never leave a half-written chunk that looks valid.
- Reject chunk IDs that fail `^[A-Za-z0-9_.-]+$`.
- No secrets and no hardcoded IPs. Use environment variables or CLI arguments (`BIND_HOST`, `METADATA_HOST`, `METADATA_PORT`, `--advertise-host`, `--meta-host`).
- Servers bind to `0.0.0.0`. A node's heartbeat sends its ADVERTISE host, never `127.0.0.1`.
- Clients connect to the host and port the metadata server returns, never a hardcoded node address.
- Keep files focused; prefer small functions. Do not reformat unrelated code.
## Failure-handling rules
- A node is dead after silence longer than `FAILURE_TIMEOUT`, never after one missed heartbeat.
- Mark a node `DEAD` exactly once so recovery is not triggered repeatedly. Mark it `ALIVE` again when its heartbeat returns.
- Re-replication copies node to node: the metadata server sends `REPLICATE_CHUNK` to a source node, and file data never passes through the metadata server.
- Recovery uses a producer-consumer `queue.Queue` with worker threads. De-duplicate in-flight tasks, retry a failed task up to 3 times, then log and drop it (the next monitor pass re-queues it).
- Never crash a worker on an exception.
- Handle explicitly: no surviving replica (log "chunk lost", do not loop), fewer alive nodes than the replication factor (warn and replicate as far as possible), source dying mid-copy (retry another source).
- Placement and re-replication only use `ALIVE` nodes with enough free space (`free_bytes - chunk_size >= RESERVE`), and never put two replicas of a chunk on one node.
- Data nodes reply `disk full` instead of crashing when a write would break the reserve.
- Verification: the metadata server asks a node to verify (`VERIFY_CHUNK`); the node recomputes the checksum locally and replies ok or corrupt. File data never travels to the metadata server for verification.
- Client downloads try each replica in turn, skipping dead nodes and any copy whose checksum fails.
## Testing rules
- Add a pytest test for every new function.
- `python -m pytest -q` must pass before a change is considered done.
- Tests must not depend on fixed ports; bind port 0 or find a free port.
- Tests must not sleep longer than 3 seconds. Make timeouts configurable (for example an injectable `FAILURE_TIMEOUT`) instead.
- Integration tests cover: upload, kill a node, wait for recovery, assert every chunk is back to 2 `STORED` replicas on alive nodes, and assert the download is byte-identical.
## Docker rules
- One image (`Dockerfile`) serves every component; services differ only by command.
- Nodes use named volumes for chunk storage. The metadata server uses a volume for `metadata.db`.
- Nodes are NOT set to auto-restart, so `docker compose stop nodeN` stays stopped during the demo.
- The client runs as a container on the same network (`docker compose run --rm client ...`).
- Never expose these ports to the internet. The system assumes a trusted network.
## Definition of done
- Tests pass.
- `docs/protocol.md` and `docs/schema.sql` are updated if messages or tables changed.
- No files outside the task's ownership area were edited.
- `docs/roadmap.md` status is updated for the item you completed.
- An entry is added to `agents/HANDOFF.md`.
## Do-not list
- Do not send file data through the metadata server.
- Do not add authentication, a login page, or new dependencies.
- Do not rename folders or change the invariants above.
- Do not reformat unrelated code.
- Do not delete or weaken tests to make them pass.
- Do not implement roadmap items other than the one named in your task.
- Do not guess when the repo contradicts this file; stop and report the conflict in `agents/HANDOFF.md`.
 