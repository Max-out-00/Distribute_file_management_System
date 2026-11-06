# Project summary
- A simplified GFS/HDFS-style distributed file storage system.
- One metadata server stores file, chunk, and node metadata in SQLite.
- Data nodes store chunk bytes locally and report liveness with heartbeats.
- Clients fetch placement from metadata and exchange chunk bytes directly with data nodes.
- The wire format uses shared JSON-header framing and SHA-256 chunk checksums.

## Architecture invariants
- One metadata server (SQLite, standard library only) stores information ABOUT data. It never stores or relays file data.
- Many data nodes store chunk files and send heartbeats to the metadata server.
- The client asks the metadata server where chunks go, then sends/fetches chunk bytes DIRECTLY to/from the nodes.
- Wire format: [4-byte big-endian header length][JSON header][optional raw payload], via `send_msg`/`recv_msg` in `shared/message.py`. Reuse these; never hand-roll socket framing.
- Replication factor 2 (`shared/constants.py`); SHA-256 checksum per chunk.
- Standard library only, except pytest (tests) and Flask (optional dashboard).
- Run everything from the repo root with `python -m package.module`. Package folders use underscores.
- Tables: `NODES(NODE_ID, HOST, PORT, FREE_BYTES, LAST_HEARTBEAT, STATUS)`, `FILES`, `CHUNKS`, `CHUNKS_LOCATION(CHUNK_ID, NODE_ID, STATUS)`. `NODES.STATUS` is `ALIVE` or `DEAD`; `CHUNKS_LOCATION.STATUS` is `PENDING` or `STORED`. All SQL uses `?` placeholders and lives only in `metadata_server/queries.py`.

## Ownership map
Each agent edits only its own area unless the task explicitly says otherwise:
- `metadata_server/`: metadata/DB agent.
- `data_node/`: data node agent.
- `failure_detector/`: failure-handling agent.
- `client/`: client agent.
- `shared/`, `docs/protocol.md`, `docs/schema.sql`: CONTRACT files. Before changing one, add a note to `agents/HANDOFF.md`; update every dependent component in the same change.
- `Dockerfile`, `docker-compose.yml`, `scripts/`: infra agent.

## Protocol rule
Every message type must be documented in `docs/protocol.md` with its fields, an example, reply, and error cases. Add or change the documentation before the code. Never invent an undocumented message type.

## Coding rules
- Use plain functions for logic; one `Handler` class per server; no custom socket classes.
- Every socket has a timeout. Do not use bare `except:`.
- Servers must not crash on bad input; return `{"status":"error","error":"..."}`.
- Validate required message fields and return a clear error.
- Use a fresh SQLite connection per request or per thread; never share connections across threads.
- Guard shared in-memory state with a lock.
- Write chunks to a `.tmp` file, then use `os.replace`.
- Reject chunk IDs that fail `^[A-Za-z0-9_.-]+$`.
- Do not add secrets or hardcoded IPs; use environment variables or CLI arguments.

## Testing rules
- Add a pytest test for every new function.
- `python -m pytest -q` must pass before a change is considered done.
- Tests must not depend on fixed ports; bind port 0 or use free ports.
- Tests must not sleep longer than 3 seconds; make timeouts configurable instead.

## Definition of done
- Tests pass.
- `docs/protocol.md` is updated if messages changed.
- No files outside the task's ownership area are edited.
- An entry is added to `agents/HANDOFF.md`.

## Do-not list
- Do not send file data through the metadata server.
- Do not add auth, a login page, or new dependencies.
- Do not rename folders.
- Do not reformat unrelated code.
- Do not delete tests to make them pass.

## File seperation
- File show their uploaded date and time.
- New files that are uploaded or are being uploaded.

## Upload and Download Progress bar
- Displaying a progression bar that shows the process in percentage.
- The bar should be visible, in green color.
- Display the download progress.

## Node Verification
Any node containing data should be matched with the original Upload in order to check the integrity of the nodes that contain the data of the set file. If any data should be corrupted and lost, data recovery must commence.

## Automatic Node recovery and Automatic Notification
When a node containing the data goes offline or is corrupted it should give it notification and work to fix the lost/corrupted data. When this corruption or data loss is found the user must be notified of it.

## Node Overloading Fix
While data storing in the nodes, if any given node does not contain the adiquit storage for the chunk to be saved. The program must scan all the nodes and fist find the highest capacity node and store the data in those nodes. 

## Node overloading Solution
-When any file is uploaded. And divided into chunks, they will be stored in a set of 5 nodes, with the chunks of the said file stored inside them equally. 
- If the nodes with a set cannot hold the chunk files due to their size, a new set of nodes will be create while preserving the old chunk data, in the old set. And the chunks will be stored in a different set of 5 nodes.

## Multi File Upload/Download
- The user must be able to upload the multiple files at once. 
- At the time of upload and download the program must scan the files and upload or download the files based on their storage size. The smallest size files should be uploaded or downloaded first.

## Data log
- Upon uploading a file, the data will get it's unique ID for that perticular file and it's uploaded date will be recoarded.