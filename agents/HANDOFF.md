# Agent handoff log

## 2026-11-15 — Data-node startup conflict repair
- What changed: Resolved the data-node entry-point and storage merge conflicts, restored chunk-cache support, temporary-write recovery, per-directory locking, graceful shutdown, and imported the shared constants used by the node server.
- Files touched: `data_node/init.py`, `data_node/storage.py`, `data_node/cache.py`, `data_node/dirlock.py`, `data_node/node_server.py`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: None.
- Open problems: Other unresolved conflict markers remain outside the metadata and data-node startup paths.
- What the next agent must know: Run the node entry point from the project root and use a separate storage directory per node.

## 2026-11-15 — Dashboard startup conflict repair
- What changed: Resolved dashboard Python, template, and stylesheet conflicts in favor of the current asynchronous operations console, and repaired dashboard database compatibility with its existing local-history schema.
- Files touched: `dashboard/app.py`, `dashboard/templates/index.html`, `dashboard/static/style.css`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: None.
- Open problems: The dashboard home page was smoke-tested successfully. The existing dashboard test module still expects legacy API/helper behavior not present in the selected console implementation; `tests/test_metadata.py` also contains unresolved conflict markers.
- What the next agent must know: `DASHBOARD_DATABASE` overrides the local history database path; use a temporary path when testing module startup to avoid changing the existing dashboard database.

## 2026-11-15 — UUID upload failure from legacy metadata schema
- What changed: Metadata initialization now detects an INTEGER `FILES.FILE_ID` column. It rebuilds that table only when empty and refuses to discard populated legacy file rows. Added regressions for both cases.
- Files touched: `metadata_server/queries.py`, `tests/test_capacity_replication.py`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: Empty integer-key schema migration and populated-schema preservation.
- Open problems: The running metadata server must be restarted to apply this migration.
- What the next agent must know: The active local metadata DB had an empty INTEGER-key `FILES` table; restarting the server safely migrates it to TEXT UUID keys while preserving node registrations.

## 2026-11-15 — Metadata startup conflict repair
- What changed: Resolved the startup-blocking merge conflicts in shared configuration, metadata placement queries, and the upload client import path. Restored the missing metadata placement policies needed by upload planning.
- Files touched: `shared/constants.py`, `metadata_server/queries.py`, `metadata_server/placement.py`, `client/upload.py`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: None.
- Open problems: Other unresolved conflict markers remain outside the metadata-server startup path.
- What the next agent must know: Start commands from the project root. Upload planning enforces the configured replica count and available per-node capacity.

## 2026-11-15 — Adaptive chunking and replica-integrity contract change notice
- Planned changes: Expose `free_bytes` through `LIST_NODES`, return each stored `chunk_size` from `GET_LOCATIONS`, document `REPORT_CORRUPT`, and select upload chunk sizes from live replica capacity while retaining the configured 4 MiB maximum.
- New or changed message types: `REPORT_CORRUPT` removes a checksum-failing STORED location so the existing monitor can replenish it from a verified source.
- Tests planned: Adaptive sizing and capacity failure, variable-size chunk reconstruction, and corruption report/fallback/re-replication.
- What the next agent must know: Keep the configured replication factor at 2; three replicas are hypothetical in the user's questions and are not the current project setting.

## 2026-11-15 — Adaptive chunking and replica integrity implemented
- What changed: Added shared capacity-aware planning for sequential, parallel, and dashboard uploads; returned per-node free space and chunk sizes in metadata responses; added chunk-size validation and corruption reporting with monitor-driven replacement; dashboard now displays live node capacity and all reported nodes.
- Files touched: `client/chunker.py`, `client/upload.py`, `client/pipeline.py`, `client/download.py`, `metadata_server/handlers.py`, `metadata_server/queries.py`, `dashboard/app.py`, `dashboard/templates/index.html`, `docs/protocol.md`, `README.md`, and focused tests.
- Tests added: Capacity-based selection, insufficient capacity/max-count errors, capacity-race replanning, empty-file completion, corruption report removal, dashboard capacity display, adaptive parallel upload/reconstruction, and corrupt-copy fallback/re-replication.
- Validation: Focused suite passed: 24 tests; changed Python modules and tests compile.
- Limitations: Replication remains 2 by default. Corruption is detected during client downloads, not by a periodic scrubber. Restart the running metadata server before using the new protocol or the earlier schema migration.

## 2026-11-12 - Capacity and replica protocol change notice
- Planned changes: Set the configured replication factor to 2; add FILES.STATUS (UPLOADING / COMPLETE) with an additive migration preserving existing rows; add REPLAN_UPLOAD for in-progress placement after node rejection; add REPORT_CORRUPT to exclude checksum-failing replicas from healthy stored locations.
- docs/protocol.md will be updated before implementation. No chunk creation, chunk sizing, indexing, counting, reconstruction, or dashboard UI changes are planned.
- Preserve existing database and storage content, including modified lock files and untracked chunk files in this worktree.
- There is no periodic VERIFY_CHUNK operation. This task will not invent one. Existing upload/download and direct REPLICATE_CHUNK checks remain checksum-verified.


## 2026-11-03 — Repository setup / baseline
- What changed: Added repository coding-agent rules and handoff log.
- Files touched: `AGENTS.md`, `.github/copilot-instructions.md`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: None.
- Open problems: Existing metadata server, SQLite schema, and client/server message conventions are legacy and do not yet match the requested architecture. User confirmed to proceed with the requested architecture as the target. Existing uncommitted edits in `client/cli.py` and `client/download.py` must be preserved and integrated.
- What the next agent must know: Before changing contract files (`shared/`, `docs/protocol.md`, `docs/schema.sql`), record the intended contract changes here; update dependent components in the same change.

## 2026-11-03 — Failure recovery / contract change notice
- What changed: Prepared the contract update needed for metadata, heartbeat, client, and replica recovery traffic.
- Files touched: `agents/HANDOFF.md` only (advance notice; implementation follows in the same task).
- New or changed message types: Document `REGISTER_NODE`, `HEARTBEAT`, `LIST_NODES`, `PLAN_UPLOAD`, `CHUNK_STORED`, `GET_LOCATIONS`, `PING`, and `REPLICATE_CHUNK`; specify data-node `PUT_CHUNK`, `GET_CHUNK`, and `PING` fields and replies.
- Tests added: None yet.
- Open problems: The existing message clients use mixed `cmd` and `type` keys; protocol docs will establish `type` for metadata messages and retain `cmd` for existing node messages.
- What the next agent must know: Update `shared/constants.py`, `docs/protocol.md`, and `docs/schema.sql` before implementing dependent metadata/client/node behavior; keep the existing `cmd` data-node interface compatible.

## 2026-11-03 — Failure recovery implemented
- What changed: Replaced the legacy metadata text server and incompatible empty/old schemas with a JSON-framed SQLite metadata service; implemented node registration/heartbeats, upload planning, replica confirmation/lookup, timed failure detection, a deduplicated retrying worker pool, direct node-to-node recovery, and client upload/download failover.
- Files touched: `shared/constants.py`; `docs/protocol.md`, `docs/schema.sql`; `metadata_server/db.py`, `metadata_server/queries.py`, `metadata_server/handlers.py`, `metadata_server/server.py`, `metadata_server/init.py`; `data_node/heartbeat.py`, `data_node/node_server.py`, `data_node/storage.py`; `failure_detector/monitor.py`, `failure_detector/replicator.py`, `failure_detector/worker_pool.py`; `client/upload.py`, `client/download.py`; `tests/test_heartbeat.py`, `tests/test_metadata.py`, `tests/test_integration.py`.
- New or changed message types: Added metadata `REGISTER_NODE`, `HEARTBEAT`, `LIST_NODES`, `PLAN_UPLOAD`, `CHUNK_STORED`, `GET_LOCATIONS`, and `PING`; documented checksum-verified `REPLICATE_CHUNK` and existing node `PUT_CHUNK`, `GET_CHUNK`, `DELETE_CHUNK`, `HAS_CHUNK`, and `PING`.
- Tests added: Heartbeat timeout/dead transition, under-replicated query and target selection, and three-node upload/failure/recovery/download integration.
- Open problems: Docker startup arguments/environment, healthchecks, Compose services, and demo scripts remain for Task 3. Existing tracked legacy metadata DB is empty; initialization migrates its empty schema and refuses to discard populated incompatible tables.
- What the next agent must know: `PLAN_UPLOAD` uses `file_size` because `size` is reserved for framed payload length. Metadata uses `type`; data nodes retain `cmd`. Current entry points are `MetadataServer(host, port, db_path, ...)`, `DataNodeServer(host, port, storage)`, and `upload_file(path)`. `python -m pytest -q`: 6 passed.

## 2026-11-03 — Docker contract change notice
- What changed: Prepared the configuration contract update required by container networking.
- Files touched: `agents/HANDOFF.md` only (advance notice; implementation follows in Task 3).
- New or changed message types: None; retain the documented PING messages for service healthchecks.
- Tests added: None yet.
- Open problems: The generic Docker template currently starts a Flask dashboard; Task 3 will switch it to the metadata, three-node, and optional client services requested.
- What the next agent must know: Update `shared/constants.py` before wiring server and client environment defaults; metadata listens on its configured port, nodes bind separately from their advertised service host, and clients use metadata-returned node endpoints.

## 2026-11-03 — Framing validation change notice
- What changed: Identified malformed JSON header shapes as able to escape data-node request validation.
- Files touched: `agents/HANDOFF.md` only (advance notice).
- New or changed message types: None; retain the existing framing format.
- Tests added: A malformed-header regression test will be added with the parser guard.
- Open problems: None.
- What the next agent must know: `shared/message.py` is a CONTRACT file; validate that decoded headers are JSON objects and reject non-integer payload sizes, then handle the resulting `ValueError` in the data-node server.

## 2026-11-03 — Docker and final validation
- What changed: Added the Python 3.12 image, Compose metadata/three-node/client-profile services, per-service PING checks, portable recovery demos, environment-driven network configuration, and Docker run instructions. Hardened malformed framed-header handling.
- Files touched: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `README.md`; `shared/constants.py`, `shared/message.py`; `metadata_server/server.py`; `data_node/init.py`, `data_node/node_server.py`; `scripts/healthcheck.py`, `scripts/demo.py`, `scripts/run_demo.sh`, `scripts/run_demo.ps1`; `tests/test_docker_support.py`, `tests/test_message_protocol.py`; `agents/HANDOFF.md`.
- New or changed message types: None; healthchecks use the documented metadata and node PING messages.
- Tests added: PING/demo-helper checks and malformed JSON-header/payload-size rejection. Final `python -m pytest -q`: 10 passed. Both Compose configurations validate; PowerShell and Bash demo scripts pass syntax checks.
- Open problems: `docker compose build` could not run because the Docker Desktop Linux engine is unavailable in this environment. Runtime image/service behavior remains to be verified on a host with Docker running.
- What the next agent must know: The client profile uses `METADATA_HOST=metadata` and `METADATA_PORT=5000`; node advertised hosts are Compose service names. The recovery demo uses `--no-deps` for the post-failure download so stopped `node2` remains stopped.

## 2026-11-06 — Local cleanup and run guide
- What changed: Removed generated caches and demo artifacts, kept the source tree focused on the working application, and added a concise local run guide to `README.md`.
- Files touched: `README.md`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: None.
- Open problems: None.
- What the next agent must know: Use the local metadata server + three-node startup sequence from the README before running the CLI or demo script; generated chunk files/database artifacts are recreated automatically when needed.
<<<<<<< Updated upstream
=======

## 2026-11-10 — Dashboard download filename
- What changed: Fixed browser downloads to use the uploaded filename instead of the hardcoded `download.bin`.
- Files touched: `dashboard/templates/index.html`, `tests/test_dashboard.py`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: Dashboard regression test verifies the rendered filename, browser download assignment, and response attachment name.
- Open problems: None.
- What the next agent must know: The dashboard stores the sanitized upload filename in its local SQLite file log and includes it on each download link.

## 2026-11-10 — Live dashboard and transfer progress
- What changed: Added live node and replica-placement refresh, a clearer uploaded-file library, an all-replicas chunk-to-node table, and asynchronous uploads with end-to-end progress.
- Files touched: `dashboard/app.py`, `dashboard/templates/index.html`, `dashboard/static/style.css`, `client/upload.py`, `tests/test_dashboard.py`, `agents/HANDOFF.md`.
- New or changed message types: None. The dashboard reuses `LIST_NODES`, `GET_LOCATIONS`, and existing upload messages.
- Tests added: Dashboard tests cover live-node rendering/API, replica table rows, and upload progress updates.
- Open problems: None.
- What the next agent must know: `client.upload.upload_file` accepts an optional `(completed_chunks, total_chunks)` callback. Dashboard upload job state is in-process memory and is intended only for the current single-process Flask dashboard.

## 2026-11-10 — Metadata datatype mismatch and stale dashboard entries
- What changed: Metadata startup now detects a legacy `FILES.FILE_ID INTEGER PRIMARY KEY` schema, rebuilds it when related tables are empty, and refuses to discard populated data. The dashboard file API also skips local file records that no longer exist in metadata instead of failing the whole refresh.
- Files touched: `metadata_server/queries.py`, `dashboard/app.py`, `tests/test_metadata.py`, `tests/test_dashboard.py`, `agents/HANDOFF.md`.
- New or changed message types: None.
- Tests added: Regressions cover safe initialization of an empty legacy schema, successful UUID upload-plan insertion, preservation of populated incompatible metadata, and stale dashboard-file handling.
- Open problems: Existing metadata servers must be restarted to run the corrected schema initialization.
- What the next agent must know: The current local `metadata_server/metadata.db` contains no file/chunk rows; restart the metadata server to replace its incompatible empty `FILES` table. Do not delete the database.


## OS-algorithms extension
- shared/constants.py: added PLACEMENT_POLICY (env, default round_robin), REQUEST_WORKERS, REQUEST_QUEUE_SIZE, LOCK_TIMEOUT.
- metadata_server/placement.py: RoundRobinPlacement / MostFreePlacement; create_upload_plan now tracks free bytes inside a plan.
- metadata_server/server.py: bounded FIFO queue + fixed worker pool instead of thread-per-connection; full queue => "server busy".
- metadata_server/lock_manager.py: named locks, ordered acquisition (prevention) and wait-for-graph detection; used by DELETE_FILE and the replicator.
- failure_detector/replicator.py: PriorityQueue (fewest live copies first, FIFO tie-break); takes a chunk lock per replication.
- DELETE_FILE implemented (handlers.py, queries.delete_file, client/delete.py, `cli delete`). Still no orphan GC, no FILES.STATUS.
- tests/test_scheduling.py, tests/test_delete.py, scripts/os_demo.py added.

## v4: OS features integrated into the real flows
- data_node/node_server.py: bounded worker pool + 'node busy' reply, disk reserve (MIN_FREE_BYTES), STATS command.
- data_node/init.py: dirlock (data_node/dirlock.py), storage.recover() for *.tmp, signal handlers, graceful leave.
- data_node/heartbeat.py + metadata handlers/queries: LEAVE_NODE, REGISTER_NODE reconcile (orphans, removed_replicas).
- data_node/cache.py (LRU/FIFO/Clock) used by ChunkStorage.read_chunk; invalidated on write/delete.
- failure_detector/node_slots.py: per-node replication slots; replicator no longer reports in-progress uploads as lost.
- client/pipeline.py (--parallel), client/download.read_range (read), scripts/cluster.py (kill-node = crash, stop-node = graceful).
- Known gaps: no FILES.STATUS, no checksum scrubber, no directories, no access control, upload/download take no locks.
- Dashboard: DELETE /api/files/<file_id> in dashboard/app.py, Delete button in dashboard/templates/index.html (server-rendered card and renderFiles), .danger-btn in style.css, tests in tests/test_dashboard.py.


## Capacity management and replication follow-up
- Files changed in this continuation: data_node/storage.py, data_node/node_server.py, data_node/heartbeat.py, data_node/init.py, metadata_server/queries.py, client/upload.py, client/pipeline.py, client/download.py, tests/test_capacity_replication.py, tests/test_dashboard.py, tests/test_delete.py, tests/test_docker_support.py, tests/test_integration.py, tests/test_metadata.py, tests/test_node_lifecycle.py, tests/test_os_features.py, docs/roadmap.md, agents/HANDOFF.md.
- Implemented: NODE_CAPACITY_BYTES and --capacity with CLI priority; real disk free fallback; completed chunk accounting excluding .tmp; atomic capacity-checked replacement; clear disk-full replies; accurate capacity heartbeats; reserve-aware planning from ALIVE nodes with configured factor; planned capacity consumption; complete-file gating after all required replicas confirmed; client failure unless all configured replicas are confirmed; rotating download replica order with SHA-256 fallback; existing direct-node recovery retained.
- Validation: python -m pytest -q passed 56 tests; py_compile passed on changed Python modules; Docker image built; rebuilt three-node stack healthy; Docker smoke upload/download verified payload and two distinct replicas. Existing three-node volumes and metadata database preserved. Docker heartbeat free space observed for each node; demo artificial 20/20/8 MB capacities were not configured.
- Limitations / approval needed: shared/constants.py and docs/protocol.md are protected and were left as previously changed. The existing worktree already has REPLICATION_FACTOR=2 and RESERVE_BYTES=100 MiB. Approval is still required to add documented protocol operations for fresh placement after stale plans and corruption reporting, plus explicit upload-abort cleanup. No periodic VERIFY_CHUNK/scrubbing operation exists; none was invented. A failed upload can leave an UPLOADING metadata record and already-written replicas because current protocol has no abort operation.
- User changes preserved: modified metadata.db, node lock files, and untracked node chunk files were not deleted or reset.
- Chunking status: client-side chunk creation, chunk size, indexes, counts, reconstruction, and dashboard chunk-count display were not modified.
>>>>>>> Stashed changes
