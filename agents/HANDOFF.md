# Agent handoff log

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
