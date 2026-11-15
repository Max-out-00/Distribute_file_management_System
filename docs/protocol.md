# Wire protocol

All messages use `shared.message.send_msg` / `recv_msg`: a 4-byte big-endian
JSON-header length, the UTF-8 JSON header, and an optional raw payload. The
framing helper adds `size` when a payload is sent. Replies use the same format.
Metadata requests identify their message with `type`; data-node requests use
the existing `cmd` field. Errors are `{"status":"error","error":"..."}`.

## Metadata server messages

### `PING`
- Request: `{"type":"PING"}`
- Reply: `{"status":"ok"}`
- Errors: socket or framing errors.

### `REGISTER_NODE` and `HEARTBEAT`
- Request: `{"type":"REGISTER_NODE","node_id":"node1","host":"node1","port":5001,"free_bytes":1000}`. `HEARTBEAT` uses the same required fields. Optional `chunk_ids` reports local chunk IDs on registration.
- Reply: `{"status":"ok"}`
- Errors: missing/invalid node ID, host, port, or free space.
- Both refresh `LAST_HEARTBEAT` and set `NODES.STATUS` to `ALIVE`.

### `LIST_NODES`
- Request: `{"type":"LIST_NODES"}`
- Reply: `{"status":"ok","nodes":[{"node_id":"node1","host":"node1","port":5001,"alive":true,"free_bytes":1000}]}`
- `free_bytes` is the latest heartbeat value; clients and upload planning consider only ALIVE nodes.
- Errors: database errors.

### `PLAN_UPLOAD`
- Request: `{"type":"PLAN_UPLOAD","file_name":"sample.bin","file_size":12,"chunks":[{"chunk_id":"file_0","chunk_index":0,"chunk_size":12,"checksum":"<sha256>"}]}`
- Reply: `{"status":"ok","file_id":"<id>","chunks":[{"chunk_id":"file_0","chunk_index":0,"checksum":"<sha256>","nodes":[{"node_id":"node1","host":"node1","port":5001}]}]}`
- Errors: invalid fields, fewer than the replication-factor number of ALIVE nodes, insufficient eligible capacity, or database errors. Each chunk is planned on exactly the configured replication factor of distinct ALIVE nodes.
- Clients choose a per-file chunk size from the capacity reported by the `LIST_NODES` reply: the chunk must fit, including `RESERVE_BYTES`, on at least the replication-factor number of distinct ALIVE nodes. The current 4 MiB `CHUNK_SIZE` is the maximum, not a required size. A capacity race may cause the client to retry planning with smaller chunks. Upload still fails clearly if no valid placement exists.
- Chunks remain whole on one node per replica; splitting is done by the client before planning. Chunk indexes are contiguous from zero, sizes sum to `file_size`, and downloads reconstruct in index order.

### `CHUNK_STORED`
- Request: `{"type":"CHUNK_STORED","chunk_id":"file_0","node_id":"node1"}`
- Reply: `{"status":"ok"}`
- Errors: unknown chunk or node, or database errors. Marks the confirmed location `STORED`.

### `GET_LOCATIONS`
- Request: `{"type":"GET_LOCATIONS","file_id":"<id>"}`
- Reply: `{"status":"ok","chunks":[{"chunk_id":"file_0","chunk_index":0,"chunk_size":12,"checksum":"<sha256>","nodes":[{"node_id":"node1","host":"node1","port":5001,"alive":true}]}]}`
- Errors: missing/unknown file or database errors. Nodes include dead locations so clients can skip them.

### `REPORT_CORRUPT`
- Request: `{"type":"REPORT_CORRUPT","chunk_id":"file_0","node_id":"node1"}`
- Reply: `{"status":"ok","removed":true}` after the STORED location is removed from metadata.
- Errors: unknown chunk or node is not a STORED location for that chunk.
- The existing failure monitor sees the under-replicated chunk and attempts to restore the configured replica count using a surviving source whose bytes pass the source node's SHA-256 check.

## Data-node messages

The following requests use the `cmd` field.

### `PING`
- Request: `{"cmd":"PING"}`
- Reply: `{"status":"ok"}`
- Errors: socket or framing errors.

### `PUT_CHUNK`
- Request: `{"cmd":"PUT_CHUNK","chunk_id":"file_0","checksum":"<sha256>","size":12}` plus the chunk bytes.
- Reply: `{"status":"ok","checksum":"<sha256>"}`
- Errors: invalid chunk ID, checksum mismatch, missing payload, or storage error.

### `GET_CHUNK`
- Request: `{"cmd":"GET_CHUNK","chunk_id":"file_0"}`
- Reply: `{"status":"ok","checksum":"<sha256>","size":12}` plus the chunk bytes.
- Errors: invalid chunk ID, missing chunk, or storage error.

### `REPLICATE_CHUNK`
- Request to a source node: `{"cmd":"REPLICATE_CHUNK","chunk_id":"file_0","checksum":"<sha256>","target_host":"node2","target_port":5002}`
- The source reads its local chunk, verifies its SHA-256 checksum, and sends `PUT_CHUNK` with the bytes and checksum directly to the target.
- Reply: `{"status":"ok"}` after successful target storage.
- Errors: invalid/missing fields, missing source chunk, checksum mismatch, target unavailable, or target rejection.

### `DELETE_CHUNK` and `HAS_CHUNK`
- Requests: `{"cmd":"DELETE_CHUNK","chunk_id":"file_0"}` or `{"cmd":"HAS_CHUNK","chunk_id":"file_0"}`
- Replies: `{"status":"ok","deleted":true}` or `{"status":"ok","exists":true}`.
- Errors: invalid chunk ID or storage error.

## DELETE_FILE (metadata server)

Request: `{"type": "DELETE_FILE", "file_id": "<id>"}`
Reply: `{"status": "ok", "deleted_chunks": N, "unreachable_replicas": M}` or an error (`file not found`, lock timeout).
The server locks the file and all its chunks (sorted order), deletes the metadata in one transaction, then sends
`DELETE_CHUNK` to every node that held a replica. Replicas on unreachable nodes remain as orphans (no garbage collector yet).

## Overload

When the bounded request queue is full the server replies `{"status": "error", "error": "server busy, retry later"}`.


## STATS (data node)
Request: `{"cmd": "STATS"}`. Reply: `{"status": "ok", "chunks": N, "used_bytes": B, "cache": {policy, capacity, cached, hits, misses, evictions, hit_ratio}}`.

## Adaptive chunk selection
The client uses the replication-factor-th largest positive `free_bytes - RESERVE_BYTES` among ALIVE nodes as its per-chunk capacity ceiling, further capped at `CHUNK_SIZE` (4 MiB by default). This estimates the largest chunk that can be placed on the required number of distinct nodes. If planning races with changing capacity, the client retries with smaller chunks down to a 100,000-chunk-per-file guard. If the required size would exceed that guard or the cluster cannot hold all required replicas, upload fails rather than reducing replication or splitting a chunk across nodes.

## LEAVE_NODE (metadata server)
`{"type": "LEAVE_NODE", "node_id": "<id>"}` -> `{"status": "ok"}`. Sent by a data node that is shutting down gracefully;
the node is marked DEAD immediately so re-replication starts without waiting for the heartbeat timeout.

## REGISTER_NODE reply (extended)
When `chunk_ids` is sent, the reply also carries `orphans` (chunk files the database has no record of for this node;
the node deletes those older than ORPHAN_GRACE_SECONDS) and `removed_replicas` (STORED rows whose file is gone; they are
dropped so recovery re-replicates).

## Busy replies
A data node whose worker pool and queue are full answers `{"status": "error", "error": "node busy, retry later"}`.
A PUT_CHUNK that would leave less than MIN_FREE_BYTES free is refused with `insufficient disk space`.


## Capacity-aware placement and upload recovery extensions

### `PLAN_UPLOAD`
Each chunk is assigned exactly the configured replication factor of distinct ALIVE nodes. Candidates must report enough `free_bytes` for the planned chunk size plus `RESERVE_BYTES`; the default reserve is 100 MiB. Planned sizes are deducted while assigning subsequent chunks. The server returns an error naming a chunk if it cannot obtain all replicas. Non-empty file records start as `UPLOADING` and become downloadable only after all replicas are confirmed; empty files are immediately `COMPLETE` and require no data nodes.

`GET_LOCATIONS` includes chunk index, size, checksum, and each STORED node's ID, host, port, and `alive` availability. Clients skip unavailable nodes and verify SHA-256 before accepting downloaded bytes.

For each chunk, the client rotates its starting replica by `chunk_index` over the node-ID-sorted locations. It skips dead locations and tries the next copy after connection, node-read, size, or checksum failure. On a SHA-256 mismatch it sends `REPORT_CORRUPT`; the monitor then schedules replacement from a surviving replica. This detection is read-triggered, not a periodic storage scrub.

The configured replication factor is currently 2. All planned replicas are written during the initial upload and must be confirmed before a non-empty file becomes downloadable. If a node later becomes unavailable, or a read reports a corrupt copy, the failure monitor attempts to restore the factor from a verified surviving source. A replica is SHA-256 checked by the client on upload, by the source during direct replication, and by the client during download; the target node also verifies PUT_CHUNK before storing.

Data-node `PUT_CHUNK` can return an error containing `disk full` when the write exceeds the configured artificial capacity or actual disk free space. Rewriting an existing chunk charges only the replacement size delta. Temporary `.tmp` files are not counted as stored data.

There is currently no `VERIFY_CHUNK` operation and no periodic scrubber. This change does not add an undocumented verification message; the existing client and direct `REPLICATE_CHUNK` flows verify checksums.
