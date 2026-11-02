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
- Reply: `{"status":"ok","nodes":[{"node_id":"node1","host":"node1","port":5001,"alive":true}]}`
- Errors: database errors.

### `PLAN_UPLOAD`
- Request: `{"type":"PLAN_UPLOAD","file_name":"sample.bin","file_size":12,"chunks":[{"chunk_id":"file_0","chunk_index":0,"chunk_size":12,"checksum":"<sha256>"}]}`
- Reply: `{"status":"ok","file_id":"<id>","chunks":[{"chunk_id":"file_0","chunk_index":0,"checksum":"<sha256>","nodes":[{"node_id":"node1","host":"node1","port":5001}]}]}`
- Errors: invalid fields, no ALIVE node, or database errors. Each chunk is planned on up to the replication factor of ALIVE nodes.

### `CHUNK_STORED`
- Request: `{"type":"CHUNK_STORED","chunk_id":"file_0","node_id":"node1"}`
- Reply: `{"status":"ok"}`
- Errors: unknown chunk or node, or database errors. Marks the confirmed location `STORED`.

### `GET_LOCATIONS`
- Request: `{"type":"GET_LOCATIONS","file_id":"<id>"}`
- Reply: `{"status":"ok","chunks":[{"chunk_id":"file_0","chunk_index":0,"checksum":"<sha256>","nodes":[{"node_id":"node1","host":"node1","port":5001,"alive":true}]}]}`
- Errors: missing/unknown file or database errors. Nodes include dead locations so clients can skip them.

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