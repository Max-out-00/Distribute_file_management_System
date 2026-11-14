# v4 - what changed in v2 and why

Every item below is wired into the real upload / download / failure / restart flows. None is a standalone demo.

| # | Feature | Real problem it solves | Where | OS concept |
|---|---|---|---|---|
| 1 | Data-node worker pool + bounded FIFO queue | one thread per connection is unbounded; a burst could exhaust memory | `data_node/node_server.py` | thread pool, producer-consumer, backpressure |
| 2 | Graceful shutdown (SIGTERM / Ctrl+C) with `LEAVE_NODE` | a stopped node was only noticed after 10 s, delaying recovery; in-flight requests were cut off | `data_node/init.py`, `heartbeat.leave`, `queries.leave_node` | signals, graceful vs abrupt termination |
| 3 | Crash recovery at startup | a crash mid-write leaves half-written `*.tmp` files forever | `ChunkStorage.recover` | file system recovery (fsck idea), atomic rename |
| 4 | Metadata <-> node reconciliation | files deleted while a node was down stayed on its disk (orphans); chunks lost from disk stayed "stored" in the database | `queries.reconcile_node`, `heartbeat._collect_orphans` | consistency between two stores, garbage collection with a grace period |
| 5 | Storage-directory file lock | two node processes on one folder corrupt each other's chunks | `data_node/dirlock.py` | OS file locking, released automatically on crash |
| 6 | Disk-space reserve | a node could fill its disk completely | `node_server` PUT_CHUNK, `MIN_FREE_BYTES` | resource management |
| 7 | Chunk cache (LRU / FIFO / Clock) | repeated reads hit the disk every time | `data_node/cache.py`, `ChunkStorage.read_chunk` | page replacement, frames, hit ratio |
| 8 | Parallel upload with a bounded buffer | sequential upload waits for every round trip | `client/pipeline.py`, `cli upload --parallel N` | semaphores (empty/full/mutex), producer-consumer |
| 9 | Per-node replication slots | many simultaneous copies fill one node's worker pool | `failure_detector/node_slots.py` | counting semaphore, ordered acquisition (deadlock prevention) |
| 10 | Direct-access read | reading 10 bytes downloaded the whole file | `client/download.py: read_range`, `cli read` | direct access vs sequential access, address translation |
| 11 | Process manager | six terminals to start a cluster; no way to demo crash vs graceful stop | `scripts/cluster.py` | process creation, PIDs, SIGTERM vs SIGKILL |
| 12 | Fix: no false `chunk lost` while uploading | chunks still being uploaded were logged as lost | `failure_detector/replicator.py` | |

## How the pieces fit together
Node starts: lock directory -> remove `*.tmp` -> start worker pool -> REGISTER (sends its chunk list) -> metadata reconciles and
returns orphans -> node deletes old orphans. Node stops with SIGTERM: stop heartbeats -> LEAVE_NODE -> stop accepting -> finish
in-flight requests -> release lock. Node crashes (SIGKILL): nothing runs; the lock is released by the OS, the monitor notices
after the heartbeat timeout, and the next start cleans up and reconciles.

## Settings (all optional)
`CHUNK_CACHE_CHUNKS` (8; 0 disables), `CHUNK_CACHE_POLICY` (lru | fifo | clock), `MIN_FREE_BYTES` (64 MiB),
`ORPHAN_GRACE_SECONDS` (30), `PLACEMENT_POLICY` (round_robin | most_free).

## Known limits
Graceful stop via `cluster.py` needs POSIX signals; on Windows `cluster.py` stop is abrupt, but pressing Ctrl+C in a node's
own terminal is graceful. Upload and download take no locks. No checksum scrubber, no `FILES.STATUS`, no directories, no access control.

## Dashboard delete
Each file card has a Delete button (with a confirmation). It calls `DELETE /api/files/<file_id>`, which runs the same delete as `cli delete`: metadata first, then every replica on the data nodes, then the dashboard list. Copies on offline nodes are removed when those nodes restart (orphan cleanup).
