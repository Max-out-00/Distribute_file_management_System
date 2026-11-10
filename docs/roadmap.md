# Roadmap (implement ONLY the item named in your task)

## 1. File records [planned]
Each upload gets a uuid FILE_ID and CREATED_AT. A LIST_FILES message returns
filename, size, created_at, and status (UPLOADING / COMPLETE).
Files still uploading are listed separately from complete ones.

## 2. Progress bar [planned]
CLI upload and download show a green percentage bar (ANSI codes, no new
dependencies), updated per chunk.

## 3. Capacity-aware placement [planned]
Placement only uses ALIVE nodes with free_bytes - chunk_size >= RESERVE.
Each chunk's k replicas go on k distinct nodes, preferring most free space.
Free space is tracked in memory while planning. If fewer than k nodes
qualify, reject the upload. Nodes also reply "disk full" if a write
would break the reserve.

## 4. Chunk verification [planned]
Every 60s the metadata server sends VERIFY_CHUNK (chunk_id, checksum) to
nodes. The node recomputes SHA-256 locally and replies ok/corrupt.
Corrupt replicas are marked CORRUPT and re-replicated from a good copy.
File data never passes through the metadata server.

## 5. Notifications [planned]
Node death, corruption, and recovery events are written to an EVENTS table
and shown on the dashboard and via `cli events`.

## 6. Multi-file transfer [planned]
`upload a b c` and `download id1 id2` process files smallest-first, one at a
time (sorted by os.path.getsize / FILES.SIZE).

## 7. Node groups of 5 [stretch, needs >= 5 nodes]
Only start after items 1 to 6 are done and Compose has 5+ nodes.