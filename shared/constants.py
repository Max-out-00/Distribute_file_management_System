import os

HOST = "localhost"
METADATA_HOST = os.environ.get("METADATA_HOST", HOST)
METADATA_PORT = int(os.environ.get("METADATA_PORT", "9999"))
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
REPLICATION_FACTOR = 3
HEARTBEAT_INTERVAL = 3     # seconds, used by data nodes
FAILURE_TIMEOUT = 10       # seconds of silence before a node counts as dead
CHUNK_SIZE = 4 * 1024 * 1024
REPLICATION_WORKERS = 3

# --- scheduling / concurrency ---
PLACEMENT_POLICY = os.environ.get("PLACEMENT_POLICY", "round_robin")  # or "most_free"
REQUEST_WORKERS = 8          # metadata-server worker threads
REQUEST_QUEUE_SIZE = 64      # bounded FIFO of accepted connections; full => "server busy"
LOCK_TIMEOUT = 10            # seconds to wait for a file/chunk lock

# --- memory / resource management ---
CHUNK_CACHE_CHUNKS = int(os.environ.get("CHUNK_CACHE_CHUNKS", "8"))   # frames per data node; 0 = off
CHUNK_CACHE_POLICY = os.environ.get("CHUNK_CACHE_POLICY", "lru")      # lru | fifo | clock
REPLICATIONS_PER_NODE = 2    # concurrent re-replications a node may take part in
NODE_WORKERS = 8             # data-node worker threads
NODE_QUEUE_SIZE = 64         # bounded FIFO of accepted connections per data node
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(64 * 1024 * 1024)))  # disk reserve
ORPHAN_GRACE_SECONDS = int(os.environ.get("ORPHAN_GRACE_SECONDS", "30"))       # never GC fresh files
