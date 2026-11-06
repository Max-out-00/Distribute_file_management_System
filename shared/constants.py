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
