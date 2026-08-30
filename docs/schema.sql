CREATE TABLE IF NOT EXISTS files (
    file_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    size INTEGER NOT NULL,
    checksum TEXT NOT NULL,

    FOREIGN KEY (file_id)
        REFERENCES files(file_id),

    UNIQUE (file_id, chunk_index)
);


CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    last_heartbeat DATETIME,
    disk_used INTEGER,
    disk_total INTEGER,
    status TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS chunk_location (
    chunk_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL,

    PRIMARY KEY (chunk_id, node_id),

    FOREIGN KEY (chunk_id)
        REFERENCES chunks(chunk_id),

    FOREIGN KEY (node_id)
        REFERENCES nodes(node_id)
);