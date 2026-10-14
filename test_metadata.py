
from metadata_server.db import initialize_database
from metadata_server.queries import (
    register_file,
    get_file,
    update_file_status,
    register_chunk,
    get_chunks_for_file,
)

# Ensure the tables exist.
initialize_database()

# Use unique IDs so repeated test runs don't collide.
file_id = "test-file-001"
chunk_id = "test-chunk-001"

# Create a file record.
register_file(
    file_id=file_id,
    file_name="notes.pdf",
    size=1024,
)

# Read the file record.
file_info = get_file(file_id)
print("File record:", file_info)

# Change the file status.
updated = update_file_status(file_id, "AVAILABLE")
print("File status updated:", updated)

# Register one chunk belonging to this file.
register_chunk(
    chunk_id=chunk_id,
    file_id=file_id,
    chunk_index=0,
    size=1024,
    checksum="example-checksum",
)

# Read the chunks belonging to the file.
chunks = get_chunks_for_file(file_id)
print("Chunks:", chunks)
