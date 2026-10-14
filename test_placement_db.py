
from uuid import uuid4

from metadata_server.queries import (
    register_file,
    register_chunk,
    register_node,
    get_nodes_for_chunk,
)
from metadata_server.placement import (
    select_nodes_for_chunk,
    record_successful_placements,
)

# Unique IDs allow the test to be run repeatedly.
test_id = uuid4().hex
file_id = f"test-file-{test_id}"
chunk_id = f"test-chunk-{test_id}"

nodes = [
    {
        "node_id": f"test-node-{test_id}-{i}",
        "status": "ALIVE",
    }
    for i in range(1, 4)
]

# Create the required database records.
register_file(file_id, "placement_test.bin", 100, "UPLOADING")
register_chunk(
    chunk_id,
    file_id,
    0,
    100,
    "test-checksum",
)

for node in nodes:
    register_node(
        node["node_id"],
        "localhost",
        0,
        1000000,
    )

# Choose two different nodes for this chunk.
selected_nodes = select_nodes_for_chunk(0, nodes)

print("Selected nodes:", [
    node["node_id"] for node in selected_nodes
])

# SIMULATION: pretend both selected nodes confirmed storage.
# Real data-node communication is not implemented here.
successful_node_ids = [
    node["node_id"] for node in selected_nodes
]

recorded = record_successful_placements(
    chunk_id,
    selected_nodes,
    successful_node_ids,
)

print("Recorded locations:", recorded)

# Verify that the locations were saved in the database.
stored_nodes = get_nodes_for_chunk(chunk_id)

print("Database locations:", [
    node["node_id"] for node in stored_nodes
])

assert len(recorded) == 2
assert len(stored_nodes) == 2
print("Placement and database integration test passed!")
