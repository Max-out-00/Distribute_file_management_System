
from uuid import uuid4

from client.upload import call
from shared import constants

node_id = f"api-test-node-{uuid4().hex}"

# Register a data node through the metadata server.
response, _ = call(
    constants.HOST,
    constants.METADATA_PORT,
    {
        "command": "REGISTER_NODE",
        "node_id": node_id,
        "address": "localhost",
        "disk_used": 0,
        "disk_total": 1000000,
    },
)

print("Register response:", response)
assert response["ok"] is True

# Ask the server for the live nodes.
response, _ = call(
    constants.HOST,
    constants.METADATA_PORT,
    {"command": "GET_LIVE_NODES"},
)

print("Live-node response:", response)
assert response["ok"] is True
assert any(
    node["node_id"] == node_id
    for node in response["nodes"]
)

print("Metadata server API test passed!")
