
from shared.constants import REPLICATION_FACTOR
from metadata_server.queries import (
    get_live_nodes,
    register_chunk_location,
)


def select_nodes_for_chunk(
    chunk_index,
    nodes,
    replication_factor=REPLICATION_FACTOR,
):
    """Select distinct storage nodes using Round Robin."""

    if chunk_index < 0:
        raise ValueError("chunk_index cannot be negative.")

    if replication_factor < 1:
        raise ValueError("replication_factor must be at least 1.")

    available_nodes = sorted(nodes, key=lambda node: node["node_id"])

    if len(available_nodes) < replication_factor:
        raise RuntimeError(
            f"Need {replication_factor} live nodes, "
            f"but only {len(available_nodes)} are available."
        )

    start_index = chunk_index % len(available_nodes)

    return [
        available_nodes[(start_index + offset) % len(available_nodes)]
        for offset in range(replication_factor)
    ]


def get_chunk_placement(chunk_index):
    """Select destinations from the live nodes in the database."""

    live_nodes = get_live_nodes()

    return select_nodes_for_chunk(chunk_index, live_nodes)


def record_successful_placements(
    chunk_id,
    selected_nodes,
    successful_node_ids,
):
    """
    Record locations only for selected nodes that confirmed storage.
    The caller must supply genuine storage confirmations.
    """

    selected_ids = {node["node_id"] for node in selected_nodes}
    successful_ids = set(successful_node_ids)

    if not successful_ids.issubset(selected_ids):
        raise ValueError(
            "A storage confirmation was received for an unselected node."
        )

    recorded_nodes = []

    for node in selected_nodes:
        node_id = node["node_id"]

        if node_id in successful_ids:
            register_chunk_location(
                chunk_id=chunk_id,
                node_id=node_id,
                status="AVAILABLE",
            )
            recorded_nodes.append(node_id)

    return recorded_nodes
