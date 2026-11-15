from shared import constants


class RoundRobinPlacement:
    def __init__(self):
        self._next_index = 0

    def choose(self, nodes, count):
        if count <= 0 or not nodes:
            return []
        start = self._next_index % len(nodes)
        ordered = nodes[start:] + nodes[:start]
        selected = ordered[:count]
        self._next_index = (start + len(selected)) % len(nodes)
        return selected


class MostFreePlacement:
    def choose(self, nodes, count):
        return sorted(
            nodes,
            key=lambda node: (-node["REMAINING_BYTES"], node["NODE_ID"]),
        )[:count]


def get_policy():
    policy = constants.PLACEMENT_POLICY.strip().lower()
    if policy == "round_robin":
        return RoundRobinPlacement()
    if policy == "most_free":
        return MostFreePlacement()
    raise ValueError(f"unknown placement policy {constants.PLACEMENT_POLICY!r}")