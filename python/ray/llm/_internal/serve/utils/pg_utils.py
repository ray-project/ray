"""Placement group utilities for Ray LLM Serve."""

from collections import defaultdict
from typing import Dict, List

import ray
from ray.util.annotations import PublicAPI
from ray.util.placement_group import PlacementGroup, placement_group_table


def _sort_bundle_indices_by_node(
    bundles_to_node_id: Dict[int, str],
    driver_node_id: str,
) -> List[int]:
    """Sort bundle indices so that same-node bundles are adjacent, driver node first.

    Args:
        bundles_to_node_id: Mapping from bundle index to node ID.
        driver_node_id: The node ID of the driver node.

    Returns:
        List of bundle indices sorted with driver node bundles first,
        then remaining nodes in deterministic order.
    """
    node_to_bundles: Dict[str, List[int]] = defaultdict(list)
    for bundle_idx, node_id in bundles_to_node_id.items():
        node_to_bundles[node_id].append(bundle_idx)

    for bundles in node_to_bundles.values():
        bundles.sort()

    result: List[int] = []
    if driver_node_id in node_to_bundles:
        result.extend(node_to_bundles.pop(driver_node_id))
    for node_id in sorted(node_to_bundles.keys()):
        result.extend(node_to_bundles[node_id])

    return result


@PublicAPI
def get_bundle_indices_sorted_by_node(
    pg: PlacementGroup,
) -> List[int]:
    """Return bundle indices sorted such that same-node bundles are adjacent, driver node first.

    When a placement group is provisioned, adjacent bundle indices don't
    necessarily map to the same physical node. This utility reorders bundle
    indices so that bundles on the same node are grouped together.

    The driver node's bundles come first so that rank 0 is co-located with the driver.

    Args:
        pg: A ready placement group.

    Returns:
        List of bundle indices sorted such that same-node bundles are adjacent, driver node first.
    """
    table = placement_group_table(pg)
    bundles_to_node_id = table["bundles_to_node_id"]
    driver_node_id = ray.get_runtime_context().get_node_id()
    return _sort_bundle_indices_by_node(bundles_to_node_id, driver_node_id)
