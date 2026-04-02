from typing import Any, Dict

from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

HEAD_NODE_RESOURCE_CONSTRAINT = 0.001


def head_node_placement_options() -> Dict[str, Any]:
    """Return placement options for detached singleton actors on the head node.

    Use a head-node resource constraint to pin placement, and explicitly opt out of
    any parent placement group capture so these actors don't retain placement-group
    resources after the workload finishes.
    """
    return {
        "resources": {HEAD_NODE_RESOURCE_NAME: HEAD_NODE_RESOURCE_CONSTRAINT},
        "scheduling_strategy": PlacementGroupSchedulingStrategy(placement_group=None),
    }
