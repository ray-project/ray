from typing import Union, Optional, TYPE_CHECKING
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.util.placement_group import PlacementGroup

# "DEFAULT": The default hybrid scheduling strategy
# based on config scheduler_spread_threshold.
# This disables any potential placement group capture.

# "SPREAD": Spread scheduling on a best effort basis.


@PublicAPI(stability="beta")
class PlacementGroupSchedulingStrategy:
    """Placement group based scheduling strategy.

    Attributes:
        placement_group: the placement group this actor belongs to,
            or None if it doesn't belong to any group.
        placement_group_bundle_index: the index of the bundle
            if the actor belongs to a placement group, which may be -1 to
            specify any available bundle.
        placement_group_capture_child_tasks: Whether or not children tasks
            of this actor should implicitly use the same placement group
            as its parent. It is False by default.
    """

    def __init__(
        self,
        placement_group: "PlacementGroup",
        placement_group_bundle_index: int = -1,
        placement_group_capture_child_tasks: Optional[bool] = None,
    ):
        self.placement_group = placement_group
        self.placement_group_bundle_index = placement_group_bundle_index
        self.placement_group_capture_child_tasks = placement_group_capture_child_tasks


@PublicAPI(stability="beta")
class NodeAffinitySchedulingStrategy:
    """Static scheduling strategy used to run a task or actor on a particular node.

    Attributes:
        node_id: the hex id of the node where the task or actor should run.
        soft: If set to true, the task or actor will be scheduled on the node, unless
            the target does not exist (e.g. the node dies), is infeasible,
            or has insufficient resources at the time of scheduling. If it does
            not schedule on the node it will find another node using the default
            scheduling policy.
            If set to false, the task or actor will be scheduled on the node, unless
            the target doesn't exist (e.g. the node dies) or is infeasible.
            If it does not schedule on the node the task will fail with
            TaskUnschedulableError or ActorUnschedulableError.
    """

    def __init__(self, node_id: str, soft: bool):
        # This will be removed once we standardize on node id being hex string.
        if not isinstance(node_id, str):
            node_id = node_id.hex()

        self.node_id = node_id
        self.soft = soft


SchedulingStrategyT = Union[
    None,
    str,  # Literal["DEFAULT", "SPREAD"]
    PlacementGroupSchedulingStrategy,
    NodeAffinitySchedulingStrategy,
]
