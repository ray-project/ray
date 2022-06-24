from typing import Union, Optional
from ray.util.annotations import PublicAPI
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
        placement_group: PlacementGroup,
        placement_group_bundle_index: int = -1,
        placement_group_capture_child_tasks: Optional[bool] = None,
    ):
        if placement_group is None:
            raise ValueError(
                "placement_group needs to be an instance of PlacementGroup"
            )

        self.placement_group = placement_group
        self.placement_group_bundle_index = placement_group_bundle_index
        self.placement_group_capture_child_tasks = placement_group_capture_child_tasks


@PublicAPI(stability="beta")
class NodeAffinitySchedulingStrategy:
    """Static scheduling strategy used to run a task or actor on a particular node.

    Attributes:
        node_id: the hex id of the node where the task or actor should run.
        soft: whether the scheduler should run the task or actor somewhere else
            if the target node doesn't exist (e.g. the node dies) or is infeasible
            during scheduling.
            If the node exists and is feasible, the task or actor
            will only be scheduled there.
            This means if the node doesn't have the available resources,
            the task or actor will wait indefinitely until resources become available.
            If the node doesn't exist or is infeasible, the task or actor
            will fail if soft is False
            or be scheduled somewhere else if soft is True.
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
