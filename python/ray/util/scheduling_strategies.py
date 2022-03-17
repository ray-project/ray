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


SchedulingStrategyT = Union[
    None, str, PlacementGroupSchedulingStrategy  # Literal["DEFAULT", "SPREAD"]
]
