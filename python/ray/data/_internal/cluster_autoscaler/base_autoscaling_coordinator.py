import abc
from enum import Enum
from typing import Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces.common import (
    NodeIdStr,
    PlacementGroupIdStr,
)

ResourceType = str
ResourceDict = Dict[ResourceType, float]
RequesterId = str
LabelKey = str
LabelValue = str
LabelSelector = Dict[LabelKey, LabelValue]
ReservedResources = Dict[NodeIdStr, ResourceDict]
NodeResources = Dict[NodeIdStr, ResourceDict]

# Standard resource types Ray Data requests leftovers for when
# ``request_remaining`` is non-empty.
STANDARD_RESOURCE_TYPES: List[ResourceType] = [
    "CPU",
    "GPU",
    "memory",
    "object_store_memory",
]


class ResourceRequestPriority(Enum):
    """Priority of a resource request."""

    LOW = -10
    MEDIUM = 0
    HIGH = 10


class ResourceRequestStrategy(str, Enum):
    """How a requester's bundles are spread over nodes when reserving.
    TODO(jhsu): See if we can punt the autoscaling coordinator into the
    core execution layer."""

    PACK = "PACK"
    SPREAD = "SPREAD"
    STRICT_PACK = "STRICT_PACK"
    STRICT_SPREAD = "STRICT_SPREAD"

    def is_spread_like(self) -> bool:
        return self in (self.SPREAD, self.STRICT_SPREAD)


class AutoscalingCoordinator(abc.ABC):
    @abc.abstractmethod
    def request_resources(
        self,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: Optional[List[ResourceType]] = None,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
        label_selectors: Optional[List[LabelSelector]] = None,
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK,
    ) -> None:
        """Request cluster resources.

        Replaces this requester's previous request: ``resources`` is the full
        set needed, not an increment. The bundles serve two purposes: they are
        forwarded to the Ray autoscaler as demand, and they are packed onto
        nodes to build the reservation (see ``get_reserved_resources``).

        Args:
            resources: The requested bundles, in the format accepted by
                `ray.autoscaler.sdk.request_resources`. Includes the bundles the
                requester holds through its own placement group. A bundle that
                fits on no node is left out of the reservation, not queued.
            expire_after_s: Time in seconds after which this request will expire.
                The requester is responsible for periodically sending new requests
                to avoid the request being purged.
            request_remaining: Resource types for which this requester also gets
                an equal share, with every other requester asking for the same
                type, of each node's capacity left once all bundles are
                reserved. ``None`` or an empty list means no leftovers.
            priority: The priority of the request. Higher value means higher priority.
            label_selectors: Optional per-bundle label selectors, one per entry in
                ``resources``. Forwarded to the autoscaler as
                ``bundle_label_selectors``.
            strategy: How to distribute this requester's bundles over nodes when
                reserving. Cannot be changed on an ongoing request.
        """
        ...

    @abc.abstractmethod
    def cancel_request(self) -> None:
        """Cancel the resource request from the requester."""
        ...

    @abc.abstractmethod
    def get_reserved_resources(self) -> NodeResources:
        """Everything assigned to this requester on each node.

        Per node, the requester's bundles placed there plus its share of what
        is left (``request_remaining``), summed into one dict. That includes
        bundles it holds through a placement group: PG and non-PG are not
        split. It is an accounting split of node totals, not what is free.

        Returns:
            Resources keyed by node id. Empty if there is no ongoing request.
        """
        ...

    @abc.abstractmethod
    def register_placement_groups(
        self, placement_group_ids: Iterable[PlacementGroupIdStr]
    ) -> None:
        """Tell the coordinator which placement groups this requester holds, so
        their bundles are reserved where Ray placed them and kept out of
        everyone else's capacity. Replaces the previous set; empty unregisters
        them."""
        ...
