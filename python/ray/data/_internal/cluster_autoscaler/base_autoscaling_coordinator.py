import abc
from enum import Enum
from typing import Dict, List, Optional

from ray.data._internal.execution.interfaces.common import NodeIdStr

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

        The requested resources should represent the full set of resources needed,
        not just the incremental amount.

        Args:
            resources: The requested resources. This should match the format accepted
                by `ray.autoscaler.sdk.request_resources`.
            expire_after_s: Time in seconds after which this request will expire.
                The requester is responsible for periodically sending new requests
                to avoid the request being purged.
            request_remaining: Resource types for which leftover cluster capacity
                should also be reserved after satisfying ``resources``. ``None``
                or an empty list means no leftovers.
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
        """Get the reserved resources for the requester.

        Returns:
            A list of dictionaries representing the reserved resources bundles.
        """
        ...
