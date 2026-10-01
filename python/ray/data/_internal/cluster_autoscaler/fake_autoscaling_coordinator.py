import time
from dataclasses import dataclass
from typing import Callable, FrozenSet, List, Optional

from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    LabelSelector,
    NodeResources,
    ResourceDict,
    ResourceRequestPriority,
    ResourceRequestStrategy,
    ResourceType,
)


class FakeAutoscalingCoordinator(AutoscalingCoordinator):
    """A lightweight implementation for testing.

    This implementation always allocates the requested resources to the
    requester. It doesn't support the `priority` parameter.
    """

    @dataclass
    class Allocation:
        resources: List[ResourceDict]
        expiration_time_s: float
        request_remaining: FrozenSet[ResourceType]
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK

    def __init__(
        self,
        get_time: Callable[[], float] = time.time,
        initial_cluster_resources: Optional[List[ResourceDict]] = None,
    ):
        """Initialize the coordinator.

        Args:
            get_time: A function that returns the current time in seconds. This is a
                seam for testing.
            initial_cluster_resources: If the requester sends an empty request and
                ``request_remaining`` is non-empty, the coordinator reserves these
                resources (filtered to the requested remaining types) to the
                requester. Otherwise, the coordinator reserves the requested
                resources.
        """
        if initial_cluster_resources is None:
            initial_cluster_resources = []

        self._get_time = get_time
        self._initial_cluster_resources = initial_cluster_resources
        self._allocation: Optional[FakeAutoscalingCoordinator.Allocation] = None

    def request_resources(
        self,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: Optional[List[ResourceType]] = None,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
        label_selectors: Optional[List[LabelSelector]] = None,
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK,
    ) -> None:
        if priority != ResourceRequestPriority.MEDIUM:
            raise NotImplementedError(
                "This fake implementation doesn't support the `priority` parameter."
            )

        remaining_types = frozenset(request_remaining or ())
        if not resources and remaining_types:
            resources = []
            for r in self._initial_cluster_resources:
                filtered = {k: v for k, v in r.items() if k in remaining_types and v}
                if filtered:
                    resources.append(filtered)

        # Always accept the request and record it.
        self._allocation = self.Allocation(
            resources=resources,
            expiration_time_s=self._get_time() + expire_after_s,
            request_remaining=remaining_types,
            strategy=strategy,
        )

    def cancel_request(self) -> None:
        self._allocation = None

    def get_reserved_resources(self) -> NodeResources:
        """Return the reserved resources if they haven't expired."""
        if self._allocation is None:
            return {}

        if self._allocation.expiration_time_s < self._get_time():
            self._allocation = None
            return {}

        return {f"node_{i}": r.copy() for i, r in enumerate(self._allocation.resources)}
