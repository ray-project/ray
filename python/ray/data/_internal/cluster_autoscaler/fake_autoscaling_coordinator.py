import time
from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, List, Optional

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
        initial_cluster_resources_by_node: Optional[Dict[str, ResourceDict]] = None,
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
            initial_cluster_resources_by_node: Per-node allocation returned by
                ``get_reserved_resources`` while an unexpired
                allocation exists.
        """
        if initial_cluster_resources is None:
            initial_cluster_resources = []
        if initial_cluster_resources_by_node is None:
            initial_cluster_resources_by_node = {}

        self._get_time = get_time
        self._initial_cluster_resources = initial_cluster_resources
        self._initial_cluster_resources_by_node = initial_cluster_resources_by_node
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

        # An explicit per-node seed wins (its docstring promises exactly this);
        # otherwise synthesize one node per reserved bundle.
        if self._initial_cluster_resources_by_node:
            return {
                n: r.copy() for n, r in self._initial_cluster_resources_by_node.items()
            }
        return {f"node_{i}": r.copy() for i, r in enumerate(self._allocation.resources)}
