import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from .base_autoscaling_coordinator import (
    AutoscalingCoordinator,
    ResourceDict,
    ResourceRequestPriority,
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
        request_remaining: bool

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
                ``request_remaining`` is True, the coordinator allocates these resources
                to the requester. Otherwise, the coordinator allocates the requested
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
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
    ) -> None:
        if priority != ResourceRequestPriority.MEDIUM:
            raise NotImplementedError(
                "This fake implementation doesn't support the `priority` parameter."
            )

        if not resources and request_remaining:
            resources = [r.copy() for r in self._initial_cluster_resources]

        # Always accept the request and record it.
        self._allocation = self.Allocation(
            resources=resources,
            expiration_time_s=self._get_time() + expire_after_s,
            request_remaining=request_remaining,
        )

    def cancel_request(self) -> None:
        self._allocation = None

    def get_allocated_resources(self) -> List[ResourceDict]:
        """Return the allocated resources if they haven't expired."""
        if self._allocation is None:
            return []

        if self._allocation.expiration_time_s < self._get_time():
            self._allocation = None
            return []

        return [r.copy() for r in self._allocation.resources]
