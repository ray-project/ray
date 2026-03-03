import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

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
        remaining: Optional[List[ResourceDict]] = None,
    ):
        if remaining is None:
            remaining = []
        self._get_time = get_time
        self._remaining = remaining
        self._allocations: Dict[str, FakeAutoscalingCoordinator.Allocation] = {}

    def request_resources(
        self,
        requester_id: str,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
    ) -> None:
        if priority != ResourceRequestPriority.MEDIUM:
            raise NotImplementedError(
                "This fake implementation doesn't support the `priority` parameter."
            )

        # Always accept the request and record it.
        self._allocations[requester_id] = self.Allocation(
            resources=resources,
            expiration_time_s=self._get_time() + expire_after_s,
            request_remaining=request_remaining,
        )

    def cancel_request(self, requester_id: str):
        if requester_id in self._allocations:
            del self._allocations[requester_id]

    def get_allocated_resources(self, requester_id: str) -> List[ResourceDict]:
        """Return the allocated resources if they haven't expired."""
        allocation = self._allocations.get(requester_id)
        # Case 1: no allocation.
        if allocation is None:
            return []
        # Case 2: request expired.
        if allocation.expiration_time_s < self._get_time():
            del self._allocations[requester_id]
            return []
        # Case 3: allocation still valid.
        allocated_resources = list(allocation.resources)
        if allocation.request_remaining:
            # Unlike DefaultAutoscalingCoordinator, this fake returns all remaining
            # resources to each requester to keep tests simple.
            allocated_resources.extend(self._remaining)
        return allocated_resources
