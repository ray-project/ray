import abc
from enum import Enum
from typing import Dict, List, Optional

ResourceDict = Dict[str, float]


class ResourceRequestPriority(Enum):
    """Priority of a resource request."""

    LOW = -10
    MEDIUM = 0
    HIGH = 10


class AutoscalingCoordinator(abc.ABC):
    @abc.abstractmethod
    def request_resources(
        self,
        resources: List[ResourceDict],
        expire_after_s: float,
        request_remaining: bool = False,
        priority: ResourceRequestPriority = ResourceRequestPriority.MEDIUM,
        label_selectors: Optional[List[Dict[str, str]]] = None,
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
            request_remaining: If true, after allocating requested resources to each
                requester, remaining resources will also be allocated to this requester.
            priority: The priority of the request. Higher value means higher priority.
            label_selectors: Optional per-bundle label selectors, one per entry in
                ``resources``. Forwarded to the autoscaler as
                ``bundle_label_selectors``.
        """
        ...

    @abc.abstractmethod
    def cancel_request(self) -> None:
        """Cancel the resource request from the requester."""
        ...

    @abc.abstractmethod
    def get_allocated_resources(self) -> List[ResourceDict]:
        """Get the allocated resources for the requester.

        Returns:
            A list of dictionaries representing the allocated resources bundles.
        """
        ...
