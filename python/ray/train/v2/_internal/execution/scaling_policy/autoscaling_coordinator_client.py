import logging
import time
from functools import cached_property
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    LabelSelector,
    RequesterId,
    ReservedResources,
    ResourceDict,
)
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    ResourceRequestPriority,
    ResourceRequestStrategy,
    get_or_create_autoscaling_coordinator,
)

if TYPE_CHECKING:
    from ray.actor import ActorProxy
    from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
        _AutoscalingCoordinatorActor,
    )

logger = logging.getLogger(__name__)

# The time in seconds after which an autoscaling request will expire.
AUTOSCALING_REQUESTS_EXPIRE_TIME_S = 180
# Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
AUTOSCALING_REQUESTS_GET_TIMEOUT_S = 5
# Interval in seconds between resource requests to the AutoscalingCoordinator.
AUTOSCALING_REQUESTS_INTERVAL_S = 20


class TrainAutoscalingCoordinatorClient:
    """Thin client for registering train worker resources with the coordinator.

    This is the train version, the data version is called `DefaultAutoscalingCoordinator`"""

    # Minimum interval in seconds between querying the AutoscalingCoordinator for reserved resources.
    GET_RESERVED_RESOURCES_INTERVAL_S = 1

    def __init__(self, requester_id: RequesterId):
        self._requester_id = requester_id
        self._latest_autoscaling_request_time = float("-inf")
        self._latest_reserved_resources_query_time = float("-inf")
        self._latest_reserved_resources: ReservedResources = {}

    @cached_property
    def _autoscaling_coordinator(self) -> "ActorProxy[_AutoscalingCoordinatorActor]":
        return get_or_create_autoscaling_coordinator()

    def send_resource_request(
        self,
        resources: List[ResourceDict],
        label_selectors: Optional[List[LabelSelector]],
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK,
    ) -> None:
        """Register training resources with the AutoscalingCoordinator."""
        try:
            ray.get(
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=self._requester_id,
                    resources=resources,
                    label_selectors=label_selectors,
                    expire_after_s=AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
                    priority=ResourceRequestPriority.HIGH,
                    strategy=strategy,
                ),
                timeout=AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            )
            self._latest_autoscaling_request_time = time.monotonic()
        except Exception:
            msg = (
                f"Failed to send resource request for {self._requester_id}."
                " If this only happens transiently during network partition or"
                " CPU being overloaded, it's safe to ignore this error."
                " If this error persists, file a GitHub issue."
            )
            logger.warning(msg, exc_info=True)

    def maybe_send_resource_request(
        self,
        resources: List[ResourceDict],
        label_selectors: Optional[List[LabelSelector]],
        strategy: ResourceRequestStrategy = ResourceRequestStrategy.PACK,
    ) -> None:
        now = time.monotonic()
        if (
            now - self._latest_autoscaling_request_time
            < AUTOSCALING_REQUESTS_INTERVAL_S
        ):
            return
        self.send_resource_request(
            resources=resources,
            label_selectors=label_selectors,
            strategy=strategy,
        )

    def get_reserved_resources(self, *, recompute: bool = False) -> ReservedResources:
        """Get reserved resources for this requester.

        Returns a cached value if queried within
        ``GET_RESERVED_RESOURCES_INTERVAL_S`` unless ``recompute`` is True.
        """
        now = time.monotonic()
        if (
            not recompute
            and now - self._latest_reserved_resources_query_time
            < self.GET_RESERVED_RESOURCES_INTERVAL_S
        ):
            return self._latest_reserved_resources

        try:
            reserved_resources = ray.get(
                self._autoscaling_coordinator.get_reserved_resources.remote(
                    self._requester_id, recompute=recompute
                ),
                timeout=AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            )
        except Exception:
            logger.debug(
                f"Failed to get reserved resources for {self._requester_id}.",
                exc_info=True,
            )
            reserved_resources = self._latest_reserved_resources

        self._latest_reserved_resources_query_time = time.monotonic()
        self._latest_reserved_resources = reserved_resources
        return reserved_resources

    def cancel_resource_request(self) -> None:
        try:
            ray.get(
                self._autoscaling_coordinator.cancel_request.remote(
                    requester_id=self._requester_id,
                ),
                timeout=AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            )
        except Exception:
            msg = (
                f"Failed to cancel resource request for {self._requester_id}."
                " The request will still expire after the timeout of"
                f" {AUTOSCALING_REQUESTS_EXPIRE_TIME_S} seconds."
            )
            logger.warning(msg, exc_info=True)
