import logging
import time
from functools import cached_property
from typing import TYPE_CHECKING

import ray
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    ResourceRequestPriority,
    get_or_create_autoscaling_coordinator,
)
from ray.train._internal.autoscaling_coordinator_client import (
    build_train_resource_request,
)

if TYPE_CHECKING:
    from ray.actor import ActorProxy
    from ray.air.config import ScalingConfig
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

    def __init__(self, requester_id: str):
        self._requester_id = requester_id
        self._latest_autoscaling_request_time = float("-inf")

    @cached_property
    def _autoscaling_coordinator(self) -> "ActorProxy[_AutoscalingCoordinatorActor]":
        return get_or_create_autoscaling_coordinator()

    def send_resource_request(
        self,
        scaling_config: "ScalingConfig",
        num_workers: int,
    ) -> None:
        """Register training resources with the AutoscalingCoordinator."""
        resources, label_selectors = build_train_resource_request(
            scaling_config, num_workers
        )
        try:
            ray.get(
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=self._requester_id,
                    resources=resources,
                    label_selectors=label_selectors,
                    expire_after_s=AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
                    priority=ResourceRequestPriority.HIGH,
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
        scaling_config: "ScalingConfig",
        num_workers: int,
    ) -> None:
        now = time.monotonic()
        if (
            now - self._latest_autoscaling_request_time
            < AUTOSCALING_REQUESTS_INTERVAL_S
        ):
            return
        self.send_resource_request(scaling_config, num_workers)

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
