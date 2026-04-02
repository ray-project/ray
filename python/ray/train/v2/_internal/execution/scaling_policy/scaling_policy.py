import abc
import logging
import uuid
from dataclasses import dataclass
from functools import cached_property
from typing import Dict

import ray
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupState,
)
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.config import ScalingConfig

logger = logging.getLogger(__name__)

# The time in seconds after which an autoscaling request will expire.
AUTOSCALING_REQUESTS_EXPIRE_TIME_S = 180
# Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
AUTOSCALING_REQUESTS_GET_TIMEOUT_S = 5
# Interval in seconds between resource requests to the AutoscalingCoordinator.
AUTOSCALING_REQUESTS_INTERVAL_S = 20


@dataclass
class ScalingDecision:
    pass


@dataclass
class NoopDecision(ScalingDecision):
    pass


@dataclass
class ResizeDecision(ScalingDecision):
    num_workers: int
    resources_per_worker: Dict[str, float]


class ScalingPolicy(abc.ABC, ControllerCallback):
    """A policy that determines when and how to scale a worker group.

    This can be used to implement elasticity and fault tolerance.

    Recovery decisions are made when workers are in an inactive or unhealthy state.
    Upscale decisions are optional and are made when workers are healthy.

    Note: When adding new scaling policies, revisit the shared defaults- particularly if:
    - AutoscalingCoordinator integration is not needed or a different interface
      becomes available
    - Timeout/expiry constants need to diverge between policies
    - _get_num_workers_for_resource_request() needs variable worker counts
    - Controller lifecycle behavior diverges
    """

    # TODO: Restructure these APIs to consider different TrainControllerStates
    # instead of just running and non-running worker groups.

    def __init__(self, scaling_config: ScalingConfig):
        self.scaling_config = scaling_config
        self._requester_id = "train-" + uuid.uuid4().hex
        self._latest_autoscaling_request_time = float("-inf")

    @abc.abstractmethod
    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        """Makes a scaling decision when the worker group is initializing
        or recovering from an error."""
        raise NotImplementedError

    @abc.abstractmethod
    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        """Makes a scaling decision when monitoring healthy, running workers."""
        raise NotImplementedError

    def requires_shutdown_before_non_running_decision(self) -> bool:
        """Whether the existing non-running worker group must be shut down before making
        a scaling decision."""
        return False

    @abc.abstractmethod
    def _get_num_workers_for_resource_request(self) -> int:
        """Return the number of workers to request resources for."""
        raise NotImplementedError

    # ---------------------------------------------------
    # Methods for interacting with AutoscalingCoordinator
    # ---------------------------------------------------

    @cached_property
    def _autoscaling_coordinator(self):
        from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
            get_or_create_autoscaling_coordinator,
        )

        return get_or_create_autoscaling_coordinator()

    def _maybe_send_resource_request(self):
        """Send a resource request to AutoscalingCoordinator,
        if AUTOSCALING_REQUESTS_INTERVAL_S has passed since the last send."""
        now = time_monotonic()
        if (
            now - self._latest_autoscaling_request_time
            < AUTOSCALING_REQUESTS_INTERVAL_S
        ):
            return
        self._send_resource_request()

    def _send_resource_request(self):
        """Register training resources with the AutoscalingCoordinator."""
        resources_per_worker = self.scaling_config._resources_per_worker_not_none
        num_workers = self._get_num_workers_for_resource_request()
        try:
            from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
                ResourceRequestPriority,
            )

            ray.get(
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=self._requester_id,
                    resources=[resources_per_worker] * num_workers,
                    expire_after_s=AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
                    priority=ResourceRequestPriority.HIGH,
                ),
                timeout=AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            )
            self._latest_autoscaling_request_time = time_monotonic()
        except Exception:
            msg = (
                f"Failed to send resource request for {self._requester_id}."
                " If this only happens transiently during network partition or"
                " CPU being overloaded, it's safe to ignore this error."
                " If this error persists, file a GitHub issue."
            )
            logger.warning(msg, exc_info=True)

    def _cancel_resource_request(self):
        """Cancel the resource request to AutoscalingCoordinator."""
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

    # --------------------------
    # ControllerCallback
    # --------------------------

    def after_controller_start(self, train_run_context: TrainRunContext):
        """Register training resources with the AutoscalingCoordinator."""
        self._requester_id = f"train-{train_run_context.run_id}"
        resources_per_worker = self.scaling_config._resources_per_worker_not_none
        num_workers = self._get_num_workers_for_resource_request()
        logger.info(f"Requesting resources: {resources_per_worker} * {num_workers}")
        self._send_resource_request()

    async def before_controller_shutdown(self):
        """Cancel the resource request when the controller shuts down."""
        self._cancel_resource_request()

    def before_controller_abort(self):
        """Cancel the resource request when the controller is aborted."""
        self._cancel_resource_request()
