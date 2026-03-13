import logging
import uuid
from functools import cached_property

import ray
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupState,
)
from ray.train.v2._internal.util import time_monotonic

logger = logging.getLogger(__name__)

# The time in seconds after which an autoscaling request will expire.
AUTOSCALING_REQUESTS_EXPIRE_TIME_S = 180
# Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
AUTOSCALING_REQUESTS_GET_TIMEOUT_S = 5
# Interval in seconds between resource requests to the AutoscalingCoordinator.
AUTOSCALING_REQUESTS_INTERVAL_S = 20


class FixedScalingPolicy(ScalingPolicy):
    def __init__(self, scaling_config):
        super().__init__(scaling_config)
        self._requester_id = "train-" + uuid.uuid4().hex
        self._latest_autoscaling_request_time = 0.0

    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        return ResizeDecision(
            num_workers=self.scaling_config.num_workers,
            resources_per_worker=self.scaling_config._resources_per_worker_not_none,
        )

    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        self._maybe_send_resource_request()
        return NoopDecision()

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
        num_workers = self.scaling_config.num_workers
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
        num_workers = self.scaling_config.num_workers
        logger.info(
            "Requesting resources for fixed worker group: "
            f"{resources_per_worker} * {num_workers}"
        )
        self._send_resource_request()

    def before_controller_shutdown(self):
        """Cancel the resource request when the controller shuts down."""
        self._cancel_resource_request()

    def before_controller_abort(self):
        """Cancel the resource request when the controller is aborted."""
        self._cancel_resource_request()
