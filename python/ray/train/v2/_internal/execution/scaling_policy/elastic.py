import logging
import uuid
from functools import cached_property
from typing import TYPE_CHECKING, Dict, List, Optional

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
from ray.train.v2.api.config import ScalingConfig

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
        ResourceDict,
    )


class ElasticScalingPolicy(ScalingPolicy):

    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUESTS_EXPIRE_TIME_S = 180
    # Minimum interval in seconds between two consecutive autoscaling requests.
    AUTOSCALING_REQUESTS_INTERVAL_S = 20
    # Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
    AUTOSCALING_REQUESTS_GET_TIMEOUT_S = 5
    # Minimum interval in seconds between querying the AutoscalingCoordinator for allocated resources.
    GET_ALLOCATED_RESOURCES_INTERVAL_S = 1
    # Minimum interval in seconds between logging warnings about insufficient workers.
    INSUFFICIENT_WORKERS_WARNING_INTERVAL_S = 30

    def __init__(self, scaling_config: ScalingConfig):
        super().__init__(scaling_config)

        self._latest_monitor_time = float("-inf")
        # Requester ID for AutoscalingCoordinator.
        # Prefer the Train run_id when available (set in after_controller_start).
        self._requester_id = "train-" + uuid.uuid4().hex
        self._latest_autoscaling_request_time = float("-inf")
        self._latest_insufficient_workers_warning_time = float("-inf")
        self._latest_allocated_resources_query_time = float("-inf")
        self._latest_allocated_resources: Optional[List["ResourceDict"]] = None

    def _count_possible_workers(
        self, allocated_resources: List[Dict[str, float]]
    ) -> int:
        """Count the number of workers that can be started/restarted with the given
        the list of node resources. The returned number is capped at the maximum
        number of workers.
        """
        # TODO: Fractional resources do not work well here.
        single_worker_resources = self.scaling_config._resources_per_worker_not_none
        total_num_workers = 0

        # If workers require no resources, we can run as many as we want.
        if sum(single_worker_resources.values()) == 0:
            return self.scaling_config.max_workers

        for resources in allocated_resources:
            num_workers = min(
                [
                    resources.get(resource, 0.0) // single_worker_resources[resource]
                    for resource in single_worker_resources
                    if single_worker_resources[resource] > 0
                ]
            )
            total_num_workers += num_workers

        return min(int(total_num_workers), self.scaling_config.max_workers)

    def _get_resize_decision(self, num_workers: int) -> ResizeDecision:
        return ResizeDecision(
            num_workers=num_workers,
            resources_per_worker=self.scaling_config._resources_per_worker_not_none,
        )

    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        self._maybe_send_resource_request()

        allocated_resources = self._get_allocated_resources()
        if allocated_resources is None:
            return NoopDecision()

        num_workers = self._count_possible_workers(allocated_resources)

        if num_workers < self.scaling_config.min_workers:
            now = time_monotonic()
            # Only log this warning periodically to avoid spamming logs
            if (
                now - self._latest_insufficient_workers_warning_time
                >= self.INSUFFICIENT_WORKERS_WARNING_INTERVAL_S
            ):
                logger.info(
                    f"Detected ready resources for {num_workers} workers "
                    "in the cluster. "
                    "Deciding NOT to start/restart training due to the number of workers "
                    "falling below the minimum "
                    f"(min_workers={self.scaling_config.min_workers})."
                )
                self._latest_insufficient_workers_warning_time = now
            return NoopDecision()

        logger.info(
            f"Detected ready resources for {num_workers} workers "
            "in the cluster. "
            "Deciding to start/restart training with this worker group size."
        )
        return self._get_resize_decision(num_workers)

    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        self._maybe_send_resource_request()

        # Ensure that we don't make resizing decisions too frequently.
        # The latest restart time and the latest monitor time (whichever is later)
        # determine the time of the next resize consideration.
        latest_consideration_time = max(
            worker_group_state.start_time, self._latest_monitor_time
        )

        now = time_monotonic()
        time_since_latest_consideration = now - latest_consideration_time
        if (
            time_since_latest_consideration
            < self.scaling_config.elastic_resize_monitor_interval_s
        ):
            logger.debug(
                "Skipping resize decision due to the latest resizing consideration "
                "happening too recently: "
                "%.2f seconds < ScalingConfig(elastic_resize_monitor_interval_s=%.2f seconds).",
                time_since_latest_consideration,
                self.scaling_config.elastic_resize_monitor_interval_s,
            )
            return NoopDecision()

        self._latest_monitor_time = now

        allocated_resources = self._get_allocated_resources()
        if allocated_resources is None:
            return NoopDecision()

        num_workers = self._count_possible_workers(allocated_resources)

        if num_workers == worker_group_state.num_workers:
            logger.info(
                "Did not detect any changes in the cluster resources. "
                "Training will continue with the same worker group size "
                f"({num_workers})."
            )
            return NoopDecision()
        elif num_workers < self.scaling_config.min_workers:
            # This covers an edge case where allocated resources decrease to less
            # than the minimum number of workers.
            # This situation is rare, since cluster downsizing typically involves
            # worker failures. However, this check is still useful to fully
            # avoid entering an invalid state with fewer workers than the minimum.
            return NoopDecision()

        logger.info(
            "Detected changes in the cluster resources. "
            "Deciding to resize the worker group from "
            f"{worker_group_state.num_workers} -> {num_workers} workers."
        )
        return self._get_resize_decision(num_workers)

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
            < self.AUTOSCALING_REQUESTS_INTERVAL_S
        ):
            return

        resources_per_worker = self.scaling_config._resources_per_worker_not_none
        max_workers = self.scaling_config.max_workers
        try:
            from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
                ResourceRequestPriority,
            )

            ray.get(
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=self._requester_id,
                    resources=[resources_per_worker] * max_workers,
                    expire_after_s=self.AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
                    priority=ResourceRequestPriority.HIGH,
                ),
                timeout=self.AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
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

    def _get_allocated_resources(self) -> Optional[List["ResourceDict"]]:
        """Get allocated resources from AutoscalingCoordinator.
        Return None if there is an error."""
        now = time_monotonic()
        time_since_last_call = now - self._latest_allocated_resources_query_time

        if time_since_last_call < self.GET_ALLOCATED_RESOURCES_INTERVAL_S:
            return self._latest_allocated_resources

        allocated_resources = None
        try:
            allocated_resources = ray.get(
                self._autoscaling_coordinator.get_allocated_resources.remote(
                    self._requester_id
                ),
                timeout=self.AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            )
        except Exception:
            msg = (
                f"Failed to get allocated resources for {self._requester_id}."
                " Will not resize the worker group."
                " If this only happens transiently during network partition or"
                " CPU being overloaded, it's safe to ignore this error."
                " If this error persists, file a GitHub issue."
            )
            logger.warning(msg, exc_info=True)
        finally:
            self._latest_allocated_resources_query_time = time_monotonic()
            self._latest_allocated_resources = allocated_resources

        return self._latest_allocated_resources

    def _cancel_resource_request(self):
        """Cancel the resource request to AutoscalingCoordinator."""
        try:
            ray.get(
                self._autoscaling_coordinator.cancel_request.remote(
                    requester_id=self._requester_id,
                ),
                timeout=self.AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            )
        except Exception:
            msg = (
                f"Failed to cancel resource request for {self._requester_id}."
                " The request will still expire after the timeout of"
                f" {self.AUTOSCALING_REQUESTS_EXPIRE_TIME_S} seconds."
            )
            logger.warning(msg, exc_info=True)

    # --------------------------
    # ControllerCallback
    # --------------------------

    def after_controller_start(self, train_run_context: TrainRunContext):
        """Send cluster autoscaling requests when the control loop starts."""
        self._requester_id = f"train-{train_run_context.run_id}"
        resources_per_worker = self.scaling_config._resources_per_worker_not_none
        max_workers = self.scaling_config.max_workers
        logger.info(
            "Requesting resources to fit the maximum number of workers: "
            f"{resources_per_worker} * {max_workers}"
        )
        self._maybe_send_resource_request()

    def before_controller_shutdown(self):
        """Clear the autoscaling request eagerly when the control loop shuts down.
        So that cluster can scale down more quickly before the request timeout.
        """
        self._cancel_resource_request()

    def before_controller_abort(self):
        """Cancel the autoscaling request when the controller is aborted."""
        self._cancel_resource_request()
