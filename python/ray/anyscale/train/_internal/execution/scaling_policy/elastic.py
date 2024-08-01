import logging
import uuid
from functools import cached_property
from typing import Dict, List, Optional

import ray
from ray.anyscale.air._internal.autoscaling_coordinator import (
    ResourceDict,
    ResourceRequestPriority,
    get_or_create_autoscaling_coordinator,
)
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupStatus
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.config import ScalingConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ElasticScalingPolicy(ScalingPolicy):

    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUESTS_EXPIRE_TIME_S = 180
    # Minimum interval in seconds between two consecutive autoscaling requests.
    AUTOSCALING_REQUESTS_INTERVAL_S = 20
    # Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
    AUTOSCALING_REQUESTS_GET_TIMEOUT_S = 5

    def __init__(self, scaling_config: ScalingConfig):
        super().__init__(scaling_config)

        self._latest_monitor_time = float("-inf")
        # Requester ID for AutoscalingCoordinator.
        # TODO: define the UUID in TrainController.
        self._requester_id = "train-" + uuid.uuid4().hex
        self._latest_autoscaling_request_time = float("-inf")

    def _count_possible_workers(
        self, allocated_resources: List[Dict[str, float]]
    ) -> int:
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

        return int(total_num_workers)

    def _get_resize_decision(
        self, allocated_resources: List[ResourceDict]
    ) -> ResizeDecision:
        available_workers = self._count_possible_workers(allocated_resources)
        num_workers = min(available_workers, self.scaling_config.max_workers)
        return ResizeDecision(
            num_workers=num_workers,
            resources_per_worker=self.scaling_config._resources_per_worker_not_none,
        )

    def make_decision_for_non_running_worker_group(
        self, worker_group_status: WorkerGroupStatus
    ) -> ScalingDecision:
        self._maybe_send_resource_request()

        allocated_resources = self._get_allocated_resources()
        if allocated_resources is None:
            return NoopDecision()
        decision = self._get_resize_decision(allocated_resources)

        if decision.num_workers < self.scaling_config.min_workers:
            logger.info(
                f"Detected ready resources for {decision.num_workers} workers "
                "in the cluster. "
                "Deciding NOT to start/restart training due to the number of workers "
                "falling below the minimum "
                f"(min_workers={self.scaling_config.min_workers})."
            )
            return NoopDecision()

        logger.info(
            f"Detected ready resources for {decision.num_workers} workers "
            "in the cluster. "
            "Deciding to start/restart training with this worker group size."
        )
        return decision

    def make_decision_for_running_worker_group(
        self, worker_group_status: WorkerGroupStatus
    ) -> ScalingDecision:
        self._maybe_send_resource_request()

        # Ensure that we don't make resizing decisions too frequently.
        # The latest restart time and the latest monitor time (whichever is later)
        # determine the time of the next resize consideration.
        latest_consideration_time = max(
            worker_group_status.latest_start_time, self._latest_monitor_time
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
                "%.2f < ScalingConfig(elastic_resize_monitor_interval_s=%.2f.",
                time_since_latest_consideration,
                self.scaling_config.elastic_resize_monitor_interval_s,
            )
            return NoopDecision()

        self._latest_monitor_time = now
        allocated_resources = self._get_allocated_resources()
        if allocated_resources is None:
            return NoopDecision()
        decision = self._get_resize_decision(allocated_resources)
        if decision.num_workers == worker_group_status.num_workers:
            logger.info(
                "Did not detect any changes in the cluster resources. "
                "Training will continue with the same worker group size "
                f"({decision.num_workers})."
            )
            return NoopDecision()

        logger.info(
            "Detected changes in the cluster resources. "
            "Deciding to resize the worker group from "
            f"{worker_group_status.num_workers} -> {decision.num_workers} workers."
        )
        return decision

    # ---------------------------------------------------
    # Methods for interacting with AutoscalingCoordinator
    # ---------------------------------------------------

    @cached_property
    def _autoscaling_coordinator(self):
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
            ray.get(
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=self._requester_id,
                    resources=[resources_per_worker] * max_workers,
                    expire_after_s=self.AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
                    priority=ResourceRequestPriority.HIGH,
                )
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

    def _get_allocated_resources(self) -> Optional[List[ResourceDict]]:
        """Get allocated resources from AutoscalingCoordinator.
        Return None if there is an error."""
        try:
            return ray.get(
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
            return None

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

    def after_controller_start(self):
        """Send cluster autoscaling requests when the control loop starts."""
        resources_per_worker = self.scaling_config._resources_per_worker_not_none
        max_workers = self.scaling_config.max_workers
        logger.info(
            "Attempting to request resources to fit the maximum number of workers: "
            f"{resources_per_worker} * {max_workers}\n"
            "Ensure that your cluster's available node types are configured "
            "so that this autoscaling request is feasible. "
            "For example, if you request {'GPU': 1} * 4, but your cluster "
            "only allows a maximum of 2 single GPU nodes for upscaling, "
            "no nodes will spin up."
        )
        self._maybe_send_resource_request()

    def before_controller_shutdown(self):
        """Clear the autoscaling request eagerly when the control loop shuts down.
        So that cluster can scale down more quickly before the request timeout.
        """
        self._cancel_resource_request()
