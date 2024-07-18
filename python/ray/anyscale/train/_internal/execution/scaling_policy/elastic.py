import logging
from typing import Dict, List

from .autoscaling_requester import TrainAutoscalingRequester
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


class ElasticScalingPolicy(ScalingPolicy):
    autoscaling_requester_cls = TrainAutoscalingRequester

    def __init__(self, scaling_config: ScalingConfig):
        super().__init__(scaling_config)

        self.autoscaling_requester = self.autoscaling_requester_cls()

        self._latest_monitor_time = float("-inf")

    def _count_possible_workers(self, node_resources: List[Dict[str, float]]) -> int:
        # TODO: Fractional resources do not work well here.
        single_worker_resources = self.scaling_config._resources_per_worker_not_none
        total_num_workers = 0

        # If workers require no resources, we can run as many as we want.
        if sum(single_worker_resources.values()) == 0:
            return self.scaling_config.max_workers

        for resources in node_resources:
            num_workers = min(
                [
                    resources.get(resource, 0.0) // single_worker_resources[resource]
                    for resource in single_worker_resources
                    if single_worker_resources[resource] > 0
                ]
            )
            total_num_workers += num_workers

        return int(total_num_workers)

    def _get_resize_decision(self) -> ResizeDecision:
        node_resources = self.autoscaling_requester.node_resources()
        available_workers = self._count_possible_workers(node_resources)
        num_workers = min(available_workers, self.scaling_config.max_workers)
        return ResizeDecision(
            num_workers=num_workers,
            resources_per_worker=self.scaling_config._resources_per_worker_not_none,
        )

    def make_decision_for_non_running_worker_group(
        self, worker_group_status: WorkerGroupStatus
    ) -> ScalingDecision:
        decision = self._get_resize_decision()

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
        # Ensure that we don't make resizing decisions too frequently.
        # The latest restart time and the latest monitor time (whichever is later)
        # determine the time of the next resize consideration.
        latest_consideration_time = max(
            worker_group_status.latest_start_time, self._latest_monitor_time
        )

        time_since_latest_consideration = time_monotonic() - latest_consideration_time
        if (
            time_since_latest_consideration
            < self.scaling_config.elastic_resize_monitor_interval_s
        ):
            logger.debug(
                "Skipping resize decision due to the latest resizing consideration "
                "happening too recently: "
                f"{time_since_latest_consideration=} < "
                "ScalingConfig(elastic_resize_monitor_interval_s="
                f"{self.scaling_config.elastic_resize_monitor_interval_s})"
            )
            return NoopDecision()

        self._latest_monitor_time = time_monotonic()
        decision = self._get_resize_decision()
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

        self.autoscaling_requester.request(bundles=[resources_per_worker] * max_workers)

    def before_controller_shutdown(self):
        """Clear the autoscaling request when the control loop shuts down.
        If this is not cleared, the autoscaler will keep trying to upscale the cluster,
        and idle nodes will not be removed."""
        self.autoscaling_requester.clear_request()
