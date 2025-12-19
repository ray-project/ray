import logging
from collections import defaultdict
from typing import Dict, List, Optional

from ray._private.event.event_logger import EventLoggerAdapter
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    ResourceRequest,
)
from ray.core.generated.common_pb2 import LabelSelectorOperator
from ray.core.generated.instance_manager_pb2 import LaunchRequest, TerminationRequest

logger = logging.getLogger(__name__)


class AutoscalerEventLogger:
    """
    Logs events related to the autoscaler.

    # TODO:
    - Add more logging for other events.
    - Rate limit the events if too spammy.
    """

    def __init__(self, logger: EventLoggerAdapter):
        self._logger = logger

    def log_cluster_scheduling_update(
        self,
        cluster_resources: Dict[str, float],
        launch_requests: Optional[List[LaunchRequest]] = None,
        terminate_requests: Optional[List[TerminationRequest]] = None,
        infeasible_requests: Optional[List[ResourceRequest]] = None,
        infeasible_gang_requests: Optional[List[GangResourceRequest]] = None,
        infeasible_cluster_resource_constraints: Optional[
            List[ClusterResourceConstraint]
        ] = None,
    ) -> None:
        """
        Log updates to the autoscaler scheduling state.

        Emits:
        - info logs for node launches and terminations (counts grouped by node type).
        - an info log summarizing the cluster size after a resize (CPUs/GPUs/TPUs).
        - warnings describing infeasible single resource requests, infeasible gang
          (placement group) requests, and infeasible cluster resource constraints.

        Args:
            cluster_resources: Mapping of resource name to total resources for the
                current cluster state.
            launch_requests: Node launch requests issued in this scheduling step.
            terminate_requests: Node termination requests issued in this scheduling
                step.
            infeasible_requests: Resource requests that could not be satisfied by
                any available node type.
            infeasible_gang_requests: Gang/placement group requests that could not
                be scheduled.
            infeasible_cluster_resource_constraints: Cluster-level resource
                constraints that could not be satisfied.

        Returns:
            None
        """

        # Log any launch events.
        if launch_requests:
            launch_type_count = defaultdict(int)
            for req in launch_requests:
                launch_type_count[req.instance_type] += req.count

            for idx, (instance_type, count) in enumerate(launch_type_count.items()):
                log_str = f"Adding {count} node(s) of type {instance_type}."
                self._logger.info(f"{log_str}")
                logger.info(f"{log_str}")

        # Log any terminate events.
        if terminate_requests:
            termination_by_causes_and_type = defaultdict(int)
            for req in terminate_requests:
                termination_by_causes_and_type[(req.cause, req.instance_type)] += 1

            cause_reason_map = {
                TerminationRequest.Cause.OUTDATED: "outdated",
                TerminationRequest.Cause.MAX_NUM_NODES: "max number of worker nodes reached",  # noqa
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE: "max number of worker nodes per type reached",  # noqa
                TerminationRequest.Cause.IDLE: "idle",
            }

            for idx, ((cause, instance_type), count) in enumerate(
                termination_by_causes_and_type.items()
            ):
                log_str = f"Removing {count} nodes of type {instance_type} ({cause_reason_map[cause]})."  # noqa
                self._logger.info(f"{log_str}")
                logger.info(f"{log_str}")

        # Cluster shape changes.
        if launch_requests or terminate_requests:
            num_cpus = cluster_resources.get("CPU", 0)
            log_str = f"Resized to {int(num_cpus)} CPUs"

            if "GPU" in cluster_resources:
                log_str += f", {int(cluster_resources['GPU'])} GPUs"
            if "TPU" in cluster_resources:
                log_str += f", {int(cluster_resources['TPU'])} TPUs"

            self._logger.info(f"{log_str}.")
            self._logger.debug(f"Current cluster resources: {dict(cluster_resources)}.")

        # Log any infeasible requests.
        if infeasible_requests:
            requests_by_count = ResourceRequestUtil.group_by_count(infeasible_requests)
            log_str = "No available node types can fulfill resource requests "
            for idx, req_count in enumerate(requests_by_count):
                resource_map = ResourceRequestUtil.to_resource_map(req_count.request)
                log_str += f"{resource_map}*{req_count.count}"
                if idx < len(requests_by_count) - 1:
                    log_str += ", "

                # Parse and log label selectors if present
                if req_count.request.label_selectors:
                    selector_strs = []
                    for selector in req_count.request.label_selectors:
                        for constraint in selector.label_constraints:
                            op = LabelSelectorOperator.Name(constraint.operator)
                            values = ",".join(constraint.label_values)
                            selector_strs.append(
                                f"{constraint.label_key} {op} [{values}]"
                            )
                    if selector_strs:
                        log_str += (
                            " with label selectors: [" + "; ".join(selector_strs) + "]"
                        )

            log_str += (
                ". Add suitable node types to this cluster to resolve this issue."
            )
            self._logger.warning(log_str)

        if infeasible_gang_requests:
            # Log for each placement group requests.
            for gang_request in infeasible_gang_requests:
                log_str = (
                    "No available node types can fulfill "
                    "placement group requests (detail={details}): ".format(
                        details=gang_request.details
                    )
                )
                requests_by_count = ResourceRequestUtil.group_by_count(
                    gang_request.requests
                )
                for idx, req_count in enumerate(requests_by_count):
                    resource_map = ResourceRequestUtil.to_resource_map(
                        req_count.request
                    )
                    log_str += f"{resource_map}*{req_count.count}"
                    if idx < len(requests_by_count) - 1:
                        log_str += ", "

                log_str += (
                    ". Add suitable node types to this cluster to resolve this issue."
                )
                self._logger.warning(log_str)

        if infeasible_cluster_resource_constraints:
            # We will only have max 1 cluster resource constraint for now since it's
            # from `request_resources()` sdk, where the most recent call would override
            # the previous one.
            for infeasible_constraint in infeasible_cluster_resource_constraints:
                log_str = "No available node types can fulfill cluster constraint: "
                for i, requests_by_count in enumerate(
                    infeasible_constraint.resource_requests
                ):
                    resource_map = ResourceRequestUtil.to_resource_map(
                        requests_by_count.request
                    )
                    log_str += f"{resource_map}*{requests_by_count.count}"
                    if i < len(infeasible_constraint.resource_requests) - 1:
                        log_str += ", "

                log_str += (
                    ". Add suitable node types to this cluster to resolve this issue."
                )
                self._logger.warning(log_str)
