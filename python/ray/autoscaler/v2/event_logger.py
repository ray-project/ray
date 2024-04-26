import logging
from collections import defaultdict
from typing import Dict, List, Optional

from ray._private.event.event_logger import EventLoggerAdapter
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.schema import NodeType
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    ResourceRequest,
)
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
        node_type_configs: Dict[NodeType, NodeTypeConfig],
        cluster_shape: Dict[NodeType, int],
        launch_requests: Optional[List[LaunchRequest]] = None,
        terminate_requests: Optional[List[TerminationRequest]] = None,
        infeasible_requests: Optional[List[ResourceRequest]] = None,
        infeasible_gang_requests: Optional[List[GangResourceRequest]] = None,
        infeasible_cluster_resource_constraints: Optional[
            List[ClusterResourceConstraint]
        ] = None,
    ) -> None:
        """
        Log any update of the cluster scheduling state.
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
            total_resources = defaultdict(float)

            for node_type, count in cluster_shape.items():
                node_config = node_type_configs[node_type]
                for resource_name, resource_quantity in node_config.resources.items():
                    total_resources[resource_name] += resource_quantity * count

            num_cpus = total_resources.get("CPU", 0)
            log_str = f"Resized to {int(num_cpus)} CPUs"

            if "GPU" in total_resources:
                log_str += f", {int(total_resources['GPU'])} GPUs"
            if "TPU" in total_resources:
                log_str += f", {int(total_resources['TPU'])} TPUs"

            self._logger.info(f"{log_str}.")
            self._logger.debug(f"Current cluster shape: {dict(cluster_shape)}.")

        # Log any infeasible requests.
        if infeasible_requests:
            requests_by_count = ResourceRequestUtil.group_by_count(infeasible_requests)
            log_str = "No available node types can fulfill resource requests "
            for idx, req_count in enumerate(requests_by_count):
                resource_map = ResourceRequestUtil.to_resource_map(req_count.request)
                log_str += f"{resource_map}*{req_count.count}"
                if idx < len(requests_by_count) - 1:
                    log_str += ", "

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
                    infeasible_constraint.min_bundles
                ):
                    resource_map = ResourceRequestUtil.to_resource_map(
                        requests_by_count.request
                    )
                    log_str += f"{resource_map}*{requests_by_count.count}"
                    if i < len(infeasible_constraint.min_bundles) - 1:
                        log_str += ", "

                log_str += (
                    ". Add suitable node types to this cluster to resolve this issue."
                )
                self._logger.warning(log_str)
