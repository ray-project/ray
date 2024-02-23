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
    def __init__(self, logger: EventLoggerAdapter):
        self._base_event_logger = logger

    def log_cluster_scheduling_update(
        self,
        node_type_configs: Dict[NodeType, NodeTypeConfig],
        cluster_shape: Dict[NodeType, int],
        launch_requests: Optional[List[LaunchRequest]] = None,
        terminate_requests: Optional[List[TerminationRequest]] = None,
        infeasible_requests: Optional[List[ResourceRequest]] = None,
        infeasible_gang_requests: Optional[List[GangResourceRequest]] = None,
        infeasible_constraints: Optional[List[ClusterResourceConstraint]] = None,
    ) -> None:
        """
        Log any update of the cluster scheduling state.
        """

        # Log any launch events.
        if launch_requests:
            launch_type_count = defaultdict(int)
            for req in launch_requests:
                launch_type_count[req.instance_type] += req.count

            log_str = "Adding "
            for idx, (instance_type, count) in enumerate(launch_type_count.items()):
                log_str += f"{count} node(s) of type {instance_type}"
                if idx < len(launch_type_count) - 1:
                    log_str += ", "

            self._base_event_logger.info(f"{log_str}.")
            logger.info(f"{log_str}.")

        # Log any terminate events.
        if terminate_requests:
            termination_by_causes = defaultdict(int)
            for req in terminate_requests:
                termination_by_causes[req.cause] += 1

            cause_reason_map = {
                TerminationRequest.Cause.OUTDATED: "they are outdated",
                TerminationRequest.Cause.MAX_NUM_NODES: "max number of worker nodes reached",  # noqa
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE: "max number of worker nodes per type reached",  # noqa
                TerminationRequest.Cause.IDLE: "they are idle",
            }

            log_str = "Terminating "
            for idx, (cause, count) in enumerate(termination_by_causes.items()):
                log_str += f"{count} nodes because {cause_reason_map[cause]}"
                if idx < len(termination_by_causes) - 1:
                    log_str += ", "

            self._base_event_logger.info(f"{log_str}.")
            logger.info(f"{log_str}.")

        # Cluster shape changes.
        if launch_requests or terminate_requests:
            num_cpus = sum(
                [
                    node_type_configs[node_type].resources.get("CPU", 0) * count
                    for node_type, count in cluster_shape.items()
                ]
            )
            num_gpus = sum(
                [
                    node_type_configs[node_type].resources.get("GPU", 0) * count
                    for node_type, count in cluster_shape.items()
                ]
            )
            num_tpus = sum(
                [
                    node_type_configs[node_type].resources.get("TPU", 0) * count
                    for node_type, count in cluster_shape.items()
                ]
            )

            self._base_event_logger.info(
                f"Resized to {num_cpus} CPUs, {num_gpus} GPUs, {num_tpus} TPUs."
            )
            logger.info(
                f"Resized to {num_cpus} CPUs, {num_gpus} GPUs, {num_tpus} TPUs."
            )
            self._base_event_logger.debug(
                f"Current cluster shape: {dict(cluster_shape)}."
            )

        # Log any infeasible requests.
        if infeasible_requests:
            requests_by_count = ResourceRequestUtil.group_by_count(infeasible_requests)
            log_str = "No available node types could fulfill: "
            for idx, req_count in enumerate(requests_by_count):
                resource_map = ResourceRequestUtil.to_resource_map(req_count.request)
                log_str += f"{resource_map}*{req_count.count}"
                if idx < len(requests_by_count) - 1:
                    log_str += ", "

            self._base_event_logger.warning(log_str)

        if infeasible_gang_requests:
            # Log for each placement group requests.
            for gang_request in infeasible_gang_requests:
                log_str = (
                    "No available node types could fulfill "
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

                self._base_event_logger.warning(log_str)
                logger.warning(log_str)

        if infeasible_constraints:
            # We actually only have 1.
            for infeasible_constraint in infeasible_constraints:
                log_str = "No available node types could fulfill cluster constraint: "
                for i, requests_by_count in enumerate(
                    infeasible_constraint.min_bundles
                ):
                    resource_map = ResourceRequestUtil.to_resource_map(
                        requests_by_count.request
                    )
                    log_str += f"{resource_map}*{requests_by_count.count}"
                    if i < len(infeasible_constraint.min_bundles) - 1:
                        log_str += ", "

                self._base_event_logger.warning(log_str)
                logger.warning(log_str)
