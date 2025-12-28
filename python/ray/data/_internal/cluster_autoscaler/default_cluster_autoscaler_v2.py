import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Dict, Optional

import ray
from .default_autoscaling_coordinator import (
    DefaultAutoscalingCoordinator,
)
from .resource_utilization_gauge import (
    ResourceUtilizationGauge,
    RollingLogicalUtilizationGauge,
)
from ray.data._internal.cluster_autoscaler import ClusterAutoscaler
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = getLogger(__name__)


@dataclass(frozen=True)
class _NodeResourceSpec:

    cpu: int
    gpu: int
    mem: int

    def __post_init__(self):
        assert isinstance(self.cpu, int)
        assert self.cpu >= 0
        assert isinstance(self.gpu, int)
        assert self.gpu >= 0
        assert isinstance(self.mem, int)
        assert self.mem >= 0

    @classmethod
    def of(cls, *, cpu=0, gpu=0, mem=0):
        cpu = math.floor(cpu)
        gpu = math.floor(gpu)
        mem = math.floor(mem)
        return cls(cpu=cpu, gpu=gpu, mem=mem)

    def to_bundle(self):
        return {"CPU": self.cpu, "GPU": self.gpu, "memory": self.mem}


class DefaultClusterAutoscalerV2(ClusterAutoscaler):
    """Ray Data's second cluster autoscaler implementation.

    It works in the following way:

      * Check the average cluster utilization (CPU and memory)
        in a time window (by default 10s). If the utilization is above a threshold (by
        default 0.75), send a request to Ray's autoscaler to scale up the cluster.
      * Unlike previous implementation, each resource bundle in the request is a node
        resource spec, rather than an `incremental_resource_usage()`. This allows us
        to directly scale up nodes.
      * Cluster scaling down isn't handled here. It depends on the idle node
        termination.

    Notes:
      * It doesn't consider multiple concurrent Datasets for now, as the cluster
        utilization is calculated by "dataset_usage / global_resources".
    """

    # Default cluster utilization threshold to trigger scaling up.
    DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD: float = 0.75
    # Default interval in seconds to check cluster utilization.
    DEFAULT_CLUSTER_UTIL_CHECK_INTERVAL_S: float = 0.25
    # Default time window in seconds to calculate the average of cluster utilization.
    DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S: int = 10
    # Default number of nodes to add per node type.
    DEFAULT_CLUSTER_SCALING_UP_DELTA: int = 1

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS = 10
    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUEST_EXPIRE_TIME_S = 180
    # Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
    AUTOSCALING_REQUEST_GET_TIMEOUT_S = 5

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        execution_id: str,
        resource_utilization_calculator: Optional[ResourceUtilizationGauge] = None,
        cluster_scaling_up_util_threshold: float = DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD,  # noqa: E501
        cluster_scaling_up_delta: float = DEFAULT_CLUSTER_SCALING_UP_DELTA,
        cluster_util_avg_window_s: float = DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S,
        cluster_util_check_interval_s: float = DEFAULT_CLUSTER_UTIL_CHECK_INTERVAL_S,
    ):
        if resource_utilization_calculator is None:
            assert cluster_util_check_interval_s >= 0, cluster_util_check_interval_s
            resource_utilization_calculator = RollingLogicalUtilizationGauge(
                resource_manager, cluster_util_avg_window_s=cluster_util_avg_window_s
            )

        self._resource_utilization_calculator = resource_utilization_calculator
        # Threshold of cluster utilization to trigger scaling up.
        self._cluster_scaling_up_util_threshold = cluster_scaling_up_util_threshold
        assert cluster_scaling_up_delta > 0
        self._cluster_scaling_up_delta = cluster_scaling_up_delta
        assert cluster_util_avg_window_s > 0
        self._cluster_util_check_interval_s = cluster_util_check_interval_s
        # Last time when the cluster utilization was checked.
        self._last_cluster_util_check_time = 0
        # Last time when a request was sent to Ray's autoscaler.
        self._last_request_time = 0
        self._requester_id = f"data-{execution_id}"
        self._autoscaling_coordinator = DefaultAutoscalingCoordinator()
        # Send an empty request to register ourselves as soon as possible,
        # so the first `get_total_resources` call can get the allocated resources.
        self._send_resource_request([])
        super().__init__(topology, resource_manager, execution_id)

    def _get_node_resource_spec_and_count(self) -> Dict[_NodeResourceSpec, int]:
        """Get the unique node resource specs and their count in the cluster."""
        # Filter out the head node.
        node_resources = [
            node["Resources"]
            for node in ray.nodes()
            if node["Alive"] and "node:__internal_head__" not in node["Resources"]
        ]

        nodes_resource_spec_count = defaultdict(int)
        for r in node_resources:
            node_resource_spec = _NodeResourceSpec.of(
                cpu=r["CPU"], gpu=r.get("GPU", 0), mem=r["memory"]
            )
            nodes_resource_spec_count[node_resource_spec] += 1

        return nodes_resource_spec_count

    def try_trigger_scaling(self):
        # Note, should call this method before checking `_last_request_time`,
        # in order to update the average cluster utilization.
        now = time.time()
        if (
            now - self._last_cluster_util_check_time
            >= self._cluster_util_check_interval_s
        ):
            # Update observed resource utilization
            self._last_cluster_util_check_time = now

            self._resource_utilization_calculator.observe()

        # Limit the frequency of autoscaling requests.
        if now - self._last_request_time < self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS:
            return

        util = self._resource_utilization_calculator.get()
        if (
            util.cpu < self._cluster_scaling_up_util_threshold
            and util.gpu < self._cluster_scaling_up_util_threshold
            and util.object_store_memory < self._cluster_scaling_up_util_threshold
        ):
            logger.debug(
                "Cluster utilization is below threshold: "
                f"CPU={util.cpu:.2f}, GPU={util.gpu:.2f}, memory={util.object_store_memory:.2f}."
            )
            # Still send an empty request when upscaling is not needed,
            # to renew our registration on AutoscalingCoordinator.
            self._send_resource_request([])
            return

        resource_request = []
        node_resource_spec_count = self._get_node_resource_spec_and_count()
        debug_msg = ""
        if logger.isEnabledFor(logging.DEBUG):
            debug_msg = (
                "Scaling up cluster. Current utilization: "
                f"CPU={util.cpu:.2f}, GPU={util.gpu:.2f}, object_store_memory={util.object_store_memory:.2f}."
                " Requesting resources:"
            )
        # TODO(hchen): We scale up all nodes by the same delta for now.
        # We may want to distinguish different node types based on their individual
        # utilization.
        for node_resource_spec, count in node_resource_spec_count.items():
            bundle = node_resource_spec.to_bundle()
            num_to_request = int(math.ceil(count + self._cluster_scaling_up_delta))
            resource_request.extend([bundle] * num_to_request)
            if logger.isEnabledFor(logging.DEBUG):
                debug_msg += f" [{bundle}: {count} -> {num_to_request}]"
        logger.debug(debug_msg)
        self._send_resource_request(resource_request)

    def _send_resource_request(self, resource_request):
        # Make autoscaler resource request.
        self._autoscaling_coordinator.request_resources(
            requester_id=self._requester_id,
            resources=resource_request,
            expire_after_s=self.AUTOSCALING_REQUEST_EXPIRE_TIME_S,
            request_remaining=True,
        )
        self._last_request_time = time.time()

    def on_executor_shutdown(self):
        # Cancel the resource request when the executor is shutting down.
        try:
            self._autoscaling_coordinator.cancel_request(self._requester_id)
        except Exception:
            msg = (
                f"Failed to cancel resource request for {self._requester_id}."
                " The request will still expire after the timeout of"
                f" {self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS} seconds."
            )
            logger.warning(msg, exc_info=True)

    def get_total_resources(self) -> ExecutionResources:
        resources = self._autoscaling_coordinator.get_allocated_resources(
            requester_id=self._requester_id
        )
        total = ExecutionResources.zero()
        for res in resources:
            total = total.add(ExecutionResources.from_resource_dict(res))
        return total
