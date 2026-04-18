import logging
import math
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import ray
from .base_autoscaling_coordinator import AutoscalingCoordinator, ResourceDict
from .default_autoscaling_coordinator import (
    DefaultAutoscalingCoordinator,
)
from .resource_utilization_gauge import (
    ResourceUtilizationGauge,
    RollingLogicalUtilizationGauge,
)
from .util import cap_resource_request_to_limits, is_autoscaling_enabled
from ray._common.utils import env_bool, env_float, env_integer
from ray.data._internal.cluster_autoscaler import ClusterAutoscaler
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.util import memory_string
from ray.data._internal.util import GiB

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager

logger = logging.getLogger(__name__)


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

    def __str__(self):
        return (
            "{"
            + f"CPU: {self.cpu}, GPU: {self.gpu}, memory: {memory_string(self.mem)}"
            + "}"
        )

    @classmethod
    def of(cls, *, cpu=0, gpu=0, mem=0):
        cpu = math.floor(cpu)
        gpu = math.floor(gpu)
        # Round memory to the nearest 0.1 GiB so that nodes of the same type
        # with slightly different reported physical memory are grouped together.
        mem = int(round(mem / GiB, 1) * GiB) if mem > 0 else 0
        return cls(cpu=cpu, gpu=gpu, mem=mem)

    @classmethod
    def from_bundle(cls, bundle: Dict[str, Any]) -> "_NodeResourceSpec":
        return _NodeResourceSpec.of(
            cpu=bundle.get("CPU", 0),
            gpu=bundle.get("GPU", 0),
            mem=bundle.get("memory", 0),
        )

    def to_bundle(self):
        return {"CPU": self.cpu, "GPU": self.gpu, "memory": self.mem}


def _get_node_resource_spec_and_count() -> Dict[_NodeResourceSpec, int]:
    """Get the unique node resource specs and their count in the cluster."""
    nodes_resource_spec_count = defaultdict(int)

    cluster_config = ray._private.state.state.get_cluster_config()
    if cluster_config and cluster_config.node_group_configs:
        for node_group_config in cluster_config.node_group_configs:
            if not node_group_config.resources or node_group_config.max_count == 0:
                continue

            node_resource_spec = _NodeResourceSpec.from_bundle(
                node_group_config.resources
            )
            nodes_resource_spec_count[node_resource_spec] = 0

    # Filter out the head node.
    node_resources = [
        node["Resources"]
        for node in ray.nodes()
        if node["Alive"] and "node:__internal_head__" not in node["Resources"]
    ]

    for r in node_resources:
        node_resource_spec = _NodeResourceSpec.from_bundle(r)
        nodes_resource_spec_count[node_resource_spec] += 1

    return nodes_resource_spec_count


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
    """

    # Default cluster utilization threshold to trigger scaling up.
    DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD: float = env_float(
        "RAY_DATA_CLUSTER_SCALING_UP_UTIL_THRESHOLD",
        0.75,
    )
    # Default time window in seconds to calculate the average of cluster utilization.
    DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S: int = env_integer(
        "RAY_DATA_CLUSTER_UTIL_AVG_WINDOW_S",
        10,
    )
    # Default number of nodes to add per node type.
    DEFAULT_CLUSTER_SCALING_UP_DELTA: int = env_integer(
        "RAY_DATA_CLUSTER_SCALING_UP_DELTA",
        1,
    )

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS: int = env_integer(
        "RAY_DATA_MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS",
        10,
    )
    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUEST_EXPIRE_TIME_S: int = env_integer(
        "RAY_DATA_AUTOSCALING_REQUEST_EXPIRE_TIME_S",
        180,
    )
    # When utilization drops below the scale-up threshold, keep renewing the last
    # explicit request for a short time before releasing it.
    DEFAULT_LOW_UTIL_REQUEST_RELEASE_DELAY_S: float = env_float(
        "RAY_DATA_LOW_UTIL_REQUEST_RELEASE_DELAY_S",
        180,
    )
    # Whether to disable INFO-level logs.
    RAY_DATA_DISABLE_AUTOSCALER_LOGGING = env_bool(
        "RAY_DATA_DISABLE_AUTOSCALER_LOGGING", False
    )

    def __init__(
        self,
        resource_manager: "ResourceManager",
        execution_id: str,
        resource_limits: ExecutionResources = ExecutionResources.inf(),
        resource_utilization_calculator: Optional[ResourceUtilizationGauge] = None,
        cluster_scaling_up_util_threshold: float = DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD,  # noqa: E501
        cluster_scaling_up_delta: float = DEFAULT_CLUSTER_SCALING_UP_DELTA,
        cluster_util_avg_window_s: float = DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S,
        min_gap_between_autoscaling_requests_s: float = MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS,  # noqa: E501
        low_util_request_release_delay_s: float = DEFAULT_LOW_UTIL_REQUEST_RELEASE_DELAY_S,  # noqa: E501
        autoscaling_coordinator: Optional[AutoscalingCoordinator] = None,
        get_node_counts: Callable[[], Dict[_NodeResourceSpec, int]] = (
            _get_node_resource_spec_and_count
        ),
        get_time: Callable[[], float] = time.time,
    ):
        assert cluster_scaling_up_delta > 0
        assert cluster_util_avg_window_s > 0
        assert min_gap_between_autoscaling_requests_s >= 0
        assert low_util_request_release_delay_s >= 0

        if resource_utilization_calculator is None:
            resource_utilization_calculator = RollingLogicalUtilizationGauge(
                resource_manager,
                cluster_util_avg_window_s=cluster_util_avg_window_s,
                execution_id=execution_id,
            )

        if autoscaling_coordinator is None:
            autoscaling_coordinator = DefaultAutoscalingCoordinator()

        self._resource_limits = resource_limits
        self._resource_utilization_calculator = resource_utilization_calculator
        # Threshold of cluster utilization to trigger scaling up.
        self._cluster_scaling_up_util_threshold = cluster_scaling_up_util_threshold
        self._cluster_scaling_up_delta = int(math.ceil(cluster_scaling_up_delta))
        self._min_gap_between_autoscaling_requests_s = (
            min_gap_between_autoscaling_requests_s
        )
        self._low_util_request_release_delay_s = low_util_request_release_delay_s
        # Last time when a request was sent to Ray's autoscaler.
        self._last_request_time = 0
        # Track the last non-empty explicit request so low-utilization heartbeats
        # can keep it alive briefly without turning allocated remaining-share
        # resources into explicit autoscaler demand.
        self._last_non_empty_resource_request: List[ResourceDict] = []
        self._last_non_empty_request_time: Optional[float] = None
        self._requester_id = f"data-{execution_id}"
        self._autoscaling_coordinator = autoscaling_coordinator
        self._get_node_counts = get_node_counts
        self._get_time = get_time
        self._autoscaling_enabled = is_autoscaling_enabled()

        # Register with the coordinator immediately so the actor knows about this
        # requester before the first get_allocated_resources call. The cached value
        # returned by get_allocated_resources (and thus get_total_resources) will be
        # empty until the actor responds with the first allocation (cold-start).
        self._send_resource_request([])

    def try_trigger_scaling(self):
        # Note, should call this method before checking `_last_request_time`,
        # in order to update the average cluster utilization.
        self._resource_utilization_calculator.observe()

        # Limit the frequency of autoscaling requests.
        now = self._get_time()
        if now - self._last_request_time < self._min_gap_between_autoscaling_requests_s:
            return

        util = self._resource_utilization_calculator.get()
        if (
            util.cpu < self._cluster_scaling_up_util_threshold
            and util.gpu < self._cluster_scaling_up_util_threshold
            and util.memory < self._cluster_scaling_up_util_threshold
            and util.object_store_memory < self._cluster_scaling_up_util_threshold
        ):
            logger.debug(
                "Cluster utilization is below threshold: "
                f"CPU={util.cpu:.2f}, GPU={util.gpu:.2f}, memory={util.memory:.2f}, "
                f"object_store_memory={util.object_store_memory:.2f}."
            )
            self._send_resource_request(None)
            return

        # We separate active bundles (existing nodes) from pending bundles (scale-up delta)
        # to ensure existing nodes' resources are never crowded out by scale-up requests.
        # TODO(hchen): We scale up all nodes by the same delta for now.
        # We may want to distinguish different node types based on their individual
        # utilization.
        active_bundles = []
        pending_bundles = []
        node_resource_spec_count = self._get_node_counts()
        for node_resource_spec, count in node_resource_spec_count.items():
            bundle = node_resource_spec.to_bundle()
            # Bundles for existing nodes -> active (must include)
            active_bundles.extend([bundle] * count)
            # Bundles for scale-up delta -> pending (best-effort)
            pending_bundles.extend([bundle] * self._cluster_scaling_up_delta)

        # Cap the resource request to respect user-configured limits.
        # Active bundles (existing nodes) are always included; pending bundles
        # (scale-up requests) are best-effort.
        resource_request = cap_resource_request_to_limits(
            active_bundles, pending_bundles, self._resource_limits
        )

        if resource_request != active_bundles:
            self._log_resource_request(util, active_bundles, resource_request)

        self._send_resource_request(resource_request)

    def _log_resource_request(
        self,
        current_utilization: ExecutionResources,
        active_bundles: List[Dict[str, float]],
        resource_request: List[Dict[str, float]],
    ) -> None:
        message = (
            "The utilization of one or more logical resource is higher than the "
            f"specified threshold of {self._cluster_scaling_up_util_threshold:.0%}: "
            f"CPU={current_utilization.cpu:.0%}, GPU={current_utilization.gpu:.0%}, "
            f"memory={current_utilization.memory:.0%}, "
            f"object_store_memory={current_utilization.object_store_memory:.0%}. "
            f"Requesting {self._cluster_scaling_up_delta} node(s) of each shape:"
        )

        current_node_counts = Counter(
            [_NodeResourceSpec.from_bundle(bundle) for bundle in active_bundles]
        )
        requested_node_counts = Counter(
            [_NodeResourceSpec.from_bundle(bundle) for bundle in resource_request]
        )
        for node_spec, requested_count in requested_node_counts.items():
            current_count = current_node_counts.get(node_spec, 0)
            message += f" [{node_spec}: {current_count} -> {requested_count}]"

        if self.RAY_DATA_DISABLE_AUTOSCALER_LOGGING or not self._autoscaling_enabled:
            level = logging.DEBUG
        else:
            level = logging.INFO

        logger.log(level, message)

    def _should_keep_non_empty_request(self, now: float) -> bool:
        return (
            self._last_non_empty_request_time is not None
            and now - self._last_non_empty_request_time
            < self._low_util_request_release_delay_s
        )

    def _send_resource_request(
        self,
        resource_request: Optional[List[ResourceDict]],
    ):
        now = self._get_time()
        update_non_empty_request_state = True
        if resource_request is None:
            if self._should_keep_non_empty_request(now):
                resource_request = self._last_non_empty_resource_request
                update_non_empty_request_state = False
            else:
                # Renew our registration on AutoscalingCoordinator without
                # keeping explicit autoscaler demand alive.
                resource_request = []

        # Make autoscaler resource request.
        self._autoscaling_coordinator.request_resources(
            requester_id=self._requester_id,
            resources=resource_request,
            expire_after_s=self.AUTOSCALING_REQUEST_EXPIRE_TIME_S,
            request_remaining=True,
        )
        if resource_request and update_non_empty_request_state:
            self._last_non_empty_resource_request = [
                bundle.copy() for bundle in resource_request
            ]
            self._last_non_empty_request_time = now
        elif not resource_request:
            self._last_non_empty_resource_request = []
            self._last_non_empty_request_time = None
        self._last_request_time = now

    def on_executor_shutdown(self):
        # Cancel the resource request when the executor is shutting down.
        try:
            self._autoscaling_coordinator.cancel_request(self._requester_id)
        except Exception:
            # Intentionally broad: also catches RuntimeError raised by
            # cancel_request after MAX_CONSECUTIVE_FAILURES. At shutdown there
            # is nothing useful to do except log and let the request expire.
            msg = (
                f"Failed to cancel resource request for {self._requester_id}."
                " The request will still expire after the timeout of"
                f" {self.AUTOSCALING_REQUEST_EXPIRE_TIME_S} seconds."
            )
            logger.warning(msg, exc_info=True)

    def get_total_resources(self) -> ExecutionResources:
        """Get total resources available from the autoscaling coordinator."""
        resources = self._autoscaling_coordinator.get_allocated_resources(
            requester_id=self._requester_id
        )
        total = ExecutionResources.zero()
        for res in resources:
            total = total.add(ExecutionResources.from_resource_dict(res))
        return total
