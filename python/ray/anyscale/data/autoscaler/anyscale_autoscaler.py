import logging
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import cached_property
from logging import getLogger
from typing import TYPE_CHECKING, Deque, Dict, Optional, Tuple

import ray
from ray._private.ray_constants import env_bool
from ray.anyscale.air._internal.autoscaling_coordinator import (
    get_or_create_autoscaling_coordinator,
)
from ray.data._internal.execution.autoscaler import Autoscaler, AutoscalingActorPool
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology

logger = getLogger(__name__)


@dataclass(frozen=True)
class _NodeResourceSpec:

    cpu: int
    mem: int

    def __post_init__(self):
        assert isinstance(self.cpu, int)
        assert self.cpu >= 0
        assert isinstance(self.mem, int)
        assert self.mem >= 0

    @classmethod
    def of(cls, cpu, mem):
        cpu = math.floor(cpu)
        mem = math.floor(mem)
        return cls(cpu, mem)

    def to_bundle(self):
        return {"CPU": self.cpu, "memory": self.mem}


class _TimeWindowAverageCalculator:
    """A utility class to calculate the average of values reported in a
    time window."""

    def __init__(
        self,
        window_s: float,
    ):
        assert window_s > 0
        # Time window in seconds.
        self._window_s = window_s
        # Buffer the values reported in the time window, each value is a
        # tuple of (time, value).
        self._values: Deque[Tuple[float, float]] = deque()
        # Sum of all values in the time window.
        self._sum: float = 0

    def report(self, value: float):
        """Report a value to the calculator."""
        now = time.time()
        self._values.append((now, value))
        self._sum += value
        self._trim(now)

    def get_average(self):
        """Get the average of values reported in the time window,
        or None if no values reported in the last time window.
        """
        self._trim(time.time())
        if len(self._values) == 0:
            return None
        return self._sum / len(self._values)

    def _trim(self, now):
        """Remove the values reported outside of the time window."""
        while len(self._values) > 0 and now - self._values[0][0] > self._window_s:
            _, value = self._values.popleft()
            self._sum -= value


class AnyscaleAutoscaler(Autoscaler):
    """Anyscale's proprietary Ray Data autoscaler implementation.

    This class is responsible for both cluster and actor pool autoscaling.
    It works in the following way:

    * Cluster autoscaling:
      * Check the average cluster utilization (CPU and memory)
        in a time window (by default 10s). If the utilization is above a threshold (by
        default 0.85), send a request to Ray's autoscaler to scale up the cluster.
      * Unlike OSS autoscaler, each resource bundle in the request is a node resource
        spec (only CPU and memory), rather than an incremental_resource_usage(). This
        allows us to directly scale up the cluster by a certain ratio.
      * Cluster scaling down is not handled here. It depends on the idle node
        termination.

    * Actor pool autoscaling:
      * For each actor pool, check the average actor pool utilization in a time window
        (`actor_pool_util_avg_window_s`) and other factors (see
        `_actor_pool_should_scale_up/down`) to decide whether to scale up or down
        the actor pool.
      * The actor pool size will be increased by `_actor_pool_scaling_up_factor`
        each time when scaling up. And will be decreased by 1
        each time when scaling down.

    Notes:
      * For now, we assume GPUs are only used by actor pools. So cluster autoscaling
        doesn't need to consider GPU nodes. GPU nodes will scale up as the actor
        pools that require GPUs scale up.
      * It doesn't consider multiple concurrent Datasets for now, as the cluster
        utilization is calculated by "dataset_usage / global_resources".
    """

    # Default threshold of actor pool utilization to trigger scaling up.
    DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD: float = 0.85
    # Default threshold of actor pool utilization to trigger scaling down.
    DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD: float = 0.5
    # Default interval in seconds to check actor pool utilization.
    DEFAULT_ACTOR_POOL_UTIL_CHECK_INTERVAL_S: float = 0.5
    # Default time window in seconds to calculate the average of
    # actor pool utilization.
    DEFAULT_ACTOR_POOL_UTIL_AVG_WINDOW_S: int = 10
    # Default scaling up factor for actor pool autoscaling.
    DEFAULT_ACTOR_POOL_SCALING_UP_FACTOR: float = 2.0

    # Default cluster utilization threshold to trigger scaling up.
    DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD: float = 0.85
    # Default interval in seconds to check cluster utilization.
    DEFAULT_CLUSTER_UTIL_CHECK_INTERVAL_S: float = 0.5
    # Default time window in seconds to calculate the average of cluster utilization.
    DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S: int = 10
    # Default scaling up factor for cluster autoscaling.
    DEFAULT_CLUSTER_SCALING_UP_FACTOR: float = 2.0

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS = 20
    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUEST_EXPIRE_TIME_S = 180
    # Timeout in seconds for getting the result of a call to the AutoscalingCoordinator.
    AUTOSCALING_REQUEST_GET_TIMEOUT_S = 5

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        execution_id: str,
        actor_pool_scaling_up_threshold: float = DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD,  # noqa: E501
        actor_pool_scaling_down_threshold: float = DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD,  # noqa: E501
        actor_pool_scaling_up_factor: float = DEFAULT_ACTOR_POOL_SCALING_UP_FACTOR,
        actor_pool_util_avg_window_s: float = DEFAULT_ACTOR_POOL_UTIL_AVG_WINDOW_S,
        actor_pool_util_check_interval_s: float = DEFAULT_ACTOR_POOL_UTIL_CHECK_INTERVAL_S,  # noqa: E501
        cluster_scaling_up_util_threshold: float = DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD,  # noqa: E501
        cluster_scaling_up_factor: float = DEFAULT_CLUSTER_SCALING_UP_FACTOR,
        cluster_util_avg_window_s: float = DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S,
        cluster_util_check_interval_s: float = DEFAULT_CLUSTER_UTIL_CHECK_INTERVAL_S,
    ):
        assert actor_pool_scaling_down_threshold < actor_pool_scaling_up_threshold
        self._actor_pool_scaling_up_threshold = actor_pool_scaling_up_threshold
        self._actor_pool_scaling_down_threshold = actor_pool_scaling_down_threshold
        assert actor_pool_scaling_up_factor > 1
        self._actor_pool_scaling_up_factor = actor_pool_scaling_up_factor
        assert actor_pool_util_avg_window_s > 0
        self._actor_pool_util_calculators = defaultdict(
            lambda: _TimeWindowAverageCalculator(window_s=actor_pool_util_avg_window_s)
        )
        assert actor_pool_util_check_interval_s >= 0
        self._actor_pool_util_check_interval_s = actor_pool_util_check_interval_s
        # Last time when the actor pool utilization was checked.
        self._last_actor_pool_util_check_time = 0

        # Threshold of cluster utilization to trigger scaling up.
        self._cluster_scaling_up_util_threshold = cluster_scaling_up_util_threshold
        # TODO(hchen): Use proportion-based scaling up factors
        # when https://github.com/anyscale/rayturbo/issues/577 is done.
        assert cluster_scaling_up_factor > 1
        self._cluster_scaling_up_factor = cluster_scaling_up_factor
        assert cluster_util_avg_window_s > 0
        # Calculator to calculate the average of cluster CPU utilization.
        self._cluster_cpu_util_calculator = _TimeWindowAverageCalculator(
            window_s=cluster_util_avg_window_s,
        )
        # Calculator to calculate the average of cluster memory utilization.
        self._cluster_mem_util_calculator = _TimeWindowAverageCalculator(
            window_s=cluster_util_avg_window_s,
        )
        assert cluster_util_check_interval_s >= 0
        self._cluster_util_check_interval_s = cluster_util_check_interval_s
        # Last time when the cluster utilization was checked.
        self._last_cluster_util_check_time = 0
        # Last time when a request was sent to Ray's autoscaler.
        self._last_request_time = 0
        self._requester_id = f"data-{execution_id}"
        # Send an empty request to register ourselves as soon as possible,
        # so the first `get_total_resources` call can get the allocated resources.
        self._send_resource_request([])
        super().__init__(topology, resource_manager, execution_id)

    def try_trigger_scaling(self):
        self._try_scale_up_cluster()
        self._try_scale_up_or_down_actor_pool()

    def _calculate_actor_pool_util(self, actor_pool: AutoscalingActorPool):
        """Calculate the utilization of the given actor pool."""
        if actor_pool.num_running_actors() == 0:
            return None
        # Calculate the utilization based on the used task slots
        # of running actors.
        util = actor_pool.current_in_flight_tasks() / (
            actor_pool.num_running_actors() * actor_pool.max_tasks_in_flight_per_actor()
        )
        self._actor_pool_util_calculators[actor_pool].report(util)
        return self._actor_pool_util_calculators[actor_pool].get_average()

    def _actor_pool_should_scale_up(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
        op_state: "OpState",
        util: float,
    ):
        # Do not scale up, if the op is completed or no more inputs are coming.
        if op.completed() or (op._inputs_complete and op.internal_queue_size() == 0):
            return False
        if actor_pool.current_size() < actor_pool.min_size():
            # Scale up, if the actor pool is below min size.
            return True
        elif actor_pool.current_size() >= actor_pool.max_size():
            # Do not scale up, if the actor pool is already at max size.
            return False
        # Do not scale up, if the op does not have more resources.
        if not op_state._scheduling_status.under_resource_limits:
            return False
        # Do not scale up, if the op has enough free slots for the existing inputs.
        if op_state.num_queued() <= actor_pool.num_free_task_slots():
            return False
        if actor_pool.num_pending_actors() > 0:
            # Do not scale up, if the last scale-up hasn't finished.
            return False
        # Determine whether to scale up based on the actor pool utilization.
        return util > self._actor_pool_scaling_up_threshold

    @cached_property
    def _disable_actor_pool_scaling_down(self):
        # TODO(hchen): re-enable actor pool scaling down after fixing
        # https://github.com/anyscale/rayturbo/issues/726
        return env_bool("RAY_DATA_DISABLE_ACTOR_POOL_SCALING_DOWN", True)

    def _actor_pool_should_scale_down(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
        util: float,
    ):
        # Scale down, if the op is completed or no more inputs are coming.
        if op.completed() or (op._inputs_complete and op.internal_queue_size() == 0):
            return True
        if actor_pool.current_size() > actor_pool.max_size():
            # Scale down, if the actor pool is above max size.
            return True
        elif actor_pool.current_size() <= actor_pool.min_size():
            # Do not scale down, if the actor pool is already at min size.
            return False
        if self._disable_actor_pool_scaling_down:
            return False
        # Determine whether to scale down based on the actor pool utilization.
        return util < self._actor_pool_scaling_down_threshold

    def _try_scale_up_or_down_actor_pool(self):
        now = time.time()
        if (
            now - self._last_actor_pool_util_check_time
            < self._actor_pool_util_check_interval_s
        ):
            return

        self._last_actor_pool_util_check_time = now

        for op, state in self._topology.items():
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                util = self._calculate_actor_pool_util(actor_pool)
                if util is None:
                    continue
                # Try to scale up or down the actor pool.
                should_scale_up = self._actor_pool_should_scale_up(
                    actor_pool,
                    op,
                    state,
                    util,
                )
                should_scale_down = self._actor_pool_should_scale_down(
                    actor_pool, op, util
                )

                current_size = actor_pool.current_size()
                # scale-down has higher priority than scale-up, because when the op
                # is completed, we should scale down the actor pool regardless the
                # utilization.
                if should_scale_down:
                    if actor_pool.scale_down(1):
                        logger.debug(
                            "Scaled down actor pool %s: %d -> %d, current util: %.2f",
                            op.name,
                            current_size,
                            current_size - 1,
                            util,
                        )
                elif should_scale_up:
                    num_to_scale_up = (
                        min(
                            math.ceil(
                                current_size * self._actor_pool_scaling_up_factor
                            ),
                            actor_pool.max_size(),
                        )
                        - current_size
                    )

                    # When we launch an actor for one operator, it decreases the number
                    # of resources available to other operators. So, if we don't update
                    # the budgets before scaling up, we might launch more actors than
                    # the cluster can handle.
                    self._resource_manager.update_usages()
                    budget = self._resource_manager.op_resource_allocator.get_budget(op)
                    max_num_to_scale_up = self._get_max_scale_up(actor_pool, budget)
                    if max_num_to_scale_up is not None:
                        num_to_scale_up = min(num_to_scale_up, max_num_to_scale_up)

                    new_size = actor_pool.scale_up(num_to_scale_up) + current_size
                    logger.debug(
                        "Scaled up actor pool %s: %d -> %d, current util: %.2f",
                        op.name,
                        current_size,
                        new_size,
                        util,
                    )

    def _get_max_scale_up(
        self,
        actor_pool: AutoscalingActorPool,
        budget: ExecutionResources,
    ) -> Optional[int]:
        assert budget.cpu >= 0 and budget.gpu >= 0

        num_cpus_per_actor = actor_pool.per_actor_resource_usage().cpu
        num_gpus_per_actor = actor_pool.per_actor_resource_usage().gpu
        assert num_cpus_per_actor >= 0 and num_gpus_per_actor >= 0

        max_cpu_scale_up: float = float("inf")
        if num_cpus_per_actor > 0 and not math.isinf(budget.cpu):
            max_cpu_scale_up = budget.cpu // num_cpus_per_actor

        max_gpu_scale_up: float = float("inf")
        if num_gpus_per_actor > 0 and not math.isinf(budget.gpu):
            max_gpu_scale_up = budget.gpu // num_gpus_per_actor

        max_scale_up = min(max_cpu_scale_up, max_gpu_scale_up)
        if math.isinf(max_scale_up):
            return None
        else:
            assert not math.isnan(max_scale_up), (
                budget,
                num_cpus_per_actor,
                num_gpus_per_actor,
            )
            return int(max_scale_up)

    def _get_node_resource_spec_and_count(self) -> Dict[_NodeResourceSpec, int]:
        """Get the unique node resource specs and their count in the cluster.

        Similar to `_get_cluster_cpu_and_mem_util`, we only consider CPU and memory
        resources.
        """
        # TODO(hchen): Use the new API to get cluster scaling config
        # when https://github.com/anyscale/rayturbo/issues/577 is done.

        # Filter out the head node and GPU nodes.
        node_resources = [
            node["Resources"]
            for node in ray.nodes()
            if node["Alive"]
            and "node:__internal_head__" not in node["Resources"]
            and "GPU" not in node["Resources"]
        ]

        nodes_resource_spec_count = defaultdict(int)
        for r in node_resources:
            node_resource_spec = _NodeResourceSpec.of(r["CPU"], r["memory"])
            nodes_resource_spec_count[node_resource_spec] += 1

        return nodes_resource_spec_count

    def _get_cluster_cpu_and_mem_util(self) -> Tuple[Optional[float], Optional[float]]:
        """Return CPU and memory utilization of the cluster, or None if
        no data was reported in the last `cluster_util_avg_window_s` seconds or
        `_cluster_util_check_interval_s` seconds have not yet passed since the
        last check.

        We only consider CPU and memory utilization. Because for now we assume GPUs are
        only used by actor pools. GPU node scaling will be handled by
        `try_scale_up_or_down_actor_pool`.
        """
        now = time.time()
        if (
            now - self._last_cluster_util_check_time
            < self._cluster_util_check_interval_s
        ):
            return (
                self._cluster_cpu_util_calculator.get_average(),
                self._cluster_mem_util_calculator.get_average(),
            )
        self._last_cluster_util_check_time = now

        cur_resource_usage = self._resource_manager.get_global_usage()
        global_limits = self._resource_manager.get_global_limits()

        if global_limits.cpu:
            cpu_util = cur_resource_usage.cpu / global_limits.cpu
        else:
            cpu_util = 0
        if global_limits.object_store_memory:
            mem_util = (
                cur_resource_usage.object_store_memory
                / global_limits.object_store_memory
            )
        else:
            mem_util = 0

        self._cluster_cpu_util_calculator.report(cpu_util)
        self._cluster_mem_util_calculator.report(mem_util)

        avg_cpu_util = self._cluster_cpu_util_calculator.get_average()
        avg_mem_util = self._cluster_mem_util_calculator.get_average()

        return avg_cpu_util, avg_mem_util

    def _try_scale_up_cluster(self):
        # Note, should call this method before checking `_last_request_time`,
        # in order to update the average cluster utilization.
        cpu_util, mem_util = self._get_cluster_cpu_and_mem_util()

        # Limit the frequency of autoscaling requests.
        now = time.time()
        if now - self._last_request_time < self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS:
            return

        cpu_util = cpu_util or 0
        mem_util = mem_util or 0
        if (
            cpu_util < self._cluster_scaling_up_util_threshold
            and mem_util < self._cluster_scaling_up_util_threshold
        ):
            logger.debug(
                "Cluster utilization is below threshold: "
                f"CPU={cpu_util:.2f}, memory={mem_util:.2f}."
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
                f"CPU={cpu_util:.2f}, memory={mem_util:.2f}."
                " Requesting resources:"
            )
        # TODO(hchen): We scale up all CPU nodes by the same factor for now.
        # We may want to distinguish different node types based on their individual
        # utilization.
        for node_resource_spec, count in node_resource_spec_count.items():
            bundle = node_resource_spec.to_bundle()
            num_to_request = int(math.ceil(count * self._cluster_scaling_up_factor))
            resource_request.extend([bundle] * num_to_request)
            if logger.isEnabledFor(logging.DEBUG):
                debug_msg += f" [{bundle}: {count} -> {num_to_request}]"
        logger.debug(debug_msg)
        self._send_resource_request(resource_request)

    @cached_property
    def _autoscaling_coordinator(self):
        return get_or_create_autoscaling_coordinator()

    def _send_resource_request(self, resource_request):
        # Make autoscaler resource request.
        try:
            ray.get(
                self._autoscaling_coordinator.request_resources.remote(
                    requester_id=self._requester_id,
                    resources=resource_request,
                    expire_after_s=self.AUTOSCALING_REQUEST_EXPIRE_TIME_S,
                    request_remaining=True,
                ),
                timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,
            )
        except Exception:
            msg = (
                f"Failed to send resource request for {self._requester_id}."
                " If this only happens transiently during network partition or"
                " CPU being overloaded, it's safe to ignore this error."
                " If this error persists, file a GitHub issue."
            )
            logger.warning(msg, exc_info=True)
        self._last_request_time = time.time()

    def on_executor_shutdown(self):
        # Cancel the resource request when the executor is shutting down.
        try:
            ray.get(
                self._autoscaling_coordinator.cancel_request.remote(
                    self._requester_id,
                ),
                timeout=self.AUTOSCALING_REQUEST_GET_TIMEOUT_S,
            )
        except Exception:
            msg = (
                f"Failed to cancel resource request for {self._requester_id}."
                " The request will still expire after the timeout of"
                f" {self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS} seconds."
            )
            logger.warning(msg, exc_info=True)

    def get_total_resources(self) -> ExecutionResources:
        resources = ray.get(
            self._autoscaling_coordinator.get_allocated_resources.remote(
                requester_id=self._requester_id,
            )
        )
        total = ExecutionResources.zero()
        for res in resources:
            total = total.add(ExecutionResources.from_resource_dict(res))
        return total
