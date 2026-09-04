import logging
import math
import time
from collections import Counter
from typing import TYPE_CHECKING, Dict, List, Optional

from .base_autoscaling_coordinator import AutoscalingCoordinator
from .base_cluster_autoscaler import ClusterAutoscaler
from .default_autoscaling_coordinator import DefaultAutoscalingCoordinator
from .resource_utilization_gauge import (
    ClusterUtil,
    ResourceUtilizationGauge,
    RollingLogicalUtilizationGauge,
)
from .supports_cluster_autoscaling import SupportsClusterAutoscaling
from .throughput_solver import allocate_resources, compute_optimal_throughput
from ray._common.utils import env_float, env_integer
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
)
from ray.data._internal.util import get_max_task_capacity

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)

SHUFFLE_OP_TYPES = (AllToAllOperator, HashShufflingOperatorBase)


def _to_resource_bundle(resources: ExecutionResources) -> Dict[str, float]:
    """Convert ExecutionResources to a resource bundle dict for the autoscaler.

    Excludes object_store_memory and filters out zero values.
    """
    resource_dict = resources.copy(object_store_memory=0).to_resource_dict()
    return {k: v for k, v in resource_dict.items() if v > 0}


class RateBasedClusterAutoscaler(ClusterAutoscaler):
    """Rate-based cluster autoscaler.

    This autoscaler uses per-operator throughput rates to compute optimal resource
    allocations and scales the cluster accordingly.

    This autoscaler only scales up the cluster. It relies on idle termination to scale
    down.
    """

    # Default scaling up factor for cluster autoscaling.
    DEFAULT_CLUSTER_SCALING_UP_FACTOR: float = env_float(
        "RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_FACTOR", 2.0
    )

    # Default max resource delta for cluster autoscaling.
    DEFAULT_CLUSTER_SCALING_UP_MAX_RESOURCE_DELTA: ExecutionResources = ExecutionResources.for_limits(
        cpu=env_float(
            "RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_MAX_CPU_RESOURCE_DELTA", 256.0
        ),
        # 32 was chosen because it's not too low so that scaling by 2 is worth it
        # for smaller clusters and not too high to prevent scaling too many nodes at
        # a time. In english, this means "no more than 32 additional GPUs can be
        # requested at a time".
        gpu=env_float(
            "RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_MAX_GPU_RESOURCE_DELTA", 32.0
        ),
        memory=env_float(
            "RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_MAX_MEMORY_RESOURCE_DELTA",
            float("inf"),
        ),
    )

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS_S = env_integer(
        "RAY_DATA_MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS", 10
    )

    # The time in seconds after which an autoscaling request will expire.
    AUTOSCALING_REQUEST_EXPIRE_TIME_S: int = env_integer(
        "RAY_DATA_AUTOSCALING_REQUEST_EXPIRE_TIME_S", 180
    )

    # Default cluster utilization thresholds to trigger scaling up.
    DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD: ClusterUtil = ClusterUtil(
        cpu=env_float("RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_CPU_UTIL_THRESHOLD", 0.50),
        gpu=env_float("RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_GPU_UTIL_THRESHOLD", 0.50),
        memory=env_float(
            "RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_MEMORY_UTIL_THRESHOLD", 0.50
        ),
        object_store_memory=env_float(
            "RAY_DATA_DEFAULT_CLUSTER_SCALING_UP_OBJECT_STORE_MEMORY_UTIL_THRESHOLD",
            0.50,
        ),
    )

    # Default to no limits. The user can tune this with
    # `ExecutionOptions.resource_limits`.
    DEFAULT_MAX_CLUSTER_LIMITS: ExecutionResources = ExecutionResources.for_limits()

    def __init__(
        self,
        ops: List[SupportsClusterAutoscaling],
        execution_id: str,
        *,
        utility_calculator: ResourceUtilizationGauge,
        max_cluster_limits: ExecutionResources = DEFAULT_MAX_CLUSTER_LIMITS,
        autoscaling_coordinator: Optional["AutoscalingCoordinator"] = None,
        cluster_scaling_up_util_threshold: ClusterUtil = DEFAULT_CLUSTER_SCALING_UP_UTIL_THRESHOLD,
        cluster_scaling_up_factor: float = DEFAULT_CLUSTER_SCALING_UP_FACTOR,
        cluster_scaling_up_max_resource_delta: ExecutionResources = DEFAULT_CLUSTER_SCALING_UP_MAX_RESOURCE_DELTA,
        min_gap_between_autoscaling_requests_s: int = MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS_S,
        autoscaling_request_expire_time_s: int = AUTOSCALING_REQUEST_EXPIRE_TIME_S,
        label_selector: Optional[Dict[str, str]] = None,
    ):
        """Initialize the cluster autoscaler.

        Args:
            ops: The operators to autoscale.
            execution_id: The execution ID of the dataset. This is used to identify the
                dataset when requesting resources.
            utility_calculator: The calculator to track and compute cluster resource
                utilization (CPU, GPU, object store memory). Used to determine if cluster
                utilization is high enough to trigger scaling up.
            max_cluster_limits: Maximum cluster resource limits. Used to clamp resource
                requests to ensure we don't exceed the maximum cluster capacity.
            autoscaling_coordinator: The `AutoscalingCoordinator` to request resources
                from. This is exposed as a seam for testing. If not provided, this uses
                the default coordinator.
            cluster_scaling_up_util_threshold: Per-resource utilization thresholds
                that must be exceeded before scaling up. If CPU, GPU, and object store
                memory utilization are all below their respective thresholds, the
                autoscaler will not scale up even if there is a bottleneck.
                Defaults to 0.50 (50%) for each resource type.
            cluster_scaling_up_factor: The factor to scale up the cluster.
            cluster_scaling_up_max_resource_delta: Maximum absolute increase in
                resources when scaling up, per resource type (cpu, gpu, memory).
            min_gap_between_autoscaling_requests_s: The minimum gap between two
                autoscaling requests. This is exposed as a seam for testing.
            autoscaling_request_expire_time_s: The number of seconds before requested
                resources expire. This is exposed as a seam for testing.
            label_selector: Label selector pinning this requester to a single
                subcluster. Forwarded to the `DefaultAutoscalingCoordinator` as
                `subcluster_selector` so node bucketing, remaining-resource
                eligibility, and bundle stamping are scoped to the subcluster.
        """
        assert all(
            isinstance(op, SupportsClusterAutoscaling) for op in ops
        ), f"All ops must implement SupportsClusterAutoscaling, got {[type(op) for op in ops]}"
        assert (
            cluster_scaling_up_factor > 1.0
        ), f"cluster_scaling_up_factor must be > 1.0, got {cluster_scaling_up_factor}"

        if autoscaling_coordinator is None:
            autoscaling_coordinator = DefaultAutoscalingCoordinator(
                requester_id=f"data-{execution_id}",
                subcluster_selector=label_selector,
            )

        self._non_shuffle_ops = [
            op for op in ops if not isinstance(op, SHUFFLE_OP_TYPES)
        ]
        self._shuffle_ops = [op for op in ops if isinstance(op, SHUFFLE_OP_TYPES)]
        self._execution_id = execution_id
        self._max_cluster_limits = max_cluster_limits
        self._utility_calculator = utility_calculator
        self._autoscaling_coordinator = autoscaling_coordinator
        self._cluster_scaling_up_factor = cluster_scaling_up_factor
        self._cluster_scaling_up_max_resource_delta = (
            cluster_scaling_up_max_resource_delta
        )
        self._min_gap_between_autoscaling_requests = (
            min_gap_between_autoscaling_requests_s
        )
        self._autoscaling_request_expire_time_s = autoscaling_request_expire_time_s
        self._cluster_scaling_up_util_threshold = cluster_scaling_up_util_threshold
        self._last_request_time = 0.0
        self._requester_id = f"data-{execution_id}"
        self._last_resource_request = []

        # Log the initialized values.
        logger.debug("=== Rate-Based Autoscaler: Initialized ===")
        logger.debug(f"  max_cluster_limits: {max_cluster_limits}")
        logger.debug(
            f"  scaling_up_util_threshold: {cluster_scaling_up_util_threshold}"
        )
        logger.debug(f"  scaling_up_factor: {cluster_scaling_up_factor}")
        logger.debug(
            f"  scaling_up_max_resource_delta: {cluster_scaling_up_max_resource_delta}"
        )
        logger.debug(
            f"  min_gap_between_requests: {min_gap_between_autoscaling_requests_s}s"
        )
        logger.debug(f"  request_expire_time: {autoscaling_request_expire_time_s}s")

        # Send an empty request to register ourselves as soon as possible,
        # so the first `get_total_resources` call can get the allocated resources.
        self._send_resource_request([])

    @classmethod
    def create(
        cls,
        topology: Dict[SupportsClusterAutoscaling, "OpState"],
        execution_options: ExecutionOptions,
        resource_manager: "ResourceManager",
        *,
        execution_id: str,
    ) -> "RateBasedClusterAutoscaler":
        """Create a cluster autoscaler.

        This logic is defined here to minimize the risk of merge conflicts in the
        streaming executor, and keep the `ray.data._internal.cluster_autoscaler`
        `__init__` file small.
        """
        # `SupportsClusterAutoscaling` defines the subset of `PhysicalOperator` methods
        # required for this implementation. We depend on a protocol rather than
        # `PhyiscalOperator` directly because the `PhysicalOperator` interface is
        # wide and hard to explicitly stub for testing.
        assert all(
            isinstance(op, SupportsClusterAutoscaling) for op in topology
        ), f"All ops in topology must implement SupportsClusterAutoscaling, got {[type(op) for op in topology]}"

        # This is the amount of resources we can only scale up to.
        return RateBasedClusterAutoscaler(
            list(topology),
            execution_id=execution_id,
            max_cluster_limits=execution_options.resource_limits,
            utility_calculator=RollingLogicalUtilizationGauge(resource_manager),
            label_selector=execution_options.label_selector,
        )

    # TODO: `try_trigger_scaling` returns the last resource request so that we can
    # assert against it in tests, but the base `ClusterAutoscaler` expects this method
    # to return `None`. We should reconcile this later.
    def try_trigger_scaling(self):  # pyrefly: ignore[bad-override]
        # Observe and update the cluster utilization metrics.
        #
        # NOTE: We do this before checking the frequency of autoscaling requests to
        # ensure our utilization metrics get smoothly updated.
        self._utility_calculator.observe()

        # Limit the frequency of autoscaling requests.
        now = time.monotonic()
        if now - self._last_request_time < self._min_gap_between_autoscaling_requests:
            return

        # Check the cluster utilization. If it's low, re-send the previous resource
        # request.
        utilization = self._utility_calculator.get()
        self._log_cluster_utilization(utilization)
        if self._is_cluster_utilization_low(utilization):
            logger.debug("Cluster utilization is low -- skipping cluster autoscaling. ")
            self._send_resource_request(self._last_resource_request)
            return self._last_resource_request

        # Get the current resources allocated to the cluster. This requires an RPC
        # so we call it once here and reuse it for all the computations.
        current_resources = self.get_total_resources()
        max_resources_after_scaling = self._compute_max_resources_after_scaling(
            current_resources
        )

        # 1. Compute the desired number of bundles (tasks or actors) for each
        # operator based on throughput rates.
        bundle_counts: Dict[SupportsClusterAutoscaling, int] = {}
        throughput_rates = self._compute_throughput_rates()
        if throughput_rates:
            resource_requirements = self._compute_resource_requirements()
            concurrency_limits = self._compute_concurrency_limits()

            current_throughput = compute_optimal_throughput(
                rates=throughput_rates,
                resource_requirements=resource_requirements,
                resource_limits=current_resources,
                concurrency_limits=concurrency_limits,
            )

            desired_throughput = self._compute_desired_throughput(
                current_throughput=current_throughput,
                max_resources_after_scaling=max_resources_after_scaling,
                throughput_rates=throughput_rates,
                resource_requirements=resource_requirements,
                concurrency_limits=concurrency_limits,
            )
            self._log_desired_throughput(
                current_throughput=current_throughput,
                desired_throughput=desired_throughput,
                throughput_rates=throughput_rates,
                resource_requirements=resource_requirements,
                concurrency_limits=concurrency_limits,
            )

            bundle_counts = self._compute_bundle_counts(
                desired_throughput=desired_throughput,
                throughput_rates=throughput_rates,
                resource_requirements=resource_requirements,
            )

        # 2. Construct the resource request for the autoscaling coordinator.
        resource_request = self._construct_resource_request(bundle_counts)

        # 3. Pad the resource request to indirectly request object store memory if
        # needed.
        if self._should_pad_resource_request_for_object_store_memory(utilization):
            logger.debug("Padding resource request for object store memory")
            self._pad_resource_request_for_object_store_memory(
                resource_request,
                current_resources=current_resources,
                max_resources_after_scaling=max_resources_after_scaling,
            )

        # 4. Send the resource request to the autoscaling coordinator.
        self._log_resource_request(resource_request)
        self._send_resource_request(resource_request)

        return resource_request

    def _log_cluster_utilization(self, utilization: ClusterUtil) -> None:
        threshold = self._cluster_scaling_up_util_threshold
        logger.debug("=== Rate-Based Autoscaler: Cluster Utilization ===")
        logger.debug(
            f"  Utilization: CPU={utilization.cpu:.2f} GPU={utilization.gpu:.2f} "
            f"Memory={utilization.memory:.2f} ObjStore={utilization.object_store_memory:.2f}"
        )
        logger.debug(
            f"  Threshold: CPU={threshold.cpu:.2f} GPU={threshold.gpu:.2f} "
            f"Memory={threshold.memory:.2f} ObjStore={threshold.object_store_memory:.2f}"
        )

    def _is_cluster_utilization_low(self, utilization: ClusterUtil) -> bool:
        # We need utilization to be high enough for GPU, CPU, Memory, or Object Store
        threshold = self._cluster_scaling_up_util_threshold
        return (
            utilization.cpu < threshold.cpu
            and utilization.gpu < threshold.gpu
            and utilization.memory < threshold.memory
            and utilization.object_store_memory < threshold.object_store_memory
        )

    def _compute_desired_throughput(
        self,
        *,
        current_throughput: float,
        max_resources_after_scaling: ExecutionResources,
        throughput_rates: Dict[SupportsClusterAutoscaling, float],
        resource_requirements: Dict[SupportsClusterAutoscaling, ExecutionResources],
        concurrency_limits: Dict[SupportsClusterAutoscaling, int | None],
    ) -> float:
        # Compute the maximum throughput after scaling up while respecting the scaling
        # constraints.
        max_throughput_after_scaling = compute_optimal_throughput(
            rates=throughput_rates,
            resource_requirements=resource_requirements,
            resource_limits=max_resources_after_scaling,
            concurrency_limits=concurrency_limits,
        )
        # The desired throughput is 2x the current throughput, capped by the specified
        # scaling constraints.
        scaled_current_throughput = current_throughput * self._cluster_scaling_up_factor
        desired_throughput = min(
            scaled_current_throughput, max_throughput_after_scaling
        )

        return desired_throughput

    def _log_desired_throughput(
        self,
        *,
        current_throughput: float,
        desired_throughput: float,
        throughput_rates: Dict[SupportsClusterAutoscaling, float],
        resource_requirements: Dict[SupportsClusterAutoscaling, ExecutionResources],
        concurrency_limits: Dict[SupportsClusterAutoscaling, int | None],
    ) -> None:
        logger.debug("=== Rate-Based Autoscaler: Desired Throughput ===")
        logger.debug(
            f"  Current: {current_throughput:.2f} normalized blocks/s, "
            f"desired: {desired_throughput:.2f} normalized blocks/s"
        )
        logger.debug("  Per-operator inputs:")
        for op, rate in throughput_rates.items():
            logger.debug(
                f"    {op}: rate={rate:.2f} normalized blocks/s, "
                f"resources={resource_requirements[op]}, "
                f"concurrency_limit={concurrency_limits.get(op)}"
            )

    def _compute_max_resources_after_scaling(
        self, current_resources: ExecutionResources
    ) -> ExecutionResources:
        return current_resources.add(self._cluster_scaling_up_max_resource_delta).min(
            self._max_cluster_limits
        )

    def _compute_bundle_counts(
        self,
        *,
        desired_throughput: float,
        throughput_rates: Dict[SupportsClusterAutoscaling, float],
        resource_requirements: Dict[SupportsClusterAutoscaling, ExecutionResources],
    ) -> Dict[SupportsClusterAutoscaling, int]:
        allocations = allocate_resources(
            desired_throughput,
            rates=throughput_rates,
            resource_requirements=resource_requirements,
        )

        bundle_counts = {}
        for op in self._non_shuffle_ops:
            min_scheduling_resources_excl_object_store = (
                op.min_scheduling_resources().copy(object_store_memory=0)
            )

            if min_scheduling_resources_excl_object_store.is_zero():
                bundle_count = 0

            elif op in allocations:
                bundle_count = get_max_task_capacity(
                    allocations[op], min_scheduling_resources_excl_object_store
                )

                if math.isinf(bundle_count):
                    bundle_count = 0
                else:
                    # Ensure there's at least one bundle. Currently, `allocate_resources`
                    # assumes you can launch fractional tasks. So, when we call
                    # `get_max_task_capacity`, the capacity can be floored to 0.
                    bundle_count = max(math.ceil(bundle_count), 1)

            # If there isn't an allocation for the operator (e.g., if we don't have a
            # rate yet), default to one bundle to ensure liveness.
            elif not op.has_completed():
                bundle_count = 1

            else:
                bundle_count = 0

            bundle_counts[op] = bundle_count

        assert set(self._non_shuffle_ops) <= set(bundle_counts.keys()), (
            f"All non-shuffle ops must have bundle counts, missing: "
            f"{set(self._non_shuffle_ops) - set(bundle_counts.keys())}"
        )
        assert all(
            math.isfinite(count) for count in bundle_counts.values()
        ), f"All bundle counts must be finite, got {dict(bundle_counts)}"
        return bundle_counts

    def _compute_throughput_rates(self) -> Dict[SupportsClusterAutoscaling, float]:
        throughput_rates = {}
        for op in self._non_shuffle_ops:
            if op.has_completed():
                continue

            if (
                op.metrics.num_output_blocks_per_task_s is None
                # The optimal allocation for an operator with no output rate is
                # undefined, so we skip it until we have a rate.
                or op.metrics.num_output_blocks_per_task_s == 0
            ):
                continue

            throughput_rates[
                op
            ] = op.metrics.num_output_blocks_per_task_s * _get_normalization_factor(op)

        return throughput_rates

    def _compute_resource_requirements(
        self,
    ) -> Dict[SupportsClusterAutoscaling, ExecutionResources]:
        return {op: op.per_task_resource_allocation() for op in self._non_shuffle_ops}

    def _compute_concurrency_limits(
        self,
    ) -> Dict[SupportsClusterAutoscaling, int | None]:
        return {op: op.get_max_concurrency_limit() for op in self._non_shuffle_ops}

    def _construct_resource_request(
        self, bundle_counts: Dict[SupportsClusterAutoscaling, int]
    ) -> List[Dict[str, float]]:
        resource_request = []
        for op, count in bundle_counts.items():
            resource_request.extend(
                [_to_resource_bundle(op.min_scheduling_resources())] * count
            )
        return resource_request

    def _should_pad_resource_request_for_object_store_memory(
        self, utilization: ClusterUtil
    ) -> bool:
        """Return if we should try to increase object store memory.

        We scale object store memory bundles when all of the following holds true:
        1. Global object store memory utilization is high
        2. There is at least one incomplete all-to-all op in the pipeline.
        """
        has_incomplete_all_to_all = any(
            isinstance(op, AllToAllOperator) and not op.has_completed()
            for op in self._shuffle_ops
        )
        if not has_incomplete_all_to_all:
            logger.debug(
                "No incomplete all-to-all ops -- skipping object store memory padding"
            )
            return False

        # Global object store utilization
        threshold = self._cluster_scaling_up_util_threshold.object_store_memory
        if utilization.object_store_memory < threshold:
            logger.debug(
                "Global object store utilization is below threshold -- skipping object "
                "store memory padding."
            )
            return False

        return True

    def _pad_resource_request_for_object_store_memory(
        self,
        resource_request: List[Dict[str, float]],
        *,
        current_resources: ExecutionResources,
        max_resources_after_scaling: ExecutionResources,
    ) -> None:
        """Pad the resource request to implicitly request more object store memory.

        Ray doesn't let you direcly request more object store memory, so we need to
        implicity request object store memory by requesting more logical CPUs.
        """
        # Compute the number of logical CPUs in the request.
        num_cpus_in_request = 0
        for resource_dict in resource_request:
            num_cpus_in_request += resource_dict.get("CPU", 0)

        # Compute the target number of CPUs in the request. By default, this tries to
        # double the number of CPUs, capped by the scaling constraints.
        #
        # (This approach is naive. We need more information from the shuffle operators
        # to perform more accurate scaling.)
        desired_num_cpus_in_request = min(
            math.ceil(current_resources.cpu * self._cluster_scaling_up_factor),
            int(max_resources_after_scaling.cpu),
        )

        # Pad the request with the computed number of logical CPUs.
        num_cpus_to_add = max(
            0, math.ceil(desired_num_cpus_in_request - num_cpus_in_request)
        )
        resource_request.extend([{"CPU": 1.0}] * num_cpus_to_add)

    @staticmethod
    def _log_resource_request(resource_request: List[Dict[str, float]]) -> None:
        """Log the resource request with identical bundles grouped.

        This method is static so it's easier to unit test.
        """
        if not logger.isEnabledFor(logging.DEBUG):
            return

        hashable_bundles = [
            tuple(sorted(bundle.items())) for bundle in resource_request
        ]
        hashable_bundle_counts = Counter(hashable_bundles)
        formatted_bundles = [
            f"[{dict(bundle)}] * {count}"
            for bundle, count in hashable_bundle_counts.items()
        ]

        if formatted_bundles:
            logger.debug(f"Sending resource request: {', '.join(formatted_bundles)}")
        else:
            logger.debug("Sending empty resource request")

    def _send_resource_request(self, resource_request: List[Dict[str, float]]):
        self._last_resource_request = [r.copy() for r in resource_request]
        self._autoscaling_coordinator.request_resources(
            resources=[r.copy() for r in resource_request],
            expire_after_s=self._autoscaling_request_expire_time_s,
            request_remaining=True,
        )
        self._last_request_time = time.monotonic()

    def on_executor_shutdown(self):
        # Cancel the resource request when the executor is shutting down.
        try:
            self._autoscaling_coordinator.cancel_request()
        except Exception:
            msg = (
                f"Failed to cancel resource request for {self._requester_id}."
                " The request will still expire after the timeout of"
                f" {self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS_S} seconds."
            )
            logger.warning(msg, exc_info=True)

    def get_total_resources(self) -> ExecutionResources:
        resources = self._autoscaling_coordinator.get_reserved_resources()
        total = ExecutionResources.zero()
        for res in resources:
            total = total.add(ExecutionResources.from_resource_dict(res))
        return total


def _get_normalization_factor(op: SupportsClusterAutoscaling) -> float:
    """Calculate the normalization factor for an operator.

    To compare different operators, which might consume and produce different counts
    of blocks, this method calculates a normalization factor. This is used to
    convert every operator's rate into a single value that represents the number of
    sink operator outputs per second.

    Example:

        Consider a pipeline: A -> B -> C (sink)

        - Operator A: produces 2 outputs per 1 input (ratio = 2.0)
        - Operator B: produces 3 outputs per 2 inputs (ratio = 1.5)
        - Operator C: produces 1 output per 1 input (ratio = 1.0)

        For operator A: normalization_factor = 2.0 * 1.5 * 1.0 = 3.0
        For operator B: normalization_factor = 1.5 * 1.0 = 1.5
        For operator C: normalization_factor = 1.0

        This means:
        - If A produces 10 blocks/sec, it contributes 10 * 3.0 = 30 sink outputs/sec
        - If B produces 20 blocks/sec, it contributes 20 * 1.5 = 30 sink outputs/sec
        - If C produces 30 blocks/sec, it contributes 30 * 1.0 = 30 sink outputs/sec

        All operators now have comparable productivity metrics in terms of
        final sink outputs per second.

    Args:
        op: The operator to calculate the normalization factor for.

    Returns:
        The normalization factor.
    """
    if not op.output_dependencies:
        return 1

    # NOTE: This will recompute values if you call this method with operators in the
    # same path. The logic is much simpler this way, and the number of operators is
    # small, so we accept the extra work instead of doing a single-pass version.
    factor = 1
    while op.output_dependencies:
        assert len(op.output_dependencies) == 1, (
            f"Expected exactly 1 output dependency for {op}, "
            f"got {len(op.output_dependencies)}"
        )
        op = op.output_dependencies[0]

        if (
            op.metrics.average_num_outputs_per_task is None
            or op.metrics.average_num_outputs_per_task == 0
            or op.metrics.average_num_inputs_per_task is None
            or op.metrics.average_num_inputs_per_task == 0
        ):
            # Skip operators with unknown or degenerate values. None means no tasks
            # have finished yet; 0 means tasks ran but produced or consumed no data
            # (e.g. a task that filters rows might produce 0 output blocks).
            continue

        factor *= (
            op.metrics.average_num_outputs_per_task
            / op.metrics.average_num_inputs_per_task
        )

    return factor
