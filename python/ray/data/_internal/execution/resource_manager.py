import logging
import math
import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional

from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.execution.util import memory_string
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import OpState
    from ray.data._internal.execution.streaming_executor_state import Topology


logger = logging.getLogger(__name__)
DEBUG_RESOURCE_MANAGER = os.environ.get("RAY_DATA_DEBUG_RESOURCE_MANAGER", "0") == "1"

# These are physical operators that must receive all inputs before they start
# processing data.
MATERIALIZING_OPERATORS = (AllToAllOperator, ZipOperator)


class ResourceManager:
    """A class that manages the resource usage of a streaming executor."""

    # The interval in seconds at which the global resource limits are refreshed.
    GLOBAL_LIMITS_UPDATE_INTERVAL_S = 10

    # The fraction of the object store capacity that will be used as the default object
    # store memory limit for the streaming executor,
    # when `ReservationOpResourceAllocator` is enabled.
    DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION = 0.5

    # The fraction of the object store capacity that will be used as the default object
    # store memory limit for the streaming executor,
    # when `ReservationOpResourceAllocator` is not enabled.
    DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION_NO_RESERVATION = 0.25

    def __init__(
        self,
        topology: "Topology",
        options: ExecutionOptions,
        get_total_resources: Callable[[], ExecutionResources],
        data_context: DataContext,
    ):
        self._topology = topology
        self._options = options
        self._get_total_resources = get_total_resources
        self._global_limits = ExecutionResources.zero()
        self._global_limits_last_update_time = 0
        self._global_usage = ExecutionResources.zero()
        self._global_running_usage = ExecutionResources.zero()
        self._global_pending_usage = ExecutionResources.zero()
        self._op_usages: Dict[PhysicalOperator, ExecutionResources] = {}
        self._op_running_usages: Dict[PhysicalOperator, ExecutionResources] = {}
        self._op_pending_usages: Dict[PhysicalOperator, ExecutionResources] = {}
        # Object store memory usage internal to the operator, including the
        # pending task outputs and op's internal output buffers.
        self._mem_op_internal: Dict[PhysicalOperator, int] = defaultdict(int)
        # Object store memory usage of the blocks that have been taken out of
        # the operator, including the external output buffer in OpState, and the
        # input buffers of the downstream operators.
        self._mem_op_outputs: Dict[PhysicalOperator, int] = defaultdict(int)
        # Whether to print debug information.
        self._debug = DEBUG_RESOURCE_MANAGER

        self._downstream_fraction: Dict[PhysicalOperator, float] = {}
        self._downstream_object_store_memory: Dict[PhysicalOperator, float] = {}

        self._op_resource_allocator: Optional["OpResourceAllocator"] = None

        if data_context.op_resource_reservation_enabled:
            # We'll enable memory reservation if all operators have
            # implemented accurate memory accounting.
            should_enable = all(
                op.implements_accurate_memory_accounting() for op in topology
            )
            if should_enable:
                self._op_resource_allocator = ReservationOpResourceAllocator(
                    self, data_context.op_resource_reservation_ratio
                )

        self._object_store_memory_limit_fraction = (
            data_context.override_object_store_memory_limit_fraction
            if data_context.override_object_store_memory_limit_fraction is not None
            else (
                self.DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
                if self.op_resource_allocator_enabled()
                else self.DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION_NO_RESERVATION
            )
        )

    def _estimate_object_store_memory(
        self, op: "PhysicalOperator", state: "OpState"
    ) -> int:
        # Don't count input refs towards dynamic memory usage, as they have been
        # pre-created already outside this execution.
        if isinstance(op, InputDataBuffer):
            return 0

        # Pending task outputs.
        mem_op_internal = op.metrics.obj_store_mem_pending_task_outputs or 0
        # Op's internal output buffers.
        mem_op_internal += op.metrics.obj_store_mem_internal_outqueue

        # Op's external output buffer.
        mem_op_outputs = state.outqueue_memory_usage()
        # Input buffers of the downstream operators.
        for next_op in op.output_dependencies:
            mem_op_outputs += (
                next_op.metrics.obj_store_mem_internal_inqueue
                + next_op.metrics.obj_store_mem_pending_task_inputs
            )

        self._mem_op_internal[op] = mem_op_internal
        self._mem_op_outputs[op] = mem_op_outputs

        return mem_op_internal + mem_op_outputs

    def update_usages(self):
        """Recalculate resource usages."""
        # TODO(hchen): This method will be called frequently during the execution loop.
        # And some computations are redundant. We should either remove redundant
        # computations or remove this method entirely and compute usages on demand.
        self._global_usage = ExecutionResources(0, 0, 0)
        self._global_running_usage = ExecutionResources(0, 0, 0)
        self._global_pending_usage = ExecutionResources(0, 0, 0)
        self._op_usages.clear()
        self._op_running_usages.clear()
        self._op_pending_usages.clear()
        self._downstream_fraction.clear()
        self._downstream_object_store_memory.clear()

        # Iterate from last to first operator.
        num_ops_so_far = 0
        num_ops_total = len(self._topology)
        for op, state in reversed(self._topology.items()):
            # Update `self._op_usages`, `self._op_running_usages`,
            # and `self._op_pending_usages`.
            op.update_resource_usage()
            op_usage = op.current_processor_usage()
            op_running_usage = op.running_processor_usage()
            op_pending_usage = op.pending_processor_usage()

            assert not op_usage.object_store_memory
            assert not op_running_usage.object_store_memory
            assert not op_pending_usage.object_store_memory
            op_usage.object_store_memory = self._estimate_object_store_memory(op, state)
            op_running_usage.object_store_memory = self._estimate_object_store_memory(
                op, state
            )
            self._op_usages[op] = op_usage
            self._op_running_usages[op] = op_running_usage
            self._op_pending_usages[op] = op_pending_usage

            # Update `self._global_usage`, `self._global_running_usage`,
            # and `self._global_pending_usage`.
            self._global_usage = self._global_usage.add(op_usage)
            self._global_running_usage = self._global_running_usage.add(
                op_running_usage
            )
            self._global_pending_usage = self._global_pending_usage.add(
                op_pending_usage
            )

            # Update `self._downstream_fraction` and `_downstream_object_store_memory`.
            # Subtract one from denom to account for input buffer.
            f = (1.0 + num_ops_so_far) / max(1.0, num_ops_total - 1.0)
            num_ops_so_far += 1
            self._downstream_fraction[op] = min(1.0, f)
            self._downstream_object_store_memory[
                op
            ] = self._global_usage.object_store_memory

            # Update operator's object store usage, which is used by
            # DatasetStats and updated on the Ray Data dashboard.
            op._metrics.obj_store_mem_used = op_usage.object_store_memory

        if self._op_resource_allocator is not None:
            self._op_resource_allocator.update_usages()

    def get_global_usage(self) -> ExecutionResources:
        """Return the global resource usage at the current time."""
        return self._global_usage

    def get_global_running_usage(self) -> ExecutionResources:
        """Return the global running resource usage at the current time."""
        return self._global_running_usage

    def get_global_pending_usage(self) -> ExecutionResources:
        """Return the global pending resource usage at the current time."""
        return self._global_pending_usage

    def get_global_limits(self) -> ExecutionResources:
        """Return the global resource limits at the current time.

        This method autodetects any unspecified execution resource limits based on the
        current cluster size, refreshing these values periodically to support cluster
        autoscaling.
        """
        if (
            time.time() - self._global_limits_last_update_time
            < self.GLOBAL_LIMITS_UPDATE_INTERVAL_S
        ):
            return self._global_limits

        self._global_limits_last_update_time = time.time()
        default_limits = self._options.resource_limits
        exclude = self._options.exclude_resources
        total_resources = self._get_total_resources()
        default_mem_fraction = self._object_store_memory_limit_fraction
        total_resources.object_store_memory *= default_mem_fraction
        self._global_limits = default_limits.min(total_resources).subtract(exclude)
        return self._global_limits

    def get_op_usage(self, op: PhysicalOperator) -> ExecutionResources:
        """Return the resource usage of the given operator at the current time."""
        return self._op_usages[op]

    def get_op_usage_str(self, op: PhysicalOperator) -> str:
        """Return a human-readable string representation of the resource usage of
        the given operator."""
        usage_str = f"{self._op_running_usages[op].cpu:.1f} CPU"
        if self._op_running_usages[op].gpu:
            usage_str += f", {self._op_running_usages[op].gpu:.1f} GPU"
        usage_str += (
            f", {self._op_running_usages[op].object_store_memory_str()} object store"
        )
        if self._debug:
            usage_str += (
                f" (in={memory_string(self._mem_op_internal[op])},"
                f"out={memory_string(self._mem_op_outputs[op])})"
            )
            if (
                isinstance(self._op_resource_allocator, ReservationOpResourceAllocator)
                and op in self._op_resource_allocator._op_budgets
            ):
                budget = self._op_resource_allocator._op_budgets[op]
                usage_str += f", budget=(cpu={budget.cpu:.1f}"
                usage_str += f",gpu={budget.gpu:.1f}"
                usage_str += f",object store={budget.object_store_memory_str()})"
        return usage_str

    def op_resource_allocator_enabled(self) -> bool:
        """Return whether OpResourceAllocator is enabled."""
        return self._op_resource_allocator is not None

    @property
    def op_resource_allocator(self) -> "OpResourceAllocator":
        """Return the OpResourceAllocator."""
        assert self._op_resource_allocator is not None
        return self._op_resource_allocator


class OpResourceAllocator(ABC):
    """An interface for dynamic operator resource allocation.

    This interface allows dynamically allocating available resources to each operator,
    limiting how many tasks each operator can submit, and how much data each operator
    can read from its running tasks.
    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager = resource_manager

    @abstractmethod
    def update_usages(self):
        """Callback to update resource usages."""
        ...

    @abstractmethod
    def can_submit_new_task(self, op: PhysicalOperator) -> bool:
        """Return whether the given operator can submit a new task."""
        ...

    @abstractmethod
    def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
        """Return the maximum bytes of pending task outputs can be read for
        the given operator. None means no limit."""
        ...

    @abstractmethod
    def get_budget(self, op: PhysicalOperator) -> ExecutionResources:
        """Return the budget for the given operator."""
        ...


class ReservationOpResourceAllocator(OpResourceAllocator):
    """An OpResourceAllocator implementation that reserves resources for each operator.

    This class reserves memory and CPU resources for eligible operators, and considers
    runtime resource usages to limit the resources that each operator can use.

    It works in the following way:
    1. An operator is eligible for resource reservation, if it has enabled throttling
       and hasn't completed. Ineligible operators are not throttled, but
       their usage will be accounted for their upstream eligible operators. E.g., for
       such a dataset "map1->limit->map2->streaming_split", we'll treat "map1->limit" as
       a group and "map2->streaming_split" as another group.
    2. For each eligible operator, we reserve `reservation_ratio * global_resources /
        num_eligible_ops` resources, half of which is reserved only for the operator
        outputs, excluding pending task outputs.
    3. Non-reserved resources are shared among all operators.
    4. In each scheduling iteration, each eligible operator will get "remaining of their
       own reserved resources" + "remaining of shared resources / num_eligible_ops"
       resources.

    The `reservation_ratio` is set to 50% by default. Users can tune this value to
    adjust how aggressive or conservative the resource allocation is. A higher value
    will make the resource allocation more even, but may lead to underutilization and
    worse performance. And vice versa.
    """

    class IdleDetector:
        """Utility class for detecting idle operators.

        Note, stalling can happen when there are less resources than Data executor
        expects. E.g., when some resources are preempted by non-Data code, see
        `test_no_deadlock_on_resource_contention` as an example.

        This class is used to detect potential stalling and allow the execution
        to make progress.
        """

        # The interval to detect idle operators.
        # When downstream is idle, we'll allow reading at least one task output
        # per this interval,
        DETECTION_INTERVAL_S = 10.0
        # Print a warning if an operator is idle for this time.
        WARN_ON_IDLE_TIME_S = 60.0
        # Whether a warning has been printed.
        _warn_printed = False

        def __init__(self):
            # per-op fields
            self.last_num_outputs = defaultdict(int)
            self.last_output_time = defaultdict(lambda: time.time())
            self.last_detection_time = defaultdict(lambda: time.time())

        def detect_idle(self, op: PhysicalOperator):
            cur_time = time.time()
            if cur_time - self.last_detection_time[op] > self.DETECTION_INTERVAL_S:
                cur_num_outputs = op.metrics.num_task_outputs_generated
                if cur_num_outputs > self.last_num_outputs[op]:
                    self.last_num_outputs[op] = cur_num_outputs
                    self.last_output_time[op] = cur_time
                    self.last_detection_time[op] = cur_time
                else:
                    self.last_detection_time[op] = cur_time
                    self.print_warning_if_idle_for_too_long(
                        op, cur_time - self.last_output_time[op]
                    )
                    return True
            return False

        @classmethod
        def print_warning_if_idle_for_too_long(
            cls, op: PhysicalOperator, idle_time: float
        ):
            """Print a warning if an operator is idle for too long."""
            if idle_time < cls.WARN_ON_IDLE_TIME_S or cls._warn_printed:
                return
            cls._warn_printed = True
            msg = (
                f"Operator {op} is running but has no outputs for {idle_time} seconds."
                " Execution may be slower than expected.\n"
                "Ignore this warning if your UDF is expected to be slow."
                " Otherwise, this can happen when there are fewer cluster resources"
                " available to Ray Data than expected."
                " If you have non-Data tasks or actors running in the cluster, exclude"
                " their resources from Ray Data with"
                " `DataContext.get_current().execution_options.exclude_resources`."
                " This message will only print once."
            )
            logger.warning(msg)

    def __init__(self, resource_manager: ResourceManager, reservation_ratio: float):
        super().__init__(resource_manager)
        self._reservation_ratio = reservation_ratio
        assert 0.0 <= self._reservation_ratio <= 1.0
        # Per-op reserved resources, excluding `_reserved_for_op_outputs`.
        self._op_reserved: Dict[PhysicalOperator, ExecutionResources] = {}
        # Memory reserved exclusively for the outputs of each operator.
        # "Op outputs" refer to blocks that have been taken out of an operator,
        # i.e., `RessourceManager._mem_op_outputs`.
        #
        # Note, if we don't reserve memory for op outputs, all the budget may be used by
        # the pending task outputs, and/or op's internal output buffers (the latter can
        # happen when `preserve_order=True`).
        # Then we'll have no budget to pull blocks from the op.
        self._reserved_for_op_outputs: Dict[PhysicalOperator, float] = {}
        # Total shared resources.
        self._total_shared = ExecutionResources.zero()
        # Resource budgets for each operator, excluding `_reserved_for_op_outputs`.
        self._op_budgets: Dict[PhysicalOperator, ExecutionResources] = {}
        # Whether each operator has reserved the minimum resources to run
        # at least one task.
        # This is used to avoid edge cases where the entire resource limits are not
        # enough to run one task of each op.
        # See `test_no_deadlock_on_small_cluster_resources` as an example.
        self._reserved_min_resources: Dict[PhysicalOperator, bool] = {}

        self._cached_global_limits = ExecutionResources.zero()
        self._cached_num_eligible_ops = 0

        self._idle_detector = self.IdleDetector()

    def _is_op_eligible(self, op: PhysicalOperator) -> bool:
        """Whether the op is eligible for memory reservation."""
        return not op.throttling_disabled() and not op.completed()

    def _get_eligible_ops(self) -> List[PhysicalOperator]:
        return [
            op for op in self._resource_manager._topology if self._is_op_eligible(op)
        ]

    def _update_reservation(self):
        global_limits = self._resource_manager.get_global_limits()
        eligible_ops = self._get_eligible_ops()

        if (
            global_limits == self._cached_global_limits
            and len(eligible_ops) == self._cached_num_eligible_ops
        ):
            return
        self._cached_global_limits = global_limits
        self._cached_num_eligible_ops = len(eligible_ops)

        self._op_reserved.clear()
        self._reserved_for_op_outputs.clear()
        self._reserved_min_resources.clear()
        self._total_shared = global_limits.copy()

        if len(eligible_ops) == 0:
            return

        # Reserve `reservation_ratio * global_limits / num_ops` resources for each
        # operator.
        default_reserved = global_limits.scale(
            self._reservation_ratio / (len(eligible_ops))
        )
        for op in eligible_ops:
            # Reserve at least half of the default reserved resources for the outputs.
            # This makes sure that we will have enough budget to pull blocks from the
            # op.
            self._reserved_for_op_outputs[op] = max(
                default_reserved.object_store_memory / 2, 1.0
            )
            # Calculate the minimum amount of resources to reserve.
            # 1. Make sure the reserved resources are at least to allow one task.
            min_reserved = op.incremental_resource_usage().copy()
            # 2. To ensure that all GPUs are utilized, reserve enough resource budget
            # to launch one task for each worker.
            if op.base_resource_usage().gpu > 0:
                min_workers = sum(
                    pool.min_size() for pool in op.get_autoscaling_actor_pools()
                )
                min_reserved.object_store_memory *= min_workers
            # Also include `reserved_for_op_outputs`.
            min_reserved.object_store_memory += self._reserved_for_op_outputs[op]
            # Total resources we want to reserve for this operator.
            op_total_reserved = default_reserved.max(min_reserved)
            if op_total_reserved.satisfies_limit(self._total_shared):
                # If the remaining resources are enough to reserve `op_total_reserved`,
                # subtract it from `self._total_shared` and reserve it for this op.
                self._reserved_min_resources[op] = True
                self._total_shared = self._total_shared.subtract(op_total_reserved)
                self._op_reserved[op] = op_total_reserved
                self._op_reserved[
                    op
                ].object_store_memory -= self._reserved_for_op_outputs[op]
            else:
                # If the remaining resources are not enough to reserve the minimum
                # resources for this operator, we'll only reserve the minimum object
                # store memory, but not the CPU and GPU resources.
                # Because Ray Core doesn't allow CPU/GPU resources to be oversubscribed.
                # Note, we reserve minimum resources first for the upstream
                # ops. Downstream ops need to wait for upstream ops to finish
                # and release resources.
                self._reserved_min_resources[op] = False
                self._op_reserved[op] = ExecutionResources(
                    0,
                    0,
                    min_reserved.object_store_memory
                    - self._reserved_for_op_outputs[op],
                )
                self._total_shared = self._total_shared.subtract(
                    ExecutionResources(0, 0, min_reserved.object_store_memory)
                )

            self._total_shared = self._total_shared.max(ExecutionResources.zero())

    def can_submit_new_task(self, op: PhysicalOperator) -> bool:
        if op not in self._op_budgets:
            return True
        budget = self._op_budgets[op]
        res = op.incremental_resource_usage().satisfies_limit(budget)
        return res

    def get_budget(self, op: PhysicalOperator) -> ExecutionResources:
        return self._op_budgets[op]

    def _should_unblock_streaming_output_backpressure(
        self, op: PhysicalOperator
    ) -> bool:
        # In some edge cases, the downstream operators may have no enough resources to
        # launch tasks. Then we should temporarily unblock the streaming output
        # backpressure by allowing reading at least 1 block. So the current operator
        # can finish at least one task and yield resources to the downstream operators.
        for next_op in self._get_downstream_eligible_ops(op):
            if not self._reserved_min_resources[next_op]:
                # Case 1: the downstream operator hasn't reserved the minimum resources
                # to run at least one task.
                return True
            # Case 2: the downstream operator has reserved the minimum resources, but
            # the resources are preempted by non-Data tasks or actors.
            # We don't have a good way to detect this case, so we'll unblock
            # backpressure when the downstream operator has been idle for a while.
            if self._idle_detector.detect_idle(next_op):
                return True
        return False

    def _get_op_outputs_usage_with_downstream(self, op: PhysicalOperator) -> float:
        """Get the outputs memory usage of the given operator, including the downstream
        ineligible operators.
        """
        # Outputs usage of the current operator.
        op_outputs_usage = self._resource_manager._mem_op_outputs[op]
        # Also account the downstream ineligible operators' memory usage.
        op_outputs_usage += sum(
            self._resource_manager.get_op_usage(next_op).object_store_memory
            for next_op in self._get_downstream_ineligible_ops(op)
        )
        return op_outputs_usage

    def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
        if op not in self._op_budgets:
            return None
        res = self._op_budgets[op].object_store_memory
        # Add the remaining of `_reserved_for_op_outputs`.
        op_outputs_usage = self._get_op_outputs_usage_with_downstream(op)
        res += max(self._reserved_for_op_outputs[op] - op_outputs_usage, 0)
        if math.isinf(res):
            return None

        res = int(res)
        assert res >= 0
        if res == 0 and self._should_unblock_streaming_output_backpressure(op):
            res = 1
        return res

    def _get_downstream_ineligible_ops(
        self, op: PhysicalOperator
    ) -> Iterable[PhysicalOperator]:
        """Get the downstream ineligible operators of the given operator.

        E.g.,
          - "cur_map->downstream_map" will return an empty list.
          - "cur_map->limit1->limit2->downstream_map" will return [limit1, limit2].
        """
        for next_op in op.output_dependencies:
            if not self._is_op_eligible(next_op):
                yield next_op
                yield from self._get_downstream_ineligible_ops(next_op)

    def _get_downstream_eligible_ops(
        self, op: PhysicalOperator
    ) -> Iterable[PhysicalOperator]:
        """Get the downstream eligible operators of the given operator, ignoring
        intermediate ineligible operators.

        E.g.,
          - "cur_map->downstream_map" will return [downstream_map].
          - "cur_map->limit1->limit2->downstream_map" will return [downstream_map].
        """
        for next_op in op.output_dependencies:
            if self._is_op_eligible(next_op):
                yield next_op
            else:
                yield from self._get_downstream_eligible_ops(next_op)

    def update_usages(self):
        self._update_reservation()

        self._op_budgets.clear()
        eligible_ops = self._get_eligible_ops()
        if len(eligible_ops) == 0:
            return

        # Remaining of shared resources.
        remaining_shared = self._total_shared
        for op in eligible_ops:
            # Calculate the memory usage of the operator.
            op_mem_usage = 0
            # Add the memory usage of the operator itself,
            # excluding `_reserved_for_op_outputs`.
            op_mem_usage += self._resource_manager._mem_op_internal[op]
            # Add the portion of op outputs usage that has
            # exceeded `_reserved_for_op_outputs`.
            op_outputs_usage = self._get_op_outputs_usage_with_downstream(op)
            op_mem_usage += max(op_outputs_usage - self._reserved_for_op_outputs[op], 0)
            op_usage = self._resource_manager.get_op_usage(op).copy()
            op_usage.object_store_memory = op_mem_usage
            op_reserved = self._op_reserved[op]
            # How much of the reserved resources are remaining.
            op_reserved_remaining = op_reserved.subtract(op_usage).max(
                ExecutionResources.zero()
            )
            self._op_budgets[op] = op_reserved_remaining
            # How much of the reserved resources are exceeded.
            # If exceeded, we need to subtract from the remaining shared resources.
            op_reserved_exceeded = op_usage.subtract(op_reserved).max(
                ExecutionResources.zero()
            )
            remaining_shared = remaining_shared.subtract(op_reserved_exceeded)

        remaining_shared = remaining_shared.max(ExecutionResources.zero())

        # Allocate the remaining shared resources to each operator.
        for i, op in enumerate(reversed(eligible_ops)):
            # By default, divide the remaining shared resources equally.
            op_shared = remaining_shared.scale(1.0 / (len(eligible_ops) - i))
            # But if the op's budget is less than `incremental_resource_usage`,
            # it will be useless. So we'll let the downstream operator
            # borrow some resources from the upstream operator, if remaining_shared
            # is still enough.
            to_borrow = (
                op.incremental_resource_usage()
                .subtract(self._op_budgets[op].add(op_shared))
                .max(ExecutionResources.zero())
            )
            if not to_borrow.is_zero() and op_shared.add(to_borrow).satisfies_limit(
                remaining_shared
            ):
                op_shared = op_shared.add(to_borrow)
            remaining_shared = remaining_shared.subtract(op_shared)
            assert remaining_shared.is_non_negative(), (
                remaining_shared,
                op,
                op_shared,
                to_borrow,
            )
            self._op_budgets[op] = self._op_budgets[op].add(op_shared)
            # We don't limit GPU resources, as not all operators
            # use GPU resources.
            self._op_budgets[op].gpu = float("inf")

        # A materializing operator like `AllToAllOperator` waits for all its input
        # operator’s outputs before processing data. This often forces the input
        # operator to exceed its object store memory budget. To prevent deadlock, we
        # disable object store memory backpressure for the input operator.
        for op in eligible_ops:
            if any(
                isinstance(next_op, MATERIALIZING_OPERATORS)
                for next_op in op.output_dependencies
            ):
                self._op_budgets[op].object_store_memory = float("inf")
