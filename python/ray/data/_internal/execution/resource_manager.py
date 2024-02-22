import copy
import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional

import ray
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.util import memory_string
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology


class ResourceManager:
    """A class that manages the resource usage of a streaming executor."""

    # The interval in seconds at which the global resource limits are refreshed.
    GLOBAL_LIMITS_UPDATE_INTERVAL_S = 10

    # The fraction of the object store capacity that will be used as the default object
    # store memory limit for the streaming executor.
    DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION = 0.5

    # Memory accounting is accurate only for these operators.
    # We'll enable memory reservation if a dataset only contains these operators.
    _ACCURRATE_MEMORY_ACCOUNTING_OPS = (
        InputDataBuffer,
        MapOperator,
        LimitOperator,
        OutputSplitter,
    )

    def __init__(self, topology: "Topology", options: ExecutionOptions):
        self._topology = topology
        self._options = options
        self._global_limits = ExecutionResources.zero()
        self._global_limits_last_update_time = 0
        self._global_usage = ExecutionResources.zero()
        self._op_usages: Dict[PhysicalOperator, ExecutionResources] = {}
        # Object store memory usage of the pending task outputs.
        self._mem_pending_task_outputs: Dict[PhysicalOperator, int] = defaultdict(int)
        # Object store memory usage of the internal and external output buffers.
        self._mem_output_buffers: Dict[PhysicalOperator, int] = defaultdict(int)
        # Object store memory usage of the input buffers of the output dependency
        # operators.
        self._mem_next_op_input_buffers: Dict[PhysicalOperator, int] = defaultdict(int)
        # Whether to print debug information.
        self._debug = os.environ.get("RAY_DATA_DEBUG_RESOURCE_MANAGER", "0") == "1"

        self._downstream_fraction: Dict[PhysicalOperator, float] = {}
        self._downstream_object_store_memory: Dict[PhysicalOperator, int] = {}

        self._op_resource_alloator: Optional["OpResourceAllocator"] = None
        ctx = DataContext.get_current()

        if ctx.op_resource_reservation_enabled:
            should_enable = True
            for op in topology:
                if not isinstance(op, ResourceManager._ACCURRATE_MEMORY_ACCOUNTING_OPS):
                    should_enable = False
                    break
            if should_enable:
                self._op_resource_alloator = ReservationOpResourceAllocator(
                    self, ctx.op_resource_reservation_ratio
                )

    def _estimate_object_store_memory(self, op, state) -> int:
        # Don't count input refs towards dynamic memory usage, as they have been
        # pre-created already outside this execution.
        if isinstance(op, InputDataBuffer):
            return 0

        pending_task_outputs = op.metrics.obj_store_mem_pending_task_outputs or 0

        output_buffers = op.metrics.obj_store_mem_internal_outqueue
        output_buffers += state.outqueue_memory_usage()

        next_op_input_buffers = 0
        for next_op in op.output_dependencies:
            next_op_input_buffers += (
                next_op.metrics.obj_store_mem_internal_inqueue
                + next_op.metrics.obj_store_mem_pending_task_inputs
            )

        self._mem_pending_task_outputs[op] = pending_task_outputs
        self._mem_output_buffers[op] = output_buffers
        self._mem_next_op_input_buffers[op] = next_op_input_buffers

        return pending_task_outputs + output_buffers + next_op_input_buffers

    def update_usages(self):
        """Recalculate resource usages."""
        # TODO(hchen): This method will be called frequently during the execution loop.
        # And some computations are redundant. We should either remove redundant
        # computations or remove this method entirely and compute usages on demand.
        self._global_usage = ExecutionResources(0, 0, 0)
        self._op_usages.clear()
        self._downstream_fraction.clear()
        self._downstream_object_store_memory.clear()

        # Iterate from last to first operator.
        num_ops_so_far = 0
        num_ops_total = len(self._topology)
        for op, state in reversed(self._topology.items()):
            # Update `self._op_usages`.
            op_usage = op.current_processor_usage()
            assert not op_usage.object_store_memory
            op_usage.object_store_memory = self._estimate_object_store_memory(op, state)
            self._op_usages[op] = op_usage
            # Update `self._global_usage`.
            self._global_usage = self._global_usage.add(op_usage)
            # Update `self._downstream_fraction` and `_downstream_object_store_memory`.
            # Subtract one from denom to account for input buffer.
            f = (1.0 + num_ops_so_far) / max(1.0, num_ops_total - 1.0)
            num_ops_so_far += 1
            self._downstream_fraction[op] = min(1.0, f)
            self._downstream_object_store_memory[
                op
            ] = self._global_usage.object_store_memory

        if self._op_resource_alloator is not None:
            self._op_resource_alloator.update_usages()

    def get_global_usage(self) -> ExecutionResources:
        """Return the global resource usage at the current time."""
        return self._global_usage

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
        base = self._options.resource_limits
        exclude = self._options.exclude_resources
        cluster = ray.cluster_resources()

        cpu = base.cpu
        if cpu is None:
            cpu = cluster.get("CPU", 0.0) - (exclude.cpu or 0.0)
        gpu = base.gpu
        if gpu is None:
            gpu = cluster.get("GPU", 0.0) - (exclude.gpu or 0.0)
        object_store_memory = base.object_store_memory
        if object_store_memory is None:
            object_store_memory = round(
                self.DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
                * cluster.get("object_store_memory", 0.0)
            ) - (exclude.object_store_memory or 0)

        self._global_limits = ExecutionResources(
            cpu=cpu,
            gpu=gpu,
            object_store_memory=object_store_memory,
        )
        return self._global_limits

    def get_op_usage(self, op: PhysicalOperator) -> ExecutionResources:
        """Return the resource usage of the given operator at the current time."""
        return self._op_usages[op]

    def get_op_usage_str(self, op: PhysicalOperator) -> str:
        """Return a human-readable string representation of the resource usage of
        the given operator."""
        usage_str = f"[cpu: {self._op_usages[op].cpu:.1f}"
        if self._op_usages[op].gpu:
            usage_str += f", gpu: {self._op_usages[op].gpu:.1f}"
        usage_str += f", objects: {self._op_usages[op].object_store_memory_str()}"
        if self._debug:
            usage_str += (
                f" (PendingTaskOutputs: {memory_string(self._mem_pending_task_outputs[op])}, "
                f"OutBuffers: {memory_string(self._mem_output_buffers[op])}, "
                f"NextOpInBuffers: {memory_string(self._mem_next_op_input_buffers[op])})"
            )
        usage_str += "]"
        return usage_str

    def get_downstream_fraction(self, op: PhysicalOperator) -> float:
        """Return the downstream fraction of the given operator."""
        return self._downstream_fraction[op]

    def get_downstream_object_store_memory(self, op: PhysicalOperator) -> int:
        """Return the downstream object store memory usage of the given operator."""
        return self._downstream_object_store_memory[op]

    def op_resource_allocator_enabled(self) -> bool:
        """Return whether OpResourceAllocator is enabled."""
        return self._op_resource_alloator is not None

    @property
    def op_resource_allocator(self) -> "OpResourceAllocator":
        """Return the OpResourceAllocator."""
        assert self._op_resource_alloator is not None
        return self._op_resource_alloator


class OpResourceAllocator(ABC):
    """An interface for dynamic operator resource allocation.

    This interface allows dynamically allocate available resources to each operator,
    limiting how many tasks each operator can submit, and how much data each operator
    can read from its running tasks.
    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager = resource_manager

    @abstractmethod
    def update_usages(self) -> ExecutionResources:
        """Callback to update resource usages."""
        ...

    @abstractmethod
    def can_submit_new_task(self, op: PhysicalOperator) -> bool:
        """Return whether the given operator can submit a new task."""
        ...

    @abstractmethod
    def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
        """Return the maximum bytes of outputs that can be read from
        the given operator's running tasks. None means no limit."""
        ...


class ReservationOpResourceAllocator(OpResourceAllocator):
    """An OpResourceAllocator implementation that reserves resources for each operator.

    This class reserves memory and CPU resources for map operators, and consider runtime
    resource usages to limit the resources that each operator can use.

    It works in the following way:
    1. Currently we only limit map operators. Non-map operators get unlimited resources.
    2. For each map operator, we reserve `reservation_ratio * global_resources /
        num_map_ops` resources, half of which is reserved only for the operator outputs,
        excluding pending task outputs.
    3. Non-reserved resources are shared among all operators.
    3. In each scheduling iteration, each map operator will get "remaining of their own
       reserved resources" + "remaining of shared resources / num_map_ops" resources.

    The `reservation_ratio` is set to 50% by default. Users can tune this value to
    adjust how aggressive or conservative the resource allocation is. A higher value
    will make the resource allocation more even, but may lead to underutilization and
    worse performance. And vice versa.
    """

    def __init__(self, resource_manager: ResourceManager, reservation_ratio: float):
        super().__init__(resource_manager)
        self._reservation_ratio = reservation_ratio
        assert 0.0 <= self._reservation_ratio <= 1.0
        # Per-op reserved resources.
        self._op_reserved: Dict[PhysicalOperator, ExecutionResources] = {}
        # Memory reserved for the outputs of each operator. This includes the operator's
        # internal and external output buffers and next operator's input buffers, but
        # doesn't include the pending task outputs.
        # Note, if we don't reserve memory for outputs, all the budget may be used by
        # by the pending task outputs. Then we'll have no budget to pull the outputs
        # from the running tasks.
        self._reserved_for_op_outputs: Dict[PhysicalOperator, int] = {}
        # Total shared resources.
        self._total_shared = ExecutionResources.zero()
        # Resource budgets for each operator.
        self._op_budgets: Dict[PhysicalOperator, ExecutionResources] = {}
        self._cached_global_limits = ExecutionResources.zero()

    def _get_eligible_ops(self) -> List[PhysicalOperator]:
        # Only consider map operators that are not completed.
        return [
            op
            for op in self._resource_manager._topology
            if isinstance(op, MapOperator) and not op.completed()
        ]

    def _on_global_limits_updated(self, global_limits: ExecutionResources):
        from ray.data._internal.execution.operators.actor_pool_map_operator import (
            ActorPoolMapOperator,
        )

        eligible_ops = self._get_eligible_ops()
        if len(eligible_ops) == 0:
            return

        self._total_shared = copy.deepcopy(global_limits)

        # Reserve `reservation_ratio * global_limits / num_ops` resources for each
        # operator.
        default_reserved = global_limits.scale(
            self._reservation_ratio / len(eligible_ops)
        )
        for op in eligible_ops:
            # Reserve at least half of the default reserved resources for the outputs.
            # This makes sure that we will have enough budget to pull the outputs from
            # the running tasks.
            self._reserved_for_op_outputs[op] = max(
                default_reserved.object_store_memory // 2, 1
            )
            # Adjust the reserved resources in some special cases.
            # 1. Make sure the reserved resources are at least to allow one task.
            # TODO: do not consider autoscaling.
            min_reserved = op.incremental_resource_usage()
            # 2. To ensure that all GPUs are utilized, reserve enough object store memory
            # to launch one task for each worker.
            if (
                isinstance(op, ActorPoolMapOperator)
                and op.base_resource_usage().gpu > 0
            ):
                min_reserved.object_store_memory *= op._autoscaling_policy.min_workers

            min_reserved.object_store_memory += self._reserved_for_op_outputs[op]
            self._op_reserved[op] = default_reserved.max(min_reserved)
            self._total_shared = self._total_shared.subtract(self._op_reserved[op])
        self._total_shared = self._total_shared.max(ExecutionResources.zero())

    def can_submit_new_task(self, op: PhysicalOperator) -> bool:
        if op not in self._op_budgets:
            return True
        budget = copy.deepcopy(self._op_budgets[op])
        # Exclude the reserved memory for outputs.
        outputs_usage = (
            self._resource_manager._mem_output_buffers[op]
            + self._resource_manager._mem_next_op_input_buffers[op]
        )
        outputs_reamining_reserved = max(
            self._reserved_for_op_outputs[op] - outputs_usage, 0
        )
        budget.object_store_memory -= outputs_reamining_reserved
        res = op.incremental_resource_usage().satisfies_limit(budget)
        return res

    def max_task_output_bytes_to_read(self, op: PhysicalOperator) -> Optional[int]:
        if op not in self._op_budgets:
            return None
        return self._op_budgets[op].object_store_memory

    def update_usages(self):
        eligible_ops = self._get_eligible_ops()
        if len(eligible_ops) == 0:
            return

        global_limits = self._resource_manager.get_global_limits()
        if global_limits != self._cached_global_limits:
            self._on_global_limits_updated(global_limits)
            self._cached_global_limits = global_limits

        self._op_budgets.clear()
        # Remaining of shared resources.
        remaining_shared = self._total_shared
        for op in self._resource_manager._topology:
            op_usage = self._resource_manager.get_op_usage(op)
            if op in eligible_ops:
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
            else:
                # For non-eligible ops, we still need to subtract
                # their usage from the remaining shared resources.
                remaining_shared = remaining_shared.subtract(op_usage)

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
            if not to_borrow.is_zero() and op_shared.add(to_borrow).satisfies_limit(remaining_shared):
                op_shared = op_shared.add(to_borrow)
            remaining_shared = remaining_shared.subtract(op_shared)
            assert remaining_shared.non_negative(), (remaining_shared, op, op_shared, to_borrow)
            self._op_budgets[op] = self._op_budgets[op].add(op_shared)
            # We don't limit GPU resources, as not all operators
            # use GPU resources.
            self._op_budgets[op].gpu = float("inf")

