import time
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Dict, Optional

import ray
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology


class ResourceManager:
    """A class that manages the resource usage of a streaming executor."""

    # The interval in seconds at which the global resource limits are refreshed.
    GLOBAL_LIMITS_UPDATE_INTERVAL_S = 10

    # The fraction of the object store capacity that will be used as the default object
    # store memory limit for the streaming executor.
    DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION = 0.25

    def __init__(self, topology: "Topology", options: ExecutionOptions):
        self._topology = topology
        self._options = options
        self._global_limits = ExecutionResources()
        self._global_limits_last_update_time = 0
        self._global_usage = ExecutionResources(0, 0, 0)
        self._op_usages: Dict[PhysicalOperator, ExecutionResources] = {}

        self._downstream_fraction: Dict[PhysicalOperator, float] = {}
        self._downstream_object_store_memory: Dict[PhysicalOperator, int] = {}

        self._op_resource_limiter: Optional["OpResourceLimiter"] = None
        if True:
            self._op_resource_limiter = ReservationOpResourceLimiter(self, 0.5)

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
            op_usage = op.current_resource_usage()
            op_usage.object_store_memory = op._metrics.obj_store_mem_outputs
            for next_op in op.output_dependencies:
                op_usage.object_store_memory += next_op._metrics.obj_store_mem_inputs
            # Don't count input refs towards dynamic memory usage, as they have been
            # pre-created already outside this execution.
            if not isinstance(op, InputDataBuffer):
                op_usage.object_store_memory = (
                    op_usage.object_store_memory or 0
                ) + state.outqueue_memory_usage()
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

        if self._op_resource_limiter is not None:
            self._op_resource_limiter.update_usages()

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

    def get_downstream_fraction(self, op: PhysicalOperator) -> float:
        """Return the downstream fraction of the given operator."""
        return self._downstream_fraction[op]

    def get_downstream_object_store_memory(self, op: PhysicalOperator) -> int:
        """Return the downstream object store memory usage of the given operator."""
        return self._downstream_object_store_memory[op]

    def per_op_limiting_enabled(self, op: PhysicalOperator) -> bool:
        """Return whether per-operator resource limiting is enabled."""
        return self._op_resource_limiter is not None

    def get_op_available(self, op: PhysicalOperator) -> ExecutionResources:
        """Return the resource limit of the given operator at the current time."""
        assert self._op_resource_limiter is not None
        return self._op_resource_limiter.get_op_available(op)


class OpResourceLimiter(metaclass=ABCMeta):
    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager = resource_manager

    @abstractmethod
    def update_usages(self, op: PhysicalOperator) -> ExecutionResources:
        ...

    @abstractmethod
    def get_op_available(self, op: PhysicalOperator) -> ExecutionResources:
        ...


class ReservationOpResourceLimiter(OpResourceLimiter):
    """A resource limiter that reserves resources for each operator."""

    def __init__(self, resource_manager: ResourceManager, reservation_ratio: float):
        super().__init__(resource_manager)
        self._reservation_ratio = reservation_ratio
        self._op_limits: Dict[PhysicalOperator, ExecutionResources] = {}
        assert 0.0 <= self._reservation_ratio <= 1.0

    def update_usages(self):
        num_ops = len(self._resource_manager._topology)
        global_limits = self._resource_manager.get_global_limits()
        # Per-op reserved resources.
        reserved = global_limits.scale(self._reservation_ratio / num_ops)
        # Total shared resources.
        shared = global_limits.scale(1.0 - self._reservation_ratio)

        self._op_limits.clear()
        for op in self._resource_manager._topology:
            op_usage = self._resource_manager.get_op_usage(op)
            op_reserved_remaining = reserved.subtract(op_usage, non_negative=True)
            op_reserved_exceeded = op_usage.subtract(reserved, non_negative=True)
            self._op_limits[op] = op_reserved_remaining
            # print("===", op.name, shared, op_reserved_exceeded)
            shared = shared.subtract(op_reserved_exceeded, non_negative=True)

        shared_divided = shared.scale(1.0 / num_ops)
        for op in self._resource_manager._topology:
            self._op_limits[op] = self._op_limits[op].add(shared_divided)
            self._op_limits[op].gpu = None

        return
        print("global limits", global_limits.object_store_memory_str())
        print("reserved", reserved.object_store_memory_str())
        print("shared", shared.object_store_memory_str())
        print("shared divided", shared_divided.object_store_memory_str())
        for op in self._op_limits:
            print(
                "===",
                op.name,
                "limit",
                self._op_limits[op].object_store_memory_str(),
                "usage",
                self._resource_manager.get_op_usage(op).object_store_memory_str(),
                "internal inqueue",
                op._metrics.obj_store_mem_inputs,
                "internal outqueue",
                op._metrics.obj_store_mem_outputs,
                "external outqueue",
                self._resource_manager._topology[op].outqueue_memory_usage(),
            )

    def get_op_available(self, op: PhysicalOperator) -> ExecutionResources:
        return self._op_limits[op]
