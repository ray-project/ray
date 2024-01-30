import time
from typing import TYPE_CHECKING, Dict

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
