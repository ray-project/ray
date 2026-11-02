import math
from typing import TYPE_CHECKING, Dict, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces import ExecutionResources, PhysicalOperator
from ray.util.metrics import Gauge

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import StreamingExecutor


class ResourceAllocatorPrometheusCallback(ExecutionCallback):
    """Updates Prometheus metrics related to resource allocation.

    This callback monitors the StreamingExecutor and updates Prometheus
    Gauges for CPU, GPU, memory, and object store memory budgets for each
    operator at every execution step.
    """

    def __init__(self):
        self._cpu_budget_gauge: Gauge = Gauge(
            "data_cpu_budget",
            "Budget (CPU) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._gpu_budget_gauge: Gauge = Gauge(
            "data_gpu_budget",
            "Budget (GPU) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._memory_budget_gauge: Gauge = Gauge(
            "data_memory_budget",
            "Budget (Memory) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._osm_budget_gauge: Gauge = Gauge(
            "data_object_store_memory_budget",
            "Budget (Object Store Memory) per operator",
            tag_keys=("dataset", "operator"),
        )
        self._max_bytes_to_read_gauge: Gauge = Gauge(
            "data_max_bytes_to_read",
            description="Maximum bytes to read from streaming generator buffer.",
            tag_keys=("dataset", "operator"),
        )

    def on_execution_step(self, executor: "StreamingExecutor") -> None:
        """Called by the executor after every scheduling loop step."""
        topology = executor._topology
        resource_manager = executor._resource_manager
        dataset_id = executor._dataset_id

        if topology is None or resource_manager is None:
            return

        for op, op_state in topology.items():
            tags = {
                "dataset": dataset_id,
                "operator": op_state.op_tag(),
            }
            if executor._actor_only_backend:
                budget = _actor_only_budget(op)
                output_budget = _actor_only_output_budget(op)
            else:
                budget = resource_manager.get_budget(op)
                output_budget = (
                    resource_manager.op_resource_allocator.get_output_budget(op)
                    if resource_manager.op_resource_allocator_enabled()
                    else None
                )
            self._update_budget_metrics(budget, tags)
            self._update_max_bytes_to_read_metric(output_budget, tags)

    def after_execution_succeeds(self, executor: "StreamingExecutor") -> None:
        """Updates metrics upon successful execution to ensure final states are captured."""
        self.on_execution_step(executor)

    def after_execution_fails(
        self, executor: "StreamingExecutor", error: Exception
    ) -> None:
        """Updates metrics upon execution failure to ensure final states are captured."""
        self.on_execution_step(executor)

    def _update_budget_metrics(
        self,
        budget: Optional[ExecutionResources],
        tags: Dict[str, str],
    ):
        if budget is None:
            cpu_budget = 0
            gpu_budget = 0
            memory_budget = 0
            object_store_memory_budget = 0
        else:
            cpu_budget = -1 if math.isinf(budget.cpu) else budget.cpu
            gpu_budget = -1 if math.isinf(budget.gpu) else budget.gpu
            memory_budget = -1 if math.isinf(budget.memory) else budget.memory
            object_store_memory_budget = (
                -1
                if math.isinf(budget.object_store_memory)
                else budget.object_store_memory
            )

        self._cpu_budget_gauge.set(cpu_budget, tags=tags)
        self._gpu_budget_gauge.set(gpu_budget, tags=tags)
        self._memory_budget_gauge.set(memory_budget, tags=tags)
        self._osm_budget_gauge.set(object_store_memory_budget, tags=tags)

    def _update_max_bytes_to_read_metric(
        self,
        output_budget_bytes: Optional[float],
        tags: Dict[str, str],
    ):
        if output_budget_bytes is not None:
            if math.isinf(output_budget_bytes):
                # Convert inf to -1 to represent unlimited bytes to read
                output_budget_bytes = -1
            self._max_bytes_to_read_gauge.set(output_budget_bytes, tags=tags)


# Backwards compatibility only: the actor-only backend has no ResourceManager
# budgets, so these derive the classic budget metrics from its actor pools.
# Remove with those metrics once task pools are gone.


def _actor_only_budget(op: PhysicalOperator) -> Optional[ExecutionResources]:
    """What ``op``'s actors hold: their count times the per-actor shape, and
    their summed output limits as object store. Draining actors count, as in
    the op's usage, since they keep their resources until they die."""
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

    if not isinstance(op, ExperimentalActorPoolMapOperator):
        return None
    pool = op.actor_pool
    return (
        pool.per_actor_resource_usage()
        .scale(pool.current_size())
        .copy(object_store_memory=pool.total_output_bytes_limit())
    )


def _actor_only_output_budget(op: PhysicalOperator) -> Optional[float]:
    """Bytes ``op`` may still pull from its actors' generators."""
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

    if not isinstance(op, ExperimentalActorPoolMapOperator):
        return None
    return op.remaining_pull_bytes()
