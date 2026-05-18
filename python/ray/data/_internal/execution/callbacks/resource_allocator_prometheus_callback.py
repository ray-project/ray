import math
from typing import TYPE_CHECKING, Dict

from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.resource_manager import ResourceManager
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

        for i, op in enumerate(topology):
            tags = {
                "dataset": dataset_id,
                "operator": executor._get_operator_id(op, i),
            }
            self._update_budget_metrics(op, tags, resource_manager)
            self._update_max_bytes_to_read_metric(op, tags, resource_manager)

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
        op: PhysicalOperator,
        tags: Dict[str, str],
        resource_manager: ResourceManager,
    ):
        budget = resource_manager.get_budget(op)
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
        op: PhysicalOperator,
        tags: Dict[str, str],
        resource_manager: ResourceManager,
    ):
        if resource_manager.op_resource_allocator_enabled():
            resource_allocator = resource_manager.op_resource_allocator
            output_budget_bytes = resource_allocator.get_output_budget(op)
            if output_budget_bytes is not None:
                if math.isinf(output_budget_bytes):
                    # Convert inf to -1 to represent unlimited bytes to read
                    output_budget_bytes = -1
                self._max_bytes_to_read_gauge.set(output_budget_bytes, tags=tags)
