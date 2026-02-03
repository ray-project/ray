import abc
from typing import Optional

from ray.data._internal.average_calculator import TimeWindowAverageCalculator
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.util.metrics import Gauge

ClusterUtil = ExecutionResources


class ResourceUtilizationGauge(abc.ABC):
    @abc.abstractmethod
    def observe(self):
        """Observe the cluster utilization."""
        ...

    @abc.abstractmethod
    def get(self) -> ClusterUtil:
        """Get the resource cluster utilization."""
        ...


class RollingLogicalUtilizationGauge(ResourceUtilizationGauge):
    # Default time window in seconds to calculate the average of cluster utilization.
    DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S: int = 10

    def __init__(
        self,
        resource_manager: ResourceManager,
        *,
        cluster_util_avg_window_s: float = DEFAULT_CLUSTER_UTIL_AVG_WINDOW_S,
        execution_id: Optional[str] = None,
    ):
        self._resource_manager = resource_manager
        self._execution_id = execution_id

        self._cluster_cpu_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )
        self._cluster_gpu_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )
        self._cluster_obj_mem_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )

        self._cluster_cpu_utilization_gauge = None
        self._cluster_gpu_utilization_gauge = None
        self._cluster_object_store_memory_utilization_gauge = None

        if self._execution_id is not None:
            self._cluster_cpu_utilization_gauge = Gauge(
                "data_cluster_cpu_utilization",
                description="Cluster utilization % (CPU)",
                tag_keys=("dataset",),
            )
            self._cluster_gpu_utilization_gauge = Gauge(
                "data_cluster_gpu_utilization",
                description="Cluster utilization % (GPU)",
                tag_keys=("dataset",),
            )
            self._cluster_object_store_memory_utilization_gauge = Gauge(
                "data_cluster_object_store_memory_utilization",
                description="Cluster utilization % (Object Store Memory)",
                tag_keys=("dataset",),
            )

    def observe(self):
        """Report the cluster utilization based on global usage / global limits."""

        def save_div(numerator, denominator):
            if not denominator:
                return 0
            else:
                return numerator / denominator

        global_usage = self._resource_manager.get_global_usage()
        global_limits = self._resource_manager.get_global_limits()

        cpu_util = save_div(global_usage.cpu, global_limits.cpu)
        gpu_util = save_div(global_usage.gpu, global_limits.gpu)
        obj_store_mem_util = save_div(
            global_usage.object_store_memory, global_limits.object_store_memory
        )

        self._cluster_cpu_util_calculator.report(cpu_util)
        self._cluster_gpu_util_calculator.report(gpu_util)
        self._cluster_obj_mem_util_calculator.report(obj_store_mem_util)

        if self._execution_id is not None:
            tags = {"dataset": self._execution_id}
            if self._cluster_cpu_utilization_gauge is not None:
                self._cluster_cpu_utilization_gauge.set(cpu_util * 100, tags=tags)
            if self._cluster_gpu_utilization_gauge is not None:
                self._cluster_gpu_utilization_gauge.set(gpu_util * 100, tags=tags)
            if self._cluster_object_store_memory_utilization_gauge is not None:
                self._cluster_object_store_memory_utilization_gauge.set(
                    obj_store_mem_util * 100, tags=tags
                )

    def get(self) -> ExecutionResources:
        """Get the average cluster utilization based on global usage / global limits."""
        return ExecutionResources(
            cpu=self._cluster_cpu_util_calculator.get_average(),
            gpu=self._cluster_gpu_util_calculator.get_average(),
            object_store_memory=self._cluster_obj_mem_util_calculator.get_average(),
        )
