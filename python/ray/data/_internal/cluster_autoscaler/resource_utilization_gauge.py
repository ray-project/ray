import abc
import math
from dataclasses import dataclass
from typing import Optional

from ray.data._internal.average_calculator import TimeWindowAverageCalculator
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.util.metrics import Gauge


@dataclass(frozen=True)
class ClusterUtil:
    cpu: float = 0.0
    gpu: float = 0.0
    memory: float = 0.0
    object_store_memory: float = 0.0

    def __post_init__(self):
        # If we overcommit tasks, the logical utilization can exceed 1.0.
        assert math.isfinite(self.cpu) and 0 <= self.cpu, self.cpu
        assert math.isfinite(self.gpu) and 0 <= self.gpu, self.gpu
        assert math.isfinite(self.memory) and 0 <= self.memory, self.memory
        assert (
            math.isfinite(self.object_store_memory) and 0 <= self.object_store_memory
        ), self.object_store_memory


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
        self._cluster_mem_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )
        self._cluster_obj_mem_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )

        self._cluster_cpu_utilization_gauge = None
        self._cluster_gpu_utilization_gauge = None
        self._cluster_mem_utilization_gauge = None
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
            self._cluster_mem_utilization_gauge = Gauge(
                "data_cluster_mem_utilization",
                description="Cluster utilization % (Memory)",
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
        mem_util = save_div(global_usage.memory, global_limits.memory)
        obj_store_mem_util = save_div(
            global_usage.object_store_memory, global_limits.object_store_memory
        )

        self._cluster_cpu_util_calculator.report(cpu_util)
        self._cluster_gpu_util_calculator.report(gpu_util)
        self._cluster_mem_util_calculator.report(mem_util)
        self._cluster_obj_mem_util_calculator.report(obj_store_mem_util)

        if self._execution_id is not None:
            tags = {"dataset": self._execution_id}
            if self._cluster_cpu_utilization_gauge is not None:
                self._cluster_cpu_utilization_gauge.set(cpu_util * 100, tags=tags)
            if self._cluster_gpu_utilization_gauge is not None:
                self._cluster_gpu_utilization_gauge.set(gpu_util * 100, tags=tags)
            if self._cluster_mem_utilization_gauge is not None:
                self._cluster_mem_utilization_gauge.set(mem_util * 100, tags=tags)
            if self._cluster_object_store_memory_utilization_gauge is not None:
                self._cluster_object_store_memory_utilization_gauge.set(
                    obj_store_mem_util * 100, tags=tags
                )

    def get(self) -> ClusterUtil:
        """Get the average cluster utilization based on global usage / global limits."""
        return ClusterUtil(
            cpu=self._cluster_cpu_util_calculator.get_average() or 0,
            gpu=self._cluster_gpu_util_calculator.get_average() or 0,
            memory=self._cluster_mem_util_calculator.get_average() or 0,
            object_store_memory=self._cluster_obj_mem_util_calculator.get_average()
            or 0,
        )
