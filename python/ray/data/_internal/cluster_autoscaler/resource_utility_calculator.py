import abc

from ray.data._internal.average_calculator import TimeWindowAverageCalculator
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.resource_manager import ResourceManager

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
    ):
        self._resource_manager = resource_manager

        self._cluster_cpu_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )
        self._cluster_gpu_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )
        self._cluster_obj_mem_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
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

    def get(self) -> ExecutionResources:
        """Get the average cluster utilization based on global usage / global limits."""
        return ExecutionResources(
            cpu=self._cluster_cpu_util_calculator.get_average(),
            gpu=self._cluster_gpu_util_calculator.get_average(),
            object_store_memory=self._cluster_obj_mem_util_calculator.get_average(),
        )
