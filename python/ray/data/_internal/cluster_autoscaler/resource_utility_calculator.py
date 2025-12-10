import abc

from ray.data._internal.average_calculator import TimeWindowAverageCalculator
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.resource_manager import ResourceManager

ClusterUtil = ExecutionResources


class ResourceUtilizationCalculator(abc.ABC):
    def observe(self):
        """Observe the cluster utilization."""
        ...

    def get(self) -> ClusterUtil:
        """Get the resource cluster utilization."""
        ...


class LogicalUtilizationCalculator(ResourceUtilizationCalculator):

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
        self._cluster_ob_mem_util_calculator = TimeWindowAverageCalculator(
            cluster_util_avg_window_s
        )

    def observe(self):
        """Report the cluster utilization based on global usage / global limits."""
        global_usage = self._resource_manager.get_global_usage()
        global_limits = self._resource_manager.get_global_limits()
        global_utilization = global_usage / global_limits
        self._cluster_cpu_util_calculator.report(global_utilization.cpu)
        self._cluster_gpu_util_calculator.report(global_utilization.gpu)
        self._cluster_ob_mem_util_calculator.report(
            global_utilization.object_store_memory
        )

    def get(self) -> ExecutionResources:
        """Report the cluster utilization based on global usage / global limits."""
        return ExecutionResources(
            cpu=self._cluster_cpu_util_calculator.get_average(),
            gpu=self._cluster_gpu_util_calculator.get_average(),
            object_store_memory=self._cluster_ob_mem_util_calculator.get_average(),
        )
