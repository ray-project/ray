import math
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ray.data._internal.execution.autoscaler.autoscaling_actor_pool import (
    AutoscalingActorPool,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.execution_options import (
        ExecutionResources,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology


@DeveloperAPI
class Autoscaler(ABC):
    """Abstract interface for Ray Data autoscaler."""

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        execution_id: str,
    ):
        self._topology = topology
        self._resource_manager = resource_manager
        self._execution_id = execution_id

    @abstractmethod
    def try_trigger_scaling(self):
        """Try trigger autoscaling.

        This method will be called each time when StreamingExecutor makes
        a scheduling decision. A subclass should override this method to
        handle the autoscaling of both the cluster and `AutoscalingActorPool`s.
        """
        ...

    @abstractmethod
    def on_executor_shutdown(self):
        """Callback when the StreamingExecutor is shutting down."""
        ...

    @abstractmethod
    def get_total_resources(self) -> "ExecutionResources":
        """Get the total resources that are available to this data execution."""
        ...

    def _get_max_scale_up_wrt_budget(
        self,
        actor_pool: AutoscalingActorPool,
        budget: "ExecutionResources",
    ) -> Optional[int]:
        assert budget.cpu >= 0 and budget.gpu >= 0

        num_cpus_per_actor = actor_pool.per_actor_resource_usage().cpu
        num_gpus_per_actor = actor_pool.per_actor_resource_usage().gpu
        assert num_cpus_per_actor >= 0 and num_gpus_per_actor >= 0

        max_cpu_scale_up: float = float("inf")
        if num_cpus_per_actor > 0 and not math.isinf(budget.cpu):
            max_cpu_scale_up = budget.cpu // num_cpus_per_actor

        max_gpu_scale_up: float = float("inf")
        if num_gpus_per_actor > 0 and not math.isinf(budget.gpu):
            max_gpu_scale_up = budget.gpu // num_gpus_per_actor

        max_scale_up = min(max_cpu_scale_up, max_gpu_scale_up)
        if math.isinf(max_scale_up):
            return None
        assert not math.isnan(max_scale_up), (
            budget,
            num_cpus_per_actor,
            num_gpus_per_actor,
        )
        return int(max_scale_up)
