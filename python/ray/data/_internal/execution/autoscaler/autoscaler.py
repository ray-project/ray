from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray._private.ray_constants import env_float
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology


# Default threshold of actor pool utilization to trigger scaling up.
DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD: float = env_float(
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD",
    0.95,
)

# Default threshold of actor pool utilization to trigger scaling down.
DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD: float = env_float(
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD",
    0.5,
)


@DeveloperAPI
@dataclass
class AutoscalingConfig:

    actor_pool_util_upscaling_threshold: float = DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD,  # noqa: E501
    actor_pool_util_downscaling_threshold: float = DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD,


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
    def get_total_resources(self) -> ExecutionResources:
        """Get the total resources that are available to this data execution."""
        ...
