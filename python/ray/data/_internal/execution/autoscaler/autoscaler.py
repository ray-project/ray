from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray._private.ray_constants import env_float
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology


DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD: float = env_float(
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD",
    1.0,
)

DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD: float = env_float(
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD",
    0.5,
)


@DeveloperAPI
@dataclass
class AutoscalingConfig:
    # Actor Pool utilization threshold for upscaling. Once Actor Pool
    # exceeds this utilization threshold it will start adding new actors.
    #
    # NOTE: Actor Pool utilization is defined as ratio of
    #
    #   - Number of submitted tasks to
    #   - Max number of tasks the current set of Actors in the pool could run
    #     (defined as Ray Actor's `max_concurrency` * `pool.num_running_actors`)
    #
    # This utilization value could exceed 100%, when the number of submitted tasks
    # exceed available concurrency-slots to run them in the current set of actors.
    #
    # This is possible when `max_tasks_in_flight_per_actor` (defaults to 2 x
    # of `max_concurrency`) > Actor's `max_concurrency` and allows to overlap
    # task execution with the fetching of the blocks for the next task providing
    # for ability to negotiate a trade-off between autoscaling speed and resource
    # efficiency (ie making tasks wait instead of immediately triggering execution)
    actor_pool_util_upscaling_threshold: float = (
        DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD,
    )

    # Actor Pool utilization threshold for downscaling
    actor_pool_util_downscaling_threshold: float = (
        DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD,
    )


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
