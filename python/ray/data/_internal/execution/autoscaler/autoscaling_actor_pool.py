from abc import ABC, abstractmethod

from ray.data._internal.execution.autoscaler.default_autoscaler import (
    ScalingConfig,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class AutoscalingActorPool(ABC):
    """Abstract interface of an autoscaling actor pool.

    A `PhysicalOperator` can manage one or more `AutoscalingActorPool`s.
    `Autoscaler` is responsible for deciding autoscaling of these actor
    pools.
    """

    @abstractmethod
    def min_size(self) -> int:
        """Min size of the actor pool."""
        ...

    @abstractmethod
    def max_size(self) -> int:
        """Max size of the actor pool."""
        ...

    @abstractmethod
    def current_size(self) -> int:
        """Current size of the actor pool."""
        ...

    @abstractmethod
    def num_running_actors(self) -> int:
        """Number of running actors."""
        ...

    @abstractmethod
    def num_active_actors(self) -> int:
        """Number of actors with at least one active task."""
        ...

    @abstractmethod
    def num_pending_actors(self) -> int:
        """Number of actors pending creation."""
        ...

    @abstractmethod
    def max_tasks_in_flight_per_actor(self) -> int:
        """Max number of in-flight tasks per actor."""
        ...

    @abstractmethod
    def num_tasks_in_flight(self) -> int:
        """Number of current in-flight tasks (running + pending tasks)."""
        ...

    def num_free_task_slots(self) -> int:
        """Number of free slots to run tasks.

        This doesn't include task slots for pending actors.
        """
        return (
            self.max_tasks_in_flight_per_actor() * self.num_running_actors()
            - self.num_tasks_in_flight()
        )

    @abstractmethod
    def can_scale_down(self):
        ...

    @abstractmethod
    def scale(self, config: ScalingConfig):
        """Applies autoscaling action"""
        ...

    @abstractmethod
    def per_actor_resource_usage(self) -> ExecutionResources:
        """Per actor resource usage."""
        ...

    @abstractmethod
    def get_pool_util(self) -> float:
        """Calculate the utilization of the given actor pool."""
        ...
