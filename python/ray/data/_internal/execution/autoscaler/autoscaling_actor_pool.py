from abc import ABC, abstractmethod

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
    def current_in_flight_tasks(self) -> int:
        """Number of current in-flight tasks."""
        ...

    def num_total_task_slots(self) -> int:
        """Total number of task slots."""
        return self.max_tasks_in_flight_per_actor() * self.current_size()

    def num_free_task_slots(self) -> int:
        """Number of free slots to run tasks."""
        return (
            self.max_tasks_in_flight_per_actor() * self.current_size()
            - self.current_in_flight_tasks()
        )

    @abstractmethod
    def scale_up(self, num_actors: int) -> int:
        """Request the actor pool to scale up by the given number of actors.

        The number of actually added actors may be less than the requested
        number.

        Returns:
            The number of actors actually added.
        """
        ...

    @abstractmethod
    def scale_down(self, num_actors: int) -> int:
        """Request actor pool to scale down by the given number of actors.

        The number of actually removed actors may be less than the requested
        number.

        Returns:
            The number of actors actually removed.
        """
        ...

    @abstractmethod
    def per_actor_resource_usage(self) -> ExecutionResources:
        """Per actor resource usage."""
        ...
