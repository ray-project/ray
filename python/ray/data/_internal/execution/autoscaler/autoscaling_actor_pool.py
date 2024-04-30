from abc import ABCMeta, abstractmethod

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class AutoscalingActorPool(metaclass=ABCMeta):
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

    @abstractmethod
    def scale_up(self, num_actors: int) -> int:
        """Scale up the actor pool by the given number of actors.

        Returns:
            The number of actors actually added.
        """
        ...

    @abstractmethod
    def scale_down(self, num_actors: int) -> int:
        """Scale down the actor pool by the given number of actors.

        Returns:
            The number of actors actually removed.
        """
        ...
