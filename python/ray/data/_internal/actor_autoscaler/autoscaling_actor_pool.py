from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.util.annotations import DeveloperAPI


@dataclass(frozen=True)
class ActorPoolScalingRequest:

    delta: int
    force: bool = field(default=False)
    reason: Optional[str] = field(default=None)

    @classmethod
    def no_op(cls, *, reason: Optional[str] = None) -> "ActorPoolScalingRequest":
        return ActorPoolScalingRequest(delta=0, reason=reason)

    @classmethod
    def upscale(cls, *, delta: int, reason: Optional[str] = None):
        assert delta > 0
        return ActorPoolScalingRequest(delta=delta, reason=reason)

    @classmethod
    def downscale(
        cls, *, delta: int, force: bool = False, reason: Optional[str] = None
    ):
        assert delta < 0, "For scale down delta is expected to be negative!"
        return ActorPoolScalingRequest(delta=delta, force=force, reason=reason)


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
    def max_actor_concurrency(self) -> int:
        """Returns max number of tasks single actor could run concurrently."""
        ...

    @abstractmethod
    def num_tasks_in_flight(self) -> int:
        """Number of current in-flight tasks (ie total nubmer of tasks that have been
        submitted to the actor pool)."""
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
    def scale(self, req: ActorPoolScalingRequest):
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
