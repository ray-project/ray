from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

from ray import ObjectRef
from ray.actor import ActorHandle
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
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


@dataclass(frozen=True)
class ActorPoolInfo:
    """Breakdown of the state of the actors used by the ``PhysicalOperator``"""

    running: int
    pending: int
    restarting: int

    def __str__(self):
        return (
            f"running={self.running}, restarting={self.restarting}, "
            f"pending={self.pending}"
        )


@DeveloperAPI
class AutoscalingActorPool(ABC):
    """Abstract interface of an autoscaling actor pool.

    A `PhysicalOperator` can manage one or more `AutoscalingActorPool`s.
    `Autoscaler` is responsible for deciding autoscaling of these actor
    pools.
    """

    _LogicalActorId_LABEL_KEY = "__ray_data_logical_actor_id"

    def __init__(
        self,
        min_size: int,
        max_size: int,
        initial_size: int,
        max_tasks_in_flight_per_actor: int,
        max_actor_concurrency: int,
        per_actor_resource_usage: ExecutionResources,
    ):
        assert min_size >= 1
        assert max_size >= min_size
        assert initial_size <= max_size
        assert initial_size >= min_size
        assert max_tasks_in_flight_per_actor >= 1

        self._min_size = min_size
        self._max_size = max_size
        self._initial_size = initial_size
        self._max_tasks_in_flight_per_actor = max_tasks_in_flight_per_actor
        self._max_actor_concurrency = max_actor_concurrency
        self._per_actor_resource_usage = per_actor_resource_usage

    @abstractmethod
    def num_running_actors(self) -> int:
        """Number of running actors."""
        ...

    @abstractmethod
    def num_restarting_actors(self) -> int:
        """Number of restarting actors"""
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
    def num_tasks_in_flight(self) -> int:
        """Number of current in-flight tasks (ie total nubmer of tasks that have been
        submitted to the actor pool)."""
        ...

    def can_schedule_task(self) -> bool:
        """Returns `True` iff the actor pool has an available actor that can run a task."""
        return self.select_actor_for_bundle() is not None

    @abstractmethod
    def scale(self, req: ActorPoolScalingRequest):
        """Applies autoscaling action"""
        ...

    @abstractmethod
    def refresh_actor_state(self):
        """Refreshes the actor pool state (for, example, running, restarting, pending)"""
        ...

    @abstractmethod
    def on_task_submitted(self, actor: ActorHandle):
        """Callback when an actor is picked for running a task"""
        ...

    @abstractmethod
    def on_task_completed(self, actor: ActorHandle):
        """Called when a task completes. Returns the provided actor to the pool."""
        ...

    @abstractmethod
    def select_actor_for_bundle(
        self,
        bundle: Optional[RefBundle] = None,
        actor_locality_enabled: bool = False,
    ) -> Optional[ActorHandle]:
        """Select an actor to process the given bundle.

        When ``bundle`` is ``None``, returns any available actor with spare
        capacity (used by ``can_schedule_task`` to probe schedulability).
        When ``bundle`` is provided, returns the best actor for that bundle
        (considering locality when ``actor_locality_enabled`` is True).

        Returns:
            An actor handle if an actor with capacity is available, otherwise
            ``None``.
        """
        ...

    @abstractmethod
    def get_pending_actor_refs(self) -> List[ObjectRef]:
        """Return the list of object refs for actors that are pending creation."""
        ...

    @abstractmethod
    def pending_to_running(self, ready_ref: ObjectRef) -> Optional[ActorHandle]:
        """Mark the actor corresponding to the provided ready future as running.

        Args:
            ready_ref: The ready future for the actor to mark as running.

        Returns:
            The actor handle if the actor is still alive, otherwise ``None``.
        """
        ...

    @abstractmethod
    def get_actor_location(self, actor: ActorHandle) -> str:
        """Get the node_id of the actor"""
        ...

    @abstractmethod
    def shutdown(self, force: bool = False):
        """Kills all actors, including running/active actors.

        This is called once the operator is shutting down.
        """
        ...

    def get_logical_id_label_key(self) -> str:
        """Get the label key for the logical actor ID.

        Actors launched by this pool should have this label.
        """
        return self._LogicalActorId_LABEL_KEY

    def get_actor_info(self) -> ActorPoolInfo:
        """Returns current snapshot of actors' being used in the pool"""
        return ActorPoolInfo(
            running=self.num_alive_actors(),
            pending=self.num_pending_actors(),
            restarting=self.num_restarting_actors(),
        )

    def num_alive_actors(self) -> int:
        """Alive actors are all the running actors in ALIVE state."""
        return self.num_running_actors() - self.num_restarting_actors()

    def num_idle_actors(self) -> int:
        """Return the number of idle actors in the pool."""
        return self.num_running_actors() - self.num_active_actors()

    def per_actor_resource_usage(self) -> ExecutionResources:
        """Per actor resource usage."""
        return self._per_actor_resource_usage

    def max_actor_concurrency(self) -> int:
        """Returns max number of tasks single actor could run concurrently."""
        return self._max_actor_concurrency

    def max_tasks_in_flight_per_actor(self) -> int:
        """Max number of in-flight tasks per actor."""
        return self._max_tasks_in_flight_per_actor

    def initial_size(self) -> int:
        return self._initial_size

    def current_size(self) -> int:
        return self.num_pending_actors() + self.num_running_actors()

    def min_size(self) -> int:
        """Min size of the actor pool."""
        return self._min_size

    def max_size(self) -> int:
        """Max size of the actor pool."""
        return self._max_size

    def get_pool_util(self) -> float:
        """Calculate the utilization of the given actor pool."""

        # If there are no running actors, we set the utilization to indicate that the pool should be scaled up immediately.
        if self.current_size() == 0:
            return float("inf")
        else:
            # We compute utilization as a ratio of
            #  - Number of submitted tasks over
            #  - Max number of tasks that Actor Pool could currently run
            #
            # This value could exceed 100%, since by default actors are allowed
            # to queue tasks (to pipeline task execution by overlapping block
            # fetching with the execution of the previous task)
            return self.num_tasks_in_flight() / (
                self.max_actor_concurrency() * self.current_size()
            )
