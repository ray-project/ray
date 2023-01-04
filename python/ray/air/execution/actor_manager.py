from typing import Any, Dict, Iterable, Optional, Tuple, Type

from ray.air import AcquiredResources
from ray.air.execution.actor_spec import ActorSpec, TrackedActor
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.air.execution.event import (
    ExecutionEvent,
    FutureResult,
)


class ActorManager:
    """Management class for actors and actor task futures.

    This class provides an event-based management interface for actors and
    actor task futures.

    The actor manager can be used to start actors, stop actors, and schedule and
    track task futures on these actors. The manager will then yield events related
    to the tracked entities.

    For instance, when an actor is added with ``add_actor()``, the event
    ``ActorStarted`` will be emitted once the actor was successfully started.
    Likewise, after calling ``remove_actor()``, the event ``ActorStopped`` will
    be emitted once the actor stopped.
    If the actor fails at any time, the ``ActorFailed`` event will be emitted.

    For actor task futures, a subclass of the ``FutureResult`` event will be emitted
    when the future resolves. This is either a custom subclass provided when
    scheduling the future, or a ``FutureFailed`` event if resolving the future lead
    to a ``RayTaskError``.

    Actor properties are defined via an ``ActorSpec`` object. This object specifies
    the actor class, the keyword arguments used for initialization, and the resources
    required to start the actor.

    Once added, subsequent interaction with the actor via the manager uses an
    ``TrackedActor`` object. This object can be used to remove the actor or to schedule
    futures on it. Existence of this object does not guarantee that the actual Ray actor
    has been scheduled. This status can be inquired from the actor manager using the
    ``TrackedActor`` object.

    Args:
        resource_manager: Resource manager used to request resources for the actors.

    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        raise NotImplementedError

    def wait(self) -> None:
        """Wait until next event is available."""
        raise RuntimeError

    def has_event_available(self) -> bool:
        """Return True if there is an event immediately available."""
        raise RuntimeError

    def get_next_event(self, block: bool = True) -> Optional[ExecutionEvent]:
        """Get next event, if available.

        If ``block=False`` and there is no event immediately available, this
        method will return ``None``.

        Args:
            block: If True, will block until an event is available. Otherwise,
                will return immediately, which can be None if no event is ready.

        Returns:
            Execution event to act upon.
        """
        raise RuntimeError

    @property
    def num_started_actors(self):
        """Return number of started actors."""
        raise NotImplementedError

    @property
    def num_pending_actors(self):
        """Return number of pending (not yet started) actors."""
        raise NotImplementedError

    @property
    def num_total_actors(self):
        """Return number of total actors."""
        raise NotImplementedError

    def add_actor(self, actor_spec: ActorSpec) -> TrackedActor:
        """Add an actor to be tracked.

        The actor class to start, the constructor arguments, and the resources to
        start the actor with are provided in the ``actor_spec`` argument.

        This method will request resources to start the actor. Once the resources
        are available, the actor will be started and an event will be emitted.

        Events:
            ``ActorStarted``: Emitted once the actor has been started.

        Args:
            actor_spec: Spec for the actor to be started once resources are available.

        Returns:
            Tracked actor to be used to schedule futures or remove the actor.

        """
        raise NotImplementedError

    def remove_actor(
        self,
        tracked_actor: TrackedActor,
        resolve_futures: bool = True,
        kill: bool = False,
    ) -> None:
        """Remove an actor given its spec.

        If the actor has already been started, this will remove the actor. If the
        actor has only been requested, but not started, yet, this will cancel
        the actor request.

        If ``resolve_futures=True``, this will cache the actor removal and only
        remove it once all its tracked futures are resolved.

        If ``kill=True``, this will use ``ray.kill()`` to forcefully terminate the
        actor. Otherwise, graceful actor deconstruction will be scheduled after
        all currently tracked futures are resolved.

        After stopping the actor, an ``ActorStopped`` event will be emitted.

        Events:
            ``ActorStopped``: Emitted once the actor has been started. If the actor
                has only been requested (but has not been started, yet),
                this event won't be emitted.

        Args:
            tracked_actor: Tracked actor to be removed.
            resolve_futures: If True, will resolve associated futures (and emit
                events) first before stopping the actor.
            kill: If set, will forcefully terminate the actor instead of gracefully
                scheduling termination.
        """
        raise NotImplementedError

    def is_actor_started(self, tracked_actor: TrackedActor) -> bool:
        """Returns True if the actor has been started.

        Args:
            tracked_actor: Tracked actor to inquire state about.
        """
        raise NotImplementedError

    def get_actor_resources(
        self, tracked_actor: TrackedActor
    ) -> Optional[AcquiredResources]:
        """Returns the acquired resources of an actor that has been started.

        This will return ``None`` if the actor has not been started, yet.

        Args:
            tracked_actor: Tracked actor to get resources for.
        """
        raise NotImplementedError

    def schedule_task(
        self,
        tracked_actor: TrackedActor,
        task_spec: Tuple[str, Iterable[Any], Dict[str, Any]],
        result_cls: Type[FutureResult],
    ) -> None:
        """Track (asynchronous) future associated to a tracked actor.

        This method will schedule the remote task and track its execution. Once the
        future is resolved, it will be emitted as an event. The event
        class will be ``result_cls``, which should extend ``FutureResult``.

        The ``task_spec`` argument is submitted as a tuple of
        ``(method_name, args, kwargs)``. This will result in the future
        ``future = actor.method_name.remote(*args, **kwargs)`` to be scheduled on
        the actor and tracked in the manager.

        Events:
            ``result_cls``: Emitted once the future resolved.
            ``FutureFailed``: Emitted if the future resolution fails due to a
                ``RayTaskError`, indicating method execution ran into an exception.
            ``ActorFailed``: Emitted if the future resolution fails due to a
                ``RayActorError``, indicating the actor died.

        Args:
            tracked_actor: Tracked actor to schedule task for.
            task_spec: Tuple of ``(method_name, args, kwargs)`` to be scheduled as
                task futures on the actor.
            result_cls: Class extending ``FutureResult`` to emit when the future
                resolves successfully.

        """
        raise NotImplementedError

    def schedule_sync_tasks(
        self,
        actors_tasks: Dict[TrackedActor, Tuple[str, Iterable[Any], Dict[str, Any]]],
        result_cls: Type[FutureResult],
    ) -> None:
        """Track synchronous futures associated to multiple tracked actors.

        This method will schedule remote tasks and track their execution
        synchronously on all provided actors. This means the multi result event will
        only be emitted once _all_ provided tasks resolved.

        Once all futures resolved successfully, a ``MultiFutureResult`` event
        containing all sub results will be emitted as an event.
        The sub results will be of type ``result_cls``, which should
        extend ``FutureResult``.

        If any of the tasks fail, a ``MultiFutureFailed`` event will be emitted.
        The ``exception`` parameter will contain the first observed exception.
        Unfinished tasks will be cancelled. The sub results will be of type
        ``FutureFailed`` if for each failed task future, ``FutureCancelled`` for each
        cancelled, and ``result_cls`` for all futures that resolved before
        one of the other futures failed.

        If any of the actors fail, the same ``MultiFutureFailed`` event will be
        emitted. The sub results can then also be of type ``ActorFailed``.
        Additionally, for each failed actor, a separate ``ActorFailed`` event will be
        emitted.

        Events:
            ``MultiFutureResult``: Emitted once all futures resolved. Contains
                sub result of type ``result_cls``.
            ``MultiFutureFailed``: Emitted if any of the future resolution fails due
                to a ``RayTaskError`` or ``RayActorError``.
            ``ActorFailed``: Emitted if the future resolution fails due to a
                ``RayActorError``, indicating the actor died. This will be emitted
                in addition to the ``MultiFutureFailed`` event.

        Args:
            actors_tasks: Tracked actors mapping to actor method specifications.
            result_cls: Class extending ``FutureResult`` to emit as sub results of
                ``MultiFutureResult`` when the futures resolve successfully.
        """
        raise NotImplementedError
