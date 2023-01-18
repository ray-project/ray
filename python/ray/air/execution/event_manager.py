import enum
from numbers import Number
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from ray.air.execution.resources import (
    AcquiredResources,
    ResourceManager,
    ResourceRequest,
)

from ray.air.execution.tracked_actor import TrackedActor
from ray.air.execution.tracked_actor_task import (
    TrackedActorTask,
    TrackedActorTaskCollection,
)
from ray.air.execution.tracked_task import TrackedTask


class EventType(enum.Enum):
    """Event type to specify when yielding control to the :class:`RayEventManager`.

    This enum can be passed to
    :meth:`RayEventManager.wait() <ray.air.execution.event_manager.RayEventManager.wait`
    to specify which kind of events to await.

    Attributes:
        ALL: All event types are awaited.
        TASKS: Only events relating to tasks or actor tasks will be awaited.
        ACTORS: Only events relating to actor starts or stops will be awaited.

    """

    ALL = 0
    TASKS = 1
    ACTORS = 2


class RayEventManager:
    """Management class for Ray actors, tasks, and actor tasks.

    This class provides an event-based management interface for actors, tasks, and
    actor tasks.

    The manager can be used to start actors, stop actors, and schedule and
    track task futures individually or on these actors.
    The manager will then invoke callbacks related to the tracked entities.

    For instance, when an actor is added with
    :meth:`add_actor() <ray.air.execution.event_manager.RayEventManager.add_actor>`,
    a :ref:`TrackedActor <ray.air.execution.tracked_actor.TrackedActor` object is
    returned.
    The :meth:`TrackedActor.on_start()
    <ray.air.execution.tracked_actor.TrackedActor.on_start>`
    method can then be used to specify a callback that is invoked once the actor
    successfully started. The other callbacks relating to tracked actors
    :meth:`TrackedActor.on_stop()
    <ray.air.execution.tracked_actor.TrackedActor.on_stop>` and
    :meth:`TrackedActor.on_error()
    <ray.air.execution.tracked_actor.TrackedActor.on_error>`

    Similarly, when scheduling an actor task using
    :meth:`schedule_actor_task()
    <ray.air.execution.event_manager.RayEventManager.schedule_actor_task>`,
    a :ref:`TrackedActorTask <ray.air.execution.tracked_actor_task.TrackedActorTask`
    object is returned.
    The :meth:`TrackedActorTask.on_result()
    <ray.air.execution.tracked_actor_task.TrackedActorTask.on_result>`
    method can then be used to specify a callback that is invoked when the task
    successfully resolved.
    The :meth:`TrackedActorTask.on_error()
    <ray.air.execution.tracked_actor_task.TrackedActorTask.on_error>`
    method can then be used to specify a callback that is invoked when the task
    fails.

    Lastly, regular tasks (not on actors) can be tracked. When scheduling a task
    using
    :meth:`schedule_task()
    <ray.air.execution.event_manager.RayEventManager.schedule_task>`,
    a :ref:`TrackedTask <ray.air.execution.tracked_task.TrackedTask`
    object is returned.
    The :meth:`TrackedTask.on_result()
    <ray.air.execution.tracked_task.TrackedTask.on_result>`
    method can then be used to specify a callback that is invoked once the task
    successfully resolved.
    The :meth:`TrackedTask.on_error()
    <ray.air.execution.tracked_task.TrackedTask.on_error>`
    method can then be used to specify a callback that is invoked when the actor task
    fails.

    Args:
        resource_manager: Resource manager used to request resources for the actors.

    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        raise NotImplementedError

    def wait(
        self,
        num_events: Optional[int] = None,
        timeout: Optional[Number] = None,
        event_type: EventType = EventType.ALL,
    ) -> None:
        """Yield control to event manager to await events and invoke callbacks.

        Calling this method will wait for up to ``timeout`` seconds for up to
        ``num_events`` new events to arrive.
        When events arrive, callbacks relating to the events will be
        invoked. A timeout of ``None`` will block until the next event arrives.

        If ``num_events`` is set, it will only wait for that many events to arrive
        before returning control to the caller. If ``num_events=None``, this will
        block until all tracked tasks resolved.

        ``event_type`` specifies the event types to await. If this includes
        Ray Actor events (i.e. ``EventType.ACTORS`` or ``EventType.ALL``), a
        timeout must be specified. This is to ensure that we don't run into a
        deadlock if not enough resources are available to start ``num_events``
        actors.

        Note:
            If an actor task fails with a ``RayActorError``, this is one event,
            but it may trigger _two_ `on_error` callbacks: One for the actor,
            and one for the task.

        Note:
            The ``timeout`` argument is used for pure waiting time for events. It does
            not include time spent on processing callbacks. Depending on the processing
            time of the callbacks, it can take much longer for this function to
            return than the specified timeout.

        Args:
            num_events: Number of events to await before returning control
                to the caller.
            timeout: Timeout in seconds to wait for events.
            event_type: Type of events to await. Defaults to ``EventType.ALL``.

        Raises:
            ValueError: If awaiting actor events and no ``timeout`` is set.

        """
        raise RuntimeError

    @property
    def all_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects managed by this manager instance."""
        raise NotImplementedError

    @property
    def live_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects that are currently alive."""
        raise NotImplementedError

    @property
    def pending_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects that are currently pending."""
        raise NotImplementedError

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

    def add_actor(
        self, cls: Type, kwargs: Dict[str, Any], resource_request: ResourceRequest
    ) -> TrackedActor:
        """Add an actor to be tracked.

        This method will request resources to start the actor. Once the resources
        are available, the actor will be started and the
        :meth:`TrackedActor.on_start
        <ray.air.execution.tracked_actor.TrackedActor.on_start>` callback
        will be invoked.

        Args:
            cls: Actor class to schedule.
            kwargs: Keyword arguments to pass to actor class on construction.
            resource_request: Resources required to start the actor.

        Returns:
            Tracked actor object to reference actor in subsequent API calls.

        """
        raise NotImplementedError

    def remove_actor(
        self,
        tracked_actor: TrackedActor,
        resolve_futures: bool = True,
        kill: bool = False,
    ) -> None:
        """Remove a tracked actor.

        If the actor has already been started, this will stop the actor. This will
        trigger the :meth:`TrackedActor.on_stop
        <ray.air.execution.tracked_actor.TrackedActor.on_stop>` callback once the
        actor stopped.

        If the actor has only been requested, but not started, yet, this will cancel
        the actor request. This will not trigger any callback.

        If ``resolve_futures=True``, this will cache the actor removal and only
        remove it once all its tracked futures are resolved.

        If ``kill=True``, this will use ``ray.kill()`` to forcefully terminate the
        actor. Otherwise, graceful actor deconstruction will be scheduled after
        all currently tracked futures are resolved.

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
            tracked_actor: Tracked actor object.
        """
        raise NotImplementedError

    def get_actor_resources(
        self, tracked_actor: TrackedActor
    ) -> Optional[AcquiredResources]:
        """Returns the acquired resources of an actor that has been started.

        This will return ``None`` if the actor has not been started, yet.

        Args:
            tracked_actor: Tracked actor object.
        """
        raise NotImplementedError

    def schedule_task(
        self,
        remote_fn: Callable,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ) -> TrackedTask:
        """Schedule and track a Ray task.

        This method will schedule a Ray task and return a
        :ref:`TrackedTask <ray.air.execution.tracked_task.TrackedTask>` object.

        The ``TrackedTask`` object can be used to specify callbacks that are invoked
        when the task resolves.

        Args:
            remote_fn: Remote function to schedule and track. This should be a
                Ray function, e.g. decorated using ``ray.remote()``.
            args: Arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.

        """
        raise NotImplementedError

    def schedule_actor_task(
        self,
        tracked_actor: TrackedActor,
        method_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ) -> TrackedActorTask:
        """Schedule and track a task on an actor.

        This method will schedule a remote task ``method_name`` on the
        ``tracked_actor`` and return a
        :ref:`TrackedActorTask
        <ray.air.execution.tracked_actor_task.TrackedActorTask>` object.

        The ``TrackedActorTask`` object can be used to specify callbacks that are
        invoked when the task resolves, errors, or times out.

        Args:
            tracked_actor: Actor to schedule task on.
            method_name: Remote method name to invoke on the actor. If this is
                e.g. ``foo``, then ``actor.foo.remote(*args, **kwargs)`` will be
                scheduled.
            args: Arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.

        """
        raise NotImplementedError

    def schedule_actor_tasks(
        self,
        tracked_actors: List[TrackedActor],
        method_name: str,
        args: Optional[Union[Tuple, List[Tuple]]] = None,
        kwargs: Optional[Union[Dict, List[Dict]]] = None,
    ) -> TrackedActorTaskCollection:
        """Schedule and track tasks on a list of actors.

        This method will schedule a remote task ``method_name`` on all
        ``tracked_actors`` and return a
        :ref:`TrackedActorTaskCollection
        <ray.air.execution.tracked_actor_task.TrackedActorTaskCollection>`
        object.

        The ``TrackedActorTaskCollection`` object can be used to specify callbacks that
        are invoked when the actor tasks resolve, error, or time out.

        ``args`` and ``kwargs`` can be a single tuple/dict, in which case the same
        (keyword) arguments are passed to all actors. If a list is passed instead,
        they are mapped to the respective actors. In that case, the list of
        (keyword) arguments must be the same length as the list of actors.

        Args:
            tracked_actors: List of actors to schedule tasks on.
            method_name: Remote actor method to invoke on the actors. If this is
                e.g. ``foo``, then ``actor.foo.remote(*args, **kwargs)`` will be
                scheduled on all actors.
            args: Arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.

        """
        raise NotImplementedError
