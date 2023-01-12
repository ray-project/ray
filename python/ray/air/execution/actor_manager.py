from numbers import Number
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from ray.air import AcquiredResources
from ray.air.execution.actor_spec import ActorSpec, TrackedActor
from ray.air.execution.resources.resource_manager import ResourceManager


# Tuple of (method_name, args, kwargs)
from ray.air.execution.futures import TrackedFutures

TaskSpec = Tuple[str, Iterable[Any], Dict[str, Any]]


class ActorManager:
    """Management class for actors and actor task futures.

    This class provides an event-based management interface for actors and
    actor task futures.

    The actor manager can be used to start actors, stop actors, and schedule and
    track task futures on these actors. The manager will then yield events related
    to the tracked entities in the form of callbacks.

    For instance, when an actor is added with ``add_actor()``, the callback passed
    to :meth:`on_actor_start` will be invoked.
    Likewise, after calling ``remove_actor()``, the callback passed to
    meth:`on_actor_stop`
    will be invoked once the actor stopped.

    Actor properties are defined via an ``ActorSpec`` object. This object specifies
    the actor class, the keyword arguments used for initialization, and the resources
    required to start the actor.

    Once added, subsequent interaction with the actor via the manager uses an
    ``TrackedActor`` object. This object can be used to remove the actor or to schedule
    futures on it. Existence of this object does not guarantee that the actual Ray actor
    has been scheduled. This status can be inquired from the actor manager using the
    ``TrackedActor`` object.

    After scheduling futures, a :class:`TrackedFutures` object is returned. Again,
    callbacks can be defined to be invoked when these futures
    resolve (:meth:`on_result <TrackedFutures.on_result>`),
    error (:meth:`on_error <TrackedFutures.on_error>`),
    or time out (:meth:`on_timeout <TrackedFutures.on_timeout>`).

    Args:
        resource_manager: Resource manager used to request resources for the actors.

    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        raise NotImplementedError

    def handle(self) -> None:
        """Wait until next event is available and handle callbacks."""
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

    def on_actor_start(self, callback: Callable[[TrackedActor], None]) -> bool:
        """Define callback to handle actor starts.

        This callback is invoked when an actor successfully started.
        """
        raise NotImplementedError

    def on_actor_stop(self, callback: Callable[[TrackedActor], None]) -> bool:
        """Define callback to handle actor stops.

        This callback is invoked when an actor gracefully stopped.
        """
        raise NotImplementedError

    def on_actor_error(self, callback: Callable[[TrackedActor], None]) -> bool:
        """Define callback to handle actor errors.

        This callback is invoked when an actor ungracefully terminated, e.g.
        when the actor process died.
        """
        raise NotImplementedError

    def add_actor(self, actor_spec: ActorSpec) -> TrackedActor:
        """Add an actor to be tracked.

        The actor class to start, the constructor arguments, and the resources to
        start the actor with are provided in the ``actor_spec`` argument.

        This method will request resources to start the actor. Once the resources
        are available, the actor will be started and the callback passed to
        :meth:`on_actor_start` will be invoked.

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

        After stopping the actor, the callback passed to
        :meth:`on_actor_stop` will be invoked.

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

    def schedule_tasks(
        self,
        tracked_actors: Iterable[TrackedActor],
        method_name: str,
        args: Optional[Union[Tuple, List[Tuple]]] = None,
        kwargs: Optional[Union[Dict, List[Dict]]] = None,
        timeout: Optional[Number] = None,
    ) -> TrackedFutures:
        """Schedule and track tasks on a list of actors.

        This method will schedule a remote task ``method_name`` on the supplied
        ``tracked_actors`` and return a
        :ref:`TrackedFuture <ray.air.execution.futures.TrackedFutures>` object.

        The ``TrackedFutures`` object can be used to define callbacks that are invoked
        when the futures resolve.

        ``args`` and ``kwargs`` can be a single tuple/dict, in which case the same
        (keyword) arguments are passed to all actors. If a list is passed instead,
        they are mapped to the respective actors. In that case, the list of
        (keyword) arguments must be the same length as the list of actors.

        Args:
            tracked_actors: List of actors to schedule task on.
            method_name: Remote actor method to invoke on the actors. If this is
                e.g. ``foo``, then ``actor.foo.remote(*args, **kwargs)`` will be
                invoked on all actors.
            args: Arguments to pass to remote method.
            kwargs: Keyword arguments to pass to remote method.
            timeout: Timeout in seconds after which the future is cancelled and the
                ``on_timeout()`` callback is invoked.

        """
        raise NotImplementedError
