import logging
import random
import time
import uuid
from collections import Counter, defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

import ray
from ray.air.execution._internal.event_manager import RayEventManager
from ray.air.execution._internal.tracked_actor import TrackedActor
from ray.air.execution._internal.tracked_actor_task import TrackedActorTask
from ray.air.execution.resources import (
    AcquiredResources,
    ResourceManager,
    ResourceRequest,
)
from ray.exceptions import RayActorError, RayTaskError

logger = logging.getLogger(__name__)


class RayActorManager:
    """Management class for Ray actors and actor tasks.

    This class provides an event-based management interface for actors, and
    actor tasks.

    The manager can be used to start actors, stop actors, and schedule and
    track task futures on these actors.
    The manager will then invoke callbacks related to the tracked entities.

    For instance, when an actor is added with
    :meth:`add_actor() <RayActorManager.add_actor>`,
    a :ref:`TrackedActor <ray.air.execution._internal.tracked_actor.TrackedActor`
    object is returned. An ``on_start`` callback can be specified that is invoked
    once the actor successfully started. Similarly, ``on_stop`` and ``on_error``
    can be used to specify callbacks relating to the graceful or ungraceful
    end of an actor's lifetime.

    When scheduling an actor task using
    :meth:`schedule_actor_task()
    <ray.air.execution._internal.actor_manager.RayActorManager.schedule_actor_task>`,
    an ``on_result`` callback can be specified that is invoked when the task
    successfully resolves, and an ``on_error`` callback will resolve when the
    task fails.

    The RayActorManager does not implement any true asynchronous processing. Control
    has to be explicitly yielded to the event manager via :meth:`RayActorManager.next`.
    Callbacks will only be invoked when control is with the RayActorManager, and
    callbacks will always be executed sequentially in order of arriving events.

    Args:
        resource_manager: Resource manager used to request resources for the actors.

    Example:

        .. code-block:: python

            from ray.air.execution import ResourceRequest
            from ray.air.execution._internal import RayActorManager

            actor_manager = RayActorManager()

            # Request an actor
            tracked_actor = actor_manager.add_actor(
                ActorClass,
                kwargs={},
                resource_request=ResourceRequest([{"CPU": 1}]),
                on_start=actor_start_callback,
                on_stop=actor_stop_callback,
                on_error=actor_error_callback
            )

            # Yield control to event manager to start actor
            actor_manager.next()

            # Start task on the actor (ActorClass.foo.remote())
            tracked_actor_task = actor_manager.schedule_actor_task(
                tracked_actor,
                method_name="foo",
                on_result=task_result_callback,
                on_error=task_error_callback
            )

            # Again yield control to event manager to process task futures
            actor_manager.wait()

    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        self._actor_state_events = RayEventManager()
        self._actor_task_events = RayEventManager()

        # ---
        # Tracked actor futures.

        # This maps TrackedActor objects to their futures. We use this to see if an
        # actor has any futures scheduled and to remove them when we terminate an actor.

        # Actors to actor task futures
        self._tracked_actors_to_task_futures: Dict[
            TrackedActor, Set[ray.ObjectRef]
        ] = defaultdict(set)

        # Actors to actor state futures (start/terminate)
        self._tracked_actors_to_state_futures: Dict[
            TrackedActor, Set[ray.ObjectRef]
        ] = defaultdict(set)

        # ---
        # Pending actors.
        # We use three dicts for actors that are requested but not yet started.

        # This dict keeps a list of actors associated with each resource request.
        # We use this to start actors in the correct order when their resources
        # become available.
        self._resource_request_to_pending_actors: Dict[
            ResourceRequest, List[TrackedActor]
        ] = defaultdict(list)

        # This dict stores the actor class, kwargs, and resource request of
        # pending actors. Once the resources are available, we start the remote
        # actor class with its args. We need the resource request to cancel it
        # if needed.
        self._pending_actors_to_attrs: Dict[
            TrackedActor, Tuple[Type, Dict[str, Any], ResourceRequest]
        ] = {}

        # This dict keeps track of cached actor tasks. We can't schedule actor
        # tasks before the actor is actually scheduled/live. So when the caller
        # tries to schedule a task, we cache it here, and schedule it once the
        # actor is started.
        self._pending_actors_to_enqueued_actor_tasks: Dict[
            TrackedActor, List[Tuple[TrackedActorTask, str, Tuple[Any], Dict[str, Any]]]
        ] = defaultdict(list)

        # ---
        # Live actors.
        # We keep one dict for actors that are currently running and a set of
        # actors that we should forcefully kill.

        # This dict associates the TrackedActor object with the Ray actor handle
        # and the resources associated to the actor. We use it to schedule the
        # actual ray tasks, and to return the resources when the actor stopped.
        self._live_actors_to_ray_actors_resources: Dict[
            TrackedActor, Tuple[ray.actor.ActorHandle, AcquiredResources]
        ] = {}
        self._live_resource_cache: Optional[Dict[str, Any]] = None

        # This dict contains all actors that should be killed (after calling
        # `remove_actor()`). Kill requests will be handled in wait().
        self._live_actors_to_kill: Set[TrackedActor] = set()

        # Track failed actors
        self._failed_actor_ids: Set[int] = set()

    def next(self, timeout: Optional[Union[int, float]] = None) -> bool:
        """Yield control to event manager to await the next event and invoke callbacks.

        Calling this method will wait for up to ``timeout`` seconds for the next
        event to arrive.

        When events arrive, callbacks relating to the events will be
        invoked. A timeout of ``None`` will block until the next event arrives.

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
            timeout: Timeout in seconds to wait for next event.

        Returns:
            True if at least one event was processed.

        """
        # First issue any pending forceful actor kills
        actor_killed = self._try_kill_actor()

        # We always try to start actors as this won't trigger an event callback
        self._try_start_actors()

        # If an actor was killed, this was our event, and we return.
        if actor_killed:
            return True

        # Otherwise, collect all futures and await the next.
        resource_futures = self._resource_manager.get_resource_futures()
        actor_state_futures = self._actor_state_events.get_futures()
        actor_task_futures = self._actor_task_events.get_futures()

        # Shuffle state futures
        shuffled_state_futures = list(actor_state_futures)
        random.shuffle(shuffled_state_futures)

        # Shuffle task futures
        shuffled_task_futures = list(actor_task_futures)
        random.shuffle(shuffled_task_futures)

        # Prioritize resource futures over actor state over task futures
        all_futures = resource_futures + shuffled_state_futures + shuffled_task_futures

        start_wait = time.monotonic()
        ready, _ = ray.wait(all_futures, num_returns=1, timeout=timeout)

        if not ready:
            return False

        [future] = ready

        if future in actor_state_futures:
            self._actor_state_events.resolve_future(future)
        elif future in actor_task_futures:
            self._actor_task_events.resolve_future(future)
        else:
            self._handle_ready_resource_future()
            # Ready resource futures don't count as one event as they don't trigger
            # any callbacks. So we repeat until we hit anything that is not a resource
            # future.
            time_taken = time.monotonic() - start_wait
            return self.next(
                timeout=max(1e-9, timeout - time_taken) if timeout is not None else None
            )

        self._try_start_actors()
        return True

    def _actor_start_resolved(self, tracked_actor: TrackedActor, future: ray.ObjectRef):
        """Callback to be invoked when actor started"""
        self._tracked_actors_to_state_futures[tracked_actor].remove(future)

        if tracked_actor._on_start:
            tracked_actor._on_start(tracked_actor)

    def _actor_stop_resolved(self, tracked_actor: TrackedActor):
        """Callback to be invoked when actor stopped"""
        self._cleanup_actor(tracked_actor=tracked_actor)

        if tracked_actor._on_stop:
            tracked_actor._on_stop(tracked_actor)

    def _actor_start_failed(self, tracked_actor: TrackedActor, exception: Exception):
        """Callback to be invoked when actor start/stop failed"""
        self._failed_actor_ids.add(tracked_actor.actor_id)

        self._cleanup_actor(tracked_actor=tracked_actor)

        if tracked_actor._on_error:
            tracked_actor._on_error(tracked_actor, exception)

    def _actor_task_failed(
        self, tracked_actor_task: TrackedActorTask, exception: Exception
    ):
        """Handle an actor task future that became ready.

        - On actor error, trigger actor error callback AND error task error callback
        - On task error, trigger actor task error callback
        - On success, trigger actor task result callback
        """
        tracked_actor = tracked_actor_task._tracked_actor

        if isinstance(exception, RayActorError):
            self._failed_actor_ids.add(tracked_actor.actor_id)

            # Clean up any references to the actor and its futures
            self._cleanup_actor(tracked_actor=tracked_actor)

            # Handle actor state callbacks
            if tracked_actor._on_error:
                tracked_actor._on_error(tracked_actor, exception)

            # Then trigger actor task error callback
            if tracked_actor_task._on_error:
                tracked_actor_task._on_error(tracked_actor, exception)

        elif isinstance(exception, RayTaskError):
            # Otherwise only the task failed. Invoke callback
            if tracked_actor_task._on_error:
                tracked_actor_task._on_error(tracked_actor, exception)
        else:
            raise RuntimeError(
                f"Caught unexpected exception: {exception}"
            ) from exception

    def _actor_task_resolved(self, tracked_actor_task: TrackedActorTask, result: Any):
        tracked_actor = tracked_actor_task._tracked_actor

        # Trigger actor task result callback
        if tracked_actor_task._on_result:
            tracked_actor_task._on_result(tracked_actor, result)

    def _handle_ready_resource_future(self):
        """Handle a resource future that became ready.

        - Update state of the resource manager
        - Try to start one actor
        """
        # Force resource manager to update internal state
        self._resource_manager.update_state()
        # We handle resource futures one by one, so only try to start 1 actor at a time
        self._try_start_actors(max_actors=1)

    def _try_start_actors(self, max_actors: Optional[int] = None) -> int:
        """Try to start up to ``max_actors`` actors.

        This function will iterate through all resource requests we collected for
        pending actors. As long as a resource request can be fulfilled (resources
        are available), we try to start as many actors as possible.

        This will schedule a `Actor.__ray_ready__()` future which, once resolved,
        will trigger the `TrackedActor.on_start` callback.
        """
        started_actors = 0

        # Iterate through all resource requests
        for resource_request in self._resource_request_to_pending_actors:
            if max_actors is not None and started_actors >= max_actors:
                break

            # While we have resources ready and there are actors left to schedule
            while (
                self._resource_manager.has_resources_ready(resource_request)
                and self._resource_request_to_pending_actors[resource_request]
            ):
                # Acquire resources for actor
                acquired_resources = self._resource_manager.acquire_resources(
                    resource_request
                )
                assert acquired_resources

                # Get tracked actor to start
                candidate_actors = self._resource_request_to_pending_actors[
                    resource_request
                ]
                assert candidate_actors

                tracked_actor = candidate_actors.pop(0)

                # Get actor class and arguments
                actor_cls, kwargs, _ = self._pending_actors_to_attrs.pop(tracked_actor)

                if not isinstance(actor_cls, ray.actor.ActorClass):
                    actor_cls = ray.remote(actor_cls)

                # Associate to acquired resources
                [remote_actor_cls] = acquired_resources.annotate_remote_entities(
                    [actor_cls]
                )

                # Start Ray actor
                actor = remote_actor_cls.remote(**kwargs)

                # Track
                self._live_actors_to_ray_actors_resources[tracked_actor] = (
                    actor,
                    acquired_resources,
                )
                self._live_resource_cache = None

                # Schedule ready future
                future = actor.__ray_ready__.remote()

                self._tracked_actors_to_state_futures[tracked_actor].add(future)

                # We need to create the callbacks in a function so tracked_actors
                # are captured correctly.
                def create_callbacks(
                    tracked_actor: TrackedActor, future: ray.ObjectRef
                ):
                    def on_actor_start(result: Any):
                        self._actor_start_resolved(
                            tracked_actor=tracked_actor, future=future
                        )

                    def on_error(exception: Exception):
                        self._actor_start_failed(
                            tracked_actor=tracked_actor, exception=exception
                        )

                    return on_actor_start, on_error

                on_actor_start, on_error = create_callbacks(
                    tracked_actor=tracked_actor, future=future
                )

                self._actor_state_events.track_future(
                    future=future,
                    on_result=on_actor_start,
                    on_error=on_error,
                )

                self._enqueue_cached_actor_tasks(tracked_actor=tracked_actor)

                started_actors += 1

        return started_actors

    def _enqueue_cached_actor_tasks(self, tracked_actor: TrackedActor):
        assert tracked_actor in self._live_actors_to_ray_actors_resources

        # Enqueue cached futures
        cached_tasks = self._pending_actors_to_enqueued_actor_tasks.pop(
            tracked_actor, []
        )
        for tracked_actor_task, method_name, args, kwargs in cached_tasks:
            self._schedule_tracked_actor_task(
                tracked_actor_task=tracked_actor_task,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
            )

    def _try_kill_actor(self) -> bool:
        """Try to kill actor scheduled for termination."""
        if not self._live_actors_to_kill:
            return False

        tracked_actor = self._live_actors_to_kill.pop()

        # Remove from tracked actors
        (
            ray_actor,
            acquired_resources,
        ) = self._live_actors_to_ray_actors_resources[tracked_actor]

        # Hard kill if requested
        ray.kill(ray_actor)

        self._cleanup_actor_futures(tracked_actor)

        self._actor_stop_resolved(tracked_actor)

        return True

    def _cleanup_actor(self, tracked_actor: TrackedActor):
        self._cleanup_actor_futures(tracked_actor)

        # Remove from tracked actors
        (
            ray_actor,
            acquired_resources,
        ) = self._live_actors_to_ray_actors_resources.pop(tracked_actor)
        self._live_resource_cache = None

        # Return resources
        self._resource_manager.free_resources(acquired_resource=acquired_resources)

    @property
    def all_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects managed by this manager instance."""
        return self.live_actors + self.pending_actors

    @property
    def live_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects that are currently alive."""
        return list(self._live_actors_to_ray_actors_resources)

    @property
    def pending_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects that are currently pending."""
        return list(self._pending_actors_to_attrs)

    @property
    def num_live_actors(self):
        """Return number of started actors."""
        return len(self.live_actors)

    @property
    def num_pending_actors(self) -> int:
        """Return number of pending (not yet started) actors."""
        return len(self.pending_actors)

    @property
    def num_total_actors(self):
        """Return number of total actors."""
        return len(self.all_actors)

    @property
    def num_actor_tasks(self):
        """Return number of pending tasks"""
        return self._actor_task_events.num_futures

    def get_live_actors_resources(self):
        if self._live_resource_cache:
            return self._live_resource_cache

        counter = Counter()
        for _, acq in self._live_actors_to_ray_actors_resources.values():
            for bdl in acq.resource_request.bundles:
                counter.update(bdl)
        self._live_resource_cache = dict(counter)
        return self._live_resource_cache

    def add_actor(
        self,
        cls: Union[Type, ray.actor.ActorClass],
        kwargs: Dict[str, Any],
        resource_request: ResourceRequest,
        *,
        on_start: Optional[Callable[[TrackedActor], None]] = None,
        on_stop: Optional[Callable[[TrackedActor], None]] = None,
        on_error: Optional[Callable[[TrackedActor, Exception], None]] = None,
    ) -> TrackedActor:
        """Add an actor to be tracked.

        This method will request resources to start the actor. Once the resources
        are available, the actor will be started and the
        :meth:`TrackedActor.on_start
        <ray.air.execution._internal.tracked_actor.TrackedActor.on_start>` callback
        will be invoked.

        Args:
            cls: Actor class to schedule.
            kwargs: Keyword arguments to pass to actor class on construction.
            resource_request: Resources required to start the actor.
            on_start: Callback to invoke when the actor started.
            on_stop: Callback to invoke when the actor stopped.
            on_error: Callback to invoke when the actor failed.

        Returns:
            Tracked actor object to reference actor in subsequent API calls.

        """
        tracked_actor = TrackedActor(
            uuid.uuid4().int, on_start=on_start, on_stop=on_stop, on_error=on_error
        )

        self._pending_actors_to_attrs[tracked_actor] = cls, kwargs, resource_request
        self._resource_request_to_pending_actors[resource_request].append(tracked_actor)

        self._resource_manager.request_resources(resource_request=resource_request)

        return tracked_actor

    def remove_actor(
        self,
        tracked_actor: TrackedActor,
        kill: bool = False,
        stop_future: Optional[ray.ObjectRef] = None,
    ) -> bool:
        """Remove a tracked actor.

        If the actor has already been started, this will stop the actor. This will
        trigger the :meth:`TrackedActor.on_stop
        <ray.air.execution._internal.tracked_actor.TrackedActor.on_stop>`
        callback once the actor stopped.

        If the actor has only been requested, but not started, yet, this will cancel
        the actor request. This will not trigger any callback.

        If ``kill=True``, this will use ``ray.kill()`` to forcefully terminate the
        actor. Otherwise, graceful actor deconstruction will be scheduled after
        all currently tracked futures are resolved.

        This method returns a boolean, indicating if a stop future is tracked and
        the ``on_stop`` callback will be invoked. If the actor has been alive,
        this will be ``True``. If the actor hasn't been scheduled, yet, or failed
        (and triggered the ``on_error`` callback), this will be ``False``.

        Args:
            tracked_actor: Tracked actor to be removed.
            kill: If set, will forcefully terminate the actor instead of gracefully
                scheduling termination.
            stop_future: If set, use this future to track actor termination.
                Otherwise, schedule a ``__ray_terminate__`` future.

        Returns:
            Boolean indicating if the actor was previously alive, and thus whether
            a callback will be invoked once it is terminated.

        """
        if tracked_actor.actor_id in self._failed_actor_ids:
            logger.debug(
                f"Tracked actor already failed, no need to remove: {tracked_actor}"
            )
            return False
        elif tracked_actor in self._live_actors_to_ray_actors_resources:
            # Ray actor is running.

            if not kill:
                # Schedule __ray_terminate__ future
                ray_actor, _ = self._live_actors_to_ray_actors_resources[tracked_actor]

                # Clear state futures here to avoid resolving __ray_ready__ futures
                for future in list(
                    self._tracked_actors_to_state_futures[tracked_actor]
                ):
                    self._actor_state_events.discard_future(future)
                    self._tracked_actors_to_state_futures[tracked_actor].remove(future)

                    # If the __ray_ready__ future hasn't resolved yet, but we already
                    # scheduled the actor via Actor.remote(), we just want to stop
                    # it but not trigger any callbacks. This is in accordance with
                    # the contract defined in the docstring.
                    tracked_actor._on_start = None
                    tracked_actor._on_stop = None
                    tracked_actor._on_error = None

                def on_actor_stop(*args, **kwargs):
                    self._actor_stop_resolved(tracked_actor=tracked_actor)

                if stop_future:
                    # If the stop future was schedule via the actor manager,
                    # discard (track it as state future instead).
                    self._actor_task_events.discard_future(stop_future)
                else:
                    stop_future = ray_actor.__ray_terminate__.remote()

                self._actor_state_events.track_future(
                    future=stop_future,
                    on_result=on_actor_stop,
                    on_error=on_actor_stop,
                )

                self._tracked_actors_to_state_futures[tracked_actor].add(stop_future)
            else:
                # kill = True
                self._live_actors_to_kill.add(tracked_actor)

            return True

        elif tracked_actor in self._pending_actors_to_attrs:
            # Actor is pending, stop
            _, _, resource_request = self._pending_actors_to_attrs.pop(tracked_actor)
            self._resource_request_to_pending_actors[resource_request].remove(
                tracked_actor
            )
            self._resource_manager.cancel_resource_request(
                resource_request=resource_request
            )
            return False
        else:
            raise ValueError(f"Unknown tracked actor: {tracked_actor}")

    def is_actor_started(self, tracked_actor: TrackedActor) -> bool:
        """Returns True if the actor has been started.

        Args:
            tracked_actor: Tracked actor object.
        """
        return (
            tracked_actor in self._live_actors_to_ray_actors_resources
            and tracked_actor.actor_id not in self._failed_actor_ids
        )

    def is_actor_failed(self, tracked_actor: TrackedActor) -> bool:
        return tracked_actor.actor_id in self._failed_actor_ids

    def get_actor_resources(
        self, tracked_actor: TrackedActor
    ) -> Optional[AcquiredResources]:
        """Returns the acquired resources of an actor that has been started.

        This will return ``None`` if the actor has not been started, yet.

        Args:
            tracked_actor: Tracked actor object.
        """
        if not self.is_actor_started(tracked_actor):
            return None

        return self._live_actors_to_ray_actors_resources[tracked_actor][1]

    def schedule_actor_task(
        self,
        tracked_actor: TrackedActor,
        method_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        on_result: Optional[Callable[[TrackedActor, Any], None]] = None,
        on_error: Optional[Callable[[TrackedActor, Exception], None]] = None,
        _return_future: bool = False,
    ) -> Optional[ray.ObjectRef]:
        """Schedule and track a task on an actor.

        This method will schedule a remote task ``method_name`` on the
        ``tracked_actor``.

        This method accepts two optional callbacks that will be invoked when
        their respective events are triggered.

        The ``on_result`` callback is triggered when a task resolves successfully.
        It should accept two arguments: The actor for which the
        task resolved, and the result received from the remote call.

        The ``on_error`` callback is triggered when a task fails.
        It should accept two arguments: The actor for which the
        task threw an error, and the exception.

        Args:
            tracked_actor: Actor to schedule task on.
            method_name: Remote method name to invoke on the actor. If this is
                e.g. ``foo``, then ``actor.foo.remote(*args, **kwargs)`` will be
                scheduled.
            args: Arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.
            on_result: Callback to invoke when the task resolves.
            on_error: Callback to invoke when the task fails.

        Raises:
            ValueError: If the ``tracked_actor`` is not managed by this event manager.

        """
        args = args or tuple()
        kwargs = kwargs or {}

        if tracked_actor.actor_id in self._failed_actor_ids:
            return

        tracked_actor_task = TrackedActorTask(
            tracked_actor=tracked_actor, on_result=on_result, on_error=on_error
        )

        if tracked_actor not in self._live_actors_to_ray_actors_resources:
            # Actor is not started, yet
            if tracked_actor not in self._pending_actors_to_attrs:
                raise ValueError(
                    f"Tracked actor is not managed by this event manager: "
                    f"{tracked_actor}"
                )

            # Cache tasks for future execution
            self._pending_actors_to_enqueued_actor_tasks[tracked_actor].append(
                (tracked_actor_task, method_name, args, kwargs)
            )
        else:
            res = self._schedule_tracked_actor_task(
                tracked_actor_task=tracked_actor_task,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
                _return_future=_return_future,
            )
            if _return_future:
                return res[1]

    def _schedule_tracked_actor_task(
        self,
        tracked_actor_task: TrackedActorTask,
        method_name: str,
        *,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        _return_future: bool = False,
    ) -> Union[TrackedActorTask, Tuple[TrackedActorTask, ray.ObjectRef]]:
        tracked_actor = tracked_actor_task._tracked_actor
        ray_actor, _ = self._live_actors_to_ray_actors_resources[tracked_actor]

        try:
            remote_fn = getattr(ray_actor, method_name)
        except AttributeError as e:
            raise AttributeError(
                f"Remote function `{method_name}()` does not exist for this actor."
            ) from e

        def on_result(result: Any):
            self._actor_task_resolved(
                tracked_actor_task=tracked_actor_task, result=result
            )

        def on_error(exception: Exception):
            self._actor_task_failed(
                tracked_actor_task=tracked_actor_task, exception=exception
            )

        future = remote_fn.remote(*args, **kwargs)

        self._actor_task_events.track_future(
            future=future, on_result=on_result, on_error=on_error
        )

        self._tracked_actors_to_task_futures[tracked_actor].add(future)

        if _return_future:
            return tracked_actor_task, future

        return tracked_actor_task

    def schedule_actor_tasks(
        self,
        tracked_actors: List[TrackedActor],
        method_name: str,
        *,
        args: Optional[Union[Tuple, List[Tuple]]] = None,
        kwargs: Optional[Union[Dict, List[Dict]]] = None,
        on_result: Optional[Callable[[TrackedActor, Any], None]] = None,
        on_error: Optional[Callable[[TrackedActor, Exception], None]] = None,
    ) -> None:
        """Schedule and track tasks on a list of actors.

        This method will schedule a remote task ``method_name`` on all
        ``tracked_actors``.

        ``args`` and ``kwargs`` can be a single tuple/dict, in which case the same
        (keyword) arguments are passed to all actors. If a list is passed instead,
        they are mapped to the respective actors. In that case, the list of
        (keyword) arguments must be the same length as the list of actors.

        This method accepts two optional callbacks that will be invoked when
        their respective events are triggered.

        The ``on_result`` callback is triggered when a task resolves successfully.
        It should accept two arguments: The actor for which the
        task resolved, and the result received from the remote call.

        The ``on_error`` callback is triggered when a task fails.
        It should accept two arguments: The actor for which the
        task threw an error, and the exception.

        Args:
            tracked_actors: List of actors to schedule tasks on.
            method_name: Remote actor method to invoke on the actors. If this is
                e.g. ``foo``, then ``actor.foo.remote(*args, **kwargs)`` will be
                scheduled on all actors.
            args: Arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.
            on_result: Callback to invoke when the task resolves.
            on_error: Callback to invoke when the task fails.

        """
        if not isinstance(args, List):
            args_list = [args] * len(tracked_actors)
        else:
            if len(tracked_actors) != len(args):
                raise ValueError(
                    f"Length of args must be the same as tracked_actors "
                    f"list. Got `len(kwargs)={len(kwargs)}` and "
                    f"`len(tracked_actors)={len(tracked_actors)}"
                )
            args_list = args

        if not isinstance(kwargs, List):
            kwargs_list = [kwargs] * len(tracked_actors)
        else:
            if len(tracked_actors) != len(kwargs):
                raise ValueError(
                    f"Length of kwargs must be the same as tracked_actors "
                    f"list. Got `len(args)={len(args)}` and "
                    f"`len(tracked_actors)={len(tracked_actors)}"
                )
            kwargs_list = kwargs

        for tracked_actor, args, kwargs in zip(tracked_actors, args_list, kwargs_list):
            self.schedule_actor_task(
                tracked_actor=tracked_actor,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
                on_result=on_result,
                on_error=on_error,
            )

    def clear_actor_task_futures(self, tracked_actor: TrackedActor):
        """Discard all actor task futures from a tracked actor."""
        futures = self._tracked_actors_to_task_futures.pop(tracked_actor, [])
        for future in futures:
            self._actor_task_events.discard_future(future)

    def _cleanup_actor_futures(self, tracked_actor: TrackedActor):
        # Remove all actor task futures
        self.clear_actor_task_futures(tracked_actor=tracked_actor)

        # Remove all actor state futures
        futures = self._tracked_actors_to_state_futures.pop(tracked_actor, [])
        for future in futures:
            self._actor_state_events.discard_future(future)

    def cleanup(self):
        for (
            actor,
            acquired_resources,
        ) in self._live_actors_to_ray_actors_resources.values():
            ray.kill(actor)
            self._resource_manager.free_resources(acquired_resources)

        for (
            resource_request,
            pending_actors,
        ) in self._resource_request_to_pending_actors.items():
            for i in range(len(pending_actors)):
                self._resource_manager.cancel_resource_request(resource_request)

        self._resource_manager.clear()

        self.__init__(resource_manager=self._resource_manager)
