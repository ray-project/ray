import enum
import random
import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

import ray
from ray.air.execution.resources import (
    AcquiredResources,
    ResourceManager,
    ResourceRequest,
)

from ray.air.execution._internal.tracked_actor import TrackedActor
from ray.air.execution._internal.tracked_actor_task import (
    TrackedActorTask,
    TrackedActorTaskCollection,
)
from ray.exceptions import RayTaskError, RayActorError


class EventType(enum.Enum):
    """Event type to specify when yielding control to the :class:`RayEventManager`.

    This enum can be passed to
    :meth:`RayEventManager.wait() <RayEventManager.wait>`
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
    :meth:`add_actor() <RayEventManager.add_actor>`,
    a :ref:`TrackedActor <ray.air.execution._internal.tracked_actor.TrackedActor`
    object is returned.
    The :meth:`TrackedActor.on_start()
    <ray.air.execution._internal.tracked_actor.TrackedActor.on_start>`
    method can then be used to specify a callback that is invoked once the actor
    successfully started. The other callbacks relating to tracked actors
    :meth:`TrackedActor.on_stop()
    <ray.air.execution._internal.tracked_actor.TrackedActor.on_stop>` and
    :meth:`TrackedActor.on_error()
    <ray.air.execution._internal.tracked_actor.TrackedActor.on_error>`

    Similarly, when scheduling an actor task using
    :meth:`schedule_actor_task()
    <ray.air.execution._internal.event_manager.RayEventManager.schedule_actor_task>`,
    a :ref:`TrackedActorTask <TrackedActorTask>`
    object is returned.
    The :meth:`TrackedActorTask.on_result()
    <ray.air.execution._internal.tracked_actor_task.TrackedActorTask.on_result>`
    method can then be used to specify a callback that is invoked when the task
    successfully resolved.
    The :meth:`TrackedActorTask.on_error()
    <ray.air.execution._internal.tracked_actor_task.TrackedActorTask.on_error>`
    method can then be used to specify a callback that is invoked when the task
    fails.

    The RayEventManager does not implement any true asynchronous processing. Control
    has to be explicitly yielded to the event manager via :meth:`RayEventManager.wait`.
    Callbacks will only be invoked when control is with the RayEventManager, and
    callbacks will always be executed sequentially in order of arriving events.

    Args:
        resource_manager: Resource manager used to request resources for the actors.

    Example:

        .. code-block:: python

            from ray.air.execution import ResourceRequest
            from ray.air.execution._internal import EventType, RayEventManager

            event_manager = RayEventManager()

            # Request an actor
            tracked_actor = event_manager.add_actor(
                ActorClass,
                kwargs={},
                resource_request=ResourceRequest([{"CPU": 1}])
            )
            tracked_actor.on_start(actor_start_callback)
            tracked_actor.on_stop(actor_stop_callback)
            tracked_actor.on_fail(actor_fail_callback)

            # Yield control to event manager to start actor
            event_manager.wait(timeout=1)

            # Start task on the actor (ActorClass.foo.remote())
            tracked_actor_task = event_manager.schedule_actor_task(
                tracked_actor,
                method_name="foo"
            )
            tracked_actor_task.on_result(task_result_callback)
            tracked_actor_task.on_error(task_error_callback)

            # Again yield control to event manager to process task futures
            event_manager.wait(event_type=EventType.TASKS)

    """

    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        # ---
        # Tracked actor tasks.
        # We keep two maps here for efficient lookup.

        # This maps actor task futures to the respective TrackedActorTask objects.
        # We use this to trigger callbacks when a future resolves.
        self._futures_to_tracked_actor_tasks: Dict[ray.ObjectRef, TrackedActorTask] = {}

        # This maps TrackedActor objects to their futures. We use this to see if an
        # actor has any futures scheduled and to remove them when we terminate an actor.
        self._tracked_actors_to_futures: Dict[
            TrackedActor, Set[ray.ObjectRef]
        ] = defaultdict(set)

        # ---
        # Pending actors.
        # We use three dicts for actors that are requested but not yet started.

        # This dict keeps a list of actors associated with each resource request.
        # We use this to start actors in the correct order when their resources
        # become available.
        self._resource_request_to_tracked_actors: Dict[
            ResourceRequest, List[TrackedActor]
        ] = defaultdict(list)

        # This dict stores the actor class, kwargs, and resource request of
        # pending actors. Once the resources are available, we start the remote
        # actor class with its args. We need the resource request to cancel it
        # if needed.
        self._tracked_actors_to_attrs: Dict[
            TrackedActor, Tuple[Type, Dict[str, Any], ResourceRequest]
        ] = {}

        # This dict keeps track of cached actor tasks. We can't schedule actor
        # tasks before the actor is actually scheduled/live. So when the caller
        # tries to schedule a task, we cache it here, and schedule it once the
        # actor is started.
        self._tracked_actors_to_pending_actor_tasks: Dict[
            TrackedActor, List[Tuple[TrackedActorTask, str, Tuple[Any], Dict[str, Any]]]
        ] = defaultdict(list)

        # ---
        # Live actors.
        # We keep one dict for actors that are currently running.

        # This dict associates the TrackedActor object with the Ray actor handle
        # and the resources associated to the actor. We use it to schedule the
        # actual ray tasks, and to return the resources when the actor stopped.
        self._tracked_actors_to_ray_actors_resources: Dict[
            TrackedActor, Tuple[ray.actor.ActorHandle, AcquiredResources]
        ] = {}

        # ---
        # Actors pending termination.
        # When requesting an actor to be removed, we cache the request here to
        # resolve it on the next call to wait().

        # This dict contains all actors that should be terminated (after calling
        # `remove_actor()`). Termination requests will be handled in wait().
        # The value is a tuple of (resolve_futures, kill, stop_future).
        self._tracked_actors_to_terminate: Dict[
            TrackedActor, Tuple[bool, bool, Optional[ray.ObjectRef]]
        ] = {}

        # This dict tracks the termination futures (__ray_terminate__.remote()) for
        # actors. Once resolved, we remove all references to the actor.
        self._futures_to_actor_termination: Dict[ray.ObjectRef, TrackedActor] = {}

    def wait(
        self,
        num_events: Optional[int] = None,
        timeout: Optional[Union[int, float]] = None,
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
        # This is the main event handler for the event manager. The general
        # flow is:
        # 1. Maybe stop actors
        #    - this will not block but just stop actors that can be immediately stopped
        #    - this is so that resources are freed and actors can potentially be
        #      started in the same call.
        # 2. Maybe start actors
        #    - this will only block for up to `timeout` seconds if there are no
        #      futures to await.
        #    - this is used if we don't have resource futures (e.g. with fixed/budget
        #      based resource management).
        # 3. Await any futures
        #    - Here we wait for task futures, resource futures, and actor termination
        #      futures.
        #    - React to futures (start actors, stop actors, invoke callbacks)
        # 4. Maybe start actors again.
        #    - this will not block, but just start actors that can be immediately
        #      started
        #    - we do this because actor terminations in step 3 could have freed
        #      more resources, that can now be used to start actors.

        # We keep track of the number of events processed and will only proceed
        # if we don't exceed `num_events`.

        # We count the number of started and stopped actors. These count as events
        # and we use the counters to exit the loop if `max_events` is reached.
        started_actors = 0
        stopped_actors = 0

        # We collect different kinds of futures, that we pass to ray.wait later.
        resource_futures = []
        stop_futures = []
        all_task_futures = []

        # ---
        # Handle ACTOR events (only for EventType.ACTORS and EventType.ALL).
        if event_type in [EventType.ALL, EventType.ACTORS]:
            # We require a timeout because otherwise we'll run into a deadlock
            # if we request more actors than the cluster can start.
            if timeout is None:
                raise ValueError(
                    f"When awaiting ACTOR events, `timeout` cannot be None. This is "
                    f"to ensure we don't run into a deadlock if not enough resources "
                    f"are available to start actors. Got `event_type={event_type}` and "
                    f"`timeout={timeout}`."
                )

            # First, try to stop some actors. This will not block, but processing
            # the on_stop callbacks may take some time.
            stopped_actors = self._try_stop_actors(max_actors=num_events)

            # Get the termination futures we will later await
            stop_futures = list(self._futures_to_actor_termination.keys())

            # Then, get the resource futures (to start actors)
            resource_futures = self._resource_manager.get_resource_futures()

            # If there are no resource futures but pending actors, we should try
            # to start some actors. This is because some resource backends do not
            # use resource futures.
            if not resource_futures and self._tracked_actors_to_attrs:
                # If we have some stopped actors and `num_events` is set,
                # make sure to subtract this so that we don't emit more events than
                # specified.
                if stopped_actors > 0 and num_events:
                    max_events_left = num_events - stopped_actors
                else:
                    max_events_left = num_events

                # Try to start some actors for up to `timeout` seconds.
                # This will only happen if there are no other futures to await.
                # This is to make sure that resources that become free again can
                # be used if we use a resource backend without resource futures.
                # We will then subtract the time this took from the timeout we pass
                # to `ray.wait()` below. This will work because tasks are executed
                # asynchronously, so `ray.wait(..., timeout=5)` and
                # `time.sleep(5); ray.wait(..., timeout=0)` should yield the same
                # ready futures.
                started_actors, time_taken = self._try_start_actors_timeout(
                    timeout=timeout, max_actors=max_events_left
                )
                timeout = max(1e-6, timeout - time_taken)

        # ---
        # Handle TASK events (only for EventType.TASKS and EventType.ALL).
        if event_type in [EventType.ALL, EventType.TASKS]:
            # Collect all task futures
            tracked_actor_task_futures = list(
                self._futures_to_tracked_actor_tasks.keys()
            )
            all_task_futures.extend(tracked_actor_task_futures)

        # Stop / resource futures should have priority, but task futures should
        # be shuffled.
        random.shuffle(all_task_futures)
        all_futures = stop_futures + resource_futures + all_task_futures

        if num_events:
            # If we have a fixed number of events, subtract the number of
            # started/stopped actors as they count as an event.
            num_returns = num_events - started_actors - stopped_actors
        else:
            # Otherwise, await all futures.
            num_returns = len(all_futures)

        assert num_returns >= 0

        # If we have no events left (e.g. no futures or too many actor events), exit
        if num_returns == 0:
            return

        # We can't wait for more events than we have futures
        if num_returns > len(all_futures):
            if num_returns <= len(self._tracked_actors_to_attrs):
                # In this case, there are no resource futures, but we actually waited
                # for some actors to start (without resource futures). So this is
                # not an error - there's just nothing to actually await. So we return.
                return

            # Otherwise, we specified to wait for more events than we track, which is
            # a usage error.
            raise ValueError(
                f"num_events cannot be greater than the number of events "
                f"currently pending by the RayEventManager. "
                f"Got num_events={num_returns} and {len(all_futures)} tracked events "
                f"for event_type={event_type}."
            )

        # Here we await the futures
        ready, not_ready = ray.wait(
            all_futures, num_returns=num_returns, timeout=timeout
        )

        num_processed_events = len(ready) + stopped_actors + started_actors

        # Process resolved futures
        for future in ready:
            if future in self._futures_to_tracked_actor_tasks:
                # Actor task future
                self._handle_ready_actor_task_future(future)
            elif future in self._futures_to_actor_termination:
                # Termination future resolved
                tracked_actor = self._futures_to_actor_termination[future]
                self._try_stop_tracked_actor(tracked_actor)
            else:
                # Resource future
                self._handle_ready_resource_future()

        # Lastly, if we have events left, try to start actors again (some resources
        # might have been freed by our previous stop events).
        if event_type in [EventType.ALL, EventType.ACTORS]:
            if num_processed_events > 0 and num_events:
                events_left = num_events - num_processed_events
            else:
                events_left = num_events

            self._try_start_actors(max_actors=events_left)

    def _handle_ready_actor_task_future(self, future: ray.ObjectRef):
        """Handle an actor task future that became ready.

        - Resolve future
        - On actor error, trigger actor error callback AND error task error callback
        - On task error, trigger actor task error callback
        - On success, trigger actor task result callback
        """
        tracked_actor_task = self._futures_to_tracked_actor_tasks.pop(future)
        tracked_actor = tracked_actor_task._tracked_actor
        self._tracked_actors_to_futures[tracked_actor].remove(future)

        try:
            result = ray.get(future)
        except RayActorError as exc:
            # Here the actual actor process died.
            # First, clean up any references to the actor and its futures
            self._cleanup_failed_actor(tracked_actor)

            # Actor failed - first trigger actor error callback
            if tracked_actor._on_error:
                tracked_actor._on_error(tracked_actor, exc)

            # Then trigger actor task error callback
            if tracked_actor_task._on_error:
                tracked_actor_task._on_error(tracked_actor, exc)
            return
        except RayTaskError as exc:
            # Here, only the task failed, but the actor process is still alive.
            # Trigger actor task error callback
            if tracked_actor_task._on_error:
                tracked_actor_task._on_error(tracked_actor, exc)
            return

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

    def _try_start_actors_timeout(
        self, timeout: Union[int, float], max_actors: Optional[int] = None
    ) -> Tuple[int, float]:
        """Try starting up to ``max_actors`` actors in up to ``timeout`` seconds."""
        now = start_time = time.monotonic()

        started_actors = 0
        while (
            now < start_time + timeout
            and self._tracked_actors_to_attrs
            and not (
                # If we have other futures to await, prefer that
                self._futures_to_tracked_actor_tasks
                or self._futures_to_actor_termination
            )
        ):
            started_this_iter = self._try_start_actors(max_actors=max_actors)
            started_actors += started_this_iter

            if max_actors is not None:
                max_actors -= started_this_iter
                if max_actors <= 0:
                    break

            time.sleep(0.1)
            now = time.monotonic()

        return started_actors, now - start_time

    def _try_start_actors(self, max_actors: Optional[int] = None) -> int:
        """Try to start up to ``max_actors`` actors.

        This function will iterate through all resource requests we collected for
        pending actors. As long as a resource request can be fulfilled (resources
        are available), we try to start as many actors as possible.

        This will also trigger the `TrackedActor.on_start` callback for every
        started actor, even though the actor will be started by Ray asynchronously.
        This is because we can't generically await actor scheduling state.
        """
        started_actors = 0

        # Iterate through all resource requests
        for resource_request in self._resource_request_to_tracked_actors:
            if max_actors and started_actors >= max_actors:
                break

            # While we have resources ready and there are actors left to schedule
            while (
                self._resource_manager.has_resources_ready(resource_request)
                and self._resource_request_to_tracked_actors[resource_request]
            ):
                # Acquire resources for actor
                acquired_resources = self._resource_manager.acquire_resources(
                    resource_request
                )
                assert acquired_resources

                # Get tracked actor to start
                candidate_actors = self._resource_request_to_tracked_actors[
                    resource_request
                ]
                assert candidate_actors

                tracked_actor = candidate_actors.pop(0)

                # Get actor class and arguments
                actor_cls, kwargs, _ = self._tracked_actors_to_attrs.pop(tracked_actor)

                if not isinstance(actor_cls, ray.actor.ActorClass):
                    actor_cls = ray.remote(actor_cls)

                # Associate to acquired resources
                [remote_actor_cls] = acquired_resources.annotate_remote_entities(
                    [actor_cls]
                )

                # Start Ray actor
                actor = remote_actor_cls.remote(**kwargs)

                self._tracked_actors_to_ray_actors_resources[tracked_actor] = (
                    actor,
                    acquired_resources,
                )

                # Enqueue cached futures
                cached_tasks = self._tracked_actors_to_pending_actor_tasks.pop(
                    tracked_actor, []
                )
                for tracked_actor_task, method_name, args, kwargs in cached_tasks:
                    self._schedule_tracked_actor_task(
                        tracked_actor_task=tracked_actor_task,
                        method_name=method_name,
                        args=args,
                        kwargs=kwargs,
                    )

                # Trigger on_start callback
                if tracked_actor._on_start:
                    tracked_actor._on_start(tracked_actor)

                started_actors += 1

        return started_actors

    def _try_stop_actors(self, max_actors: Optional[int] = None) -> int:
        """Try to stop up to ``max_actors`` actors."""
        stopped_actors = 0

        for tracked_actor in list(self._tracked_actors_to_terminate):
            if max_actors and stopped_actors >= max_actors:
                break

            if self._try_stop_tracked_actor(tracked_actor):
                stopped_actors += 1

        return stopped_actors

    def _try_stop_tracked_actor(self, tracked_actor: TrackedActor) -> bool:
        """Try to stop specific tracked actor.

        If futures should be resolved, wait until they are resolved and defer
        termination until no more future exist.

        If we scheduled graceful termination, make sure the termination future
        resolved. If it didn't, defer until it resolves.

        Otherwise, we can terminate. If a hard kill was requested, call
        ``ray.kill()``. Otherwise, we assume the actor has been gracefully terminated
        and any remaining state will be garbage collected by Ray when removing the
        reference.

        We then return the resources to the resource manager.
        """
        resolve_futures, kill, stop_future = self._tracked_actors_to_terminate[
            tracked_actor
        ]

        # If we should resolve existing futures first, continue
        if resolve_futures and self._tracked_actors_to_futures[tracked_actor]:
            return False

        # If we should not resolve existing futures, cancel tasks
        for future in self._tracked_actors_to_futures[tracked_actor]:
            ray.cancel(future)
            self._futures_to_tracked_actor_tasks.pop(future, None)

        self._tracked_actors_to_futures.pop(tracked_actor, None)

        # If we have a graceful termination future, it must resolve
        if stop_future:
            ready, not_ready = ray.wait([stop_future], timeout=0)
            if not_ready:
                # Has not resolved
                return False

            # Remove
            self._futures_to_actor_termination.pop(stop_future, None)

        # Otherwise, terminate now.
        # Remove kill instruction
        self._tracked_actors_to_terminate.pop(tracked_actor)

        # Remove from tracked actors
        (
            ray_actor,
            acquired_resources,
        ) = self._tracked_actors_to_ray_actors_resources.pop(tracked_actor)

        # Hard kill if requested
        if kill:
            ray.kill(ray_actor)

        # Return resources
        self._resource_manager.free_resources(acquired_resource=acquired_resources)

        # Trigger stop callback
        if tracked_actor._on_stop:
            tracked_actor._on_stop(tracked_actor)

        return True

    def _cleanup_failed_actor(self, tracked_actor: TrackedActor):
        # Remove all actor task futures
        futures = self._tracked_actors_to_futures.pop(tracked_actor, [])
        for future in futures:
            self._futures_to_tracked_actor_tasks.pop(future)

        # Remove stop future
        _, _, stop_future = self._tracked_actors_to_terminate.pop(
            tracked_actor, (False, False, None)
        )
        if stop_future:
            self._futures_to_actor_termination.pop(stop_future)

        # Remove from tracked actors
        (
            ray_actor,
            acquired_resources,
        ) = self._tracked_actors_to_ray_actors_resources.pop(tracked_actor)

        # Return resources
        self._resource_manager.free_resources(acquired_resource=acquired_resources)

    @property
    def all_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects managed by this manager instance."""
        return self.live_actors + self.pending_actors

    @property
    def live_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects that are currently alive."""
        return list(self._tracked_actors_to_ray_actors_resources)

    @property
    def pending_actors(self) -> List[TrackedActor]:
        """Return all ``TrackedActor`` objects that are currently pending."""
        return list(self._tracked_actors_to_attrs)

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

    def add_actor(
        self,
        cls: Union[Type, ray.actor.ActorClass],
        kwargs: Dict[str, Any],
        resource_request: ResourceRequest,
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

        Returns:
            Tracked actor object to reference actor in subsequent API calls.

        """
        tracked_actor = TrackedActor(uuid.uuid4().int)

        self._tracked_actors_to_attrs[tracked_actor] = cls, kwargs, resource_request
        self._resource_request_to_tracked_actors[resource_request].append(tracked_actor)

        self._resource_manager.request_resources(resource_request=resource_request)

        return tracked_actor

    def remove_actor(
        self,
        tracked_actor: TrackedActor,
        resolve_futures: bool = True,
        kill: bool = False,
    ) -> None:
        """Remove a tracked actor.

        If the actor has already been started, this will stop the actor. This will
        trigger the :meth:`TrackedActor.on_stop
        <ray.air.execution._internal.tracked_actor.TrackedActor.on_stop>`
        callback once the actor stopped.

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
        if tracked_actor in self._tracked_actors_to_ray_actors_resources:
            # Ray actor is running.

            if not kill:
                # Schedule __ray_terminate__ future
                ray_actor, _ = self._tracked_actors_to_ray_actors_resources[
                    tracked_actor
                ]

                stop_future = ray_actor.__ray_terminate__.remote()
                self._futures_to_actor_termination[stop_future] = tracked_actor
                self._tracked_actors_to_terminate[tracked_actor] = (
                    resolve_futures,
                    kill,
                    stop_future,
                )
            else:
                self._tracked_actors_to_terminate[tracked_actor] = (
                    resolve_futures,
                    kill,
                    None,
                )

        elif tracked_actor in self._tracked_actors_to_attrs:
            # Actor is pending, stop
            _, _, resource_request = self._tracked_actors_to_attrs.pop(tracked_actor)
            self._resource_manager.cancel_resource_request(
                resource_request=resource_request
            )
        else:
            raise ValueError(f"Unknown tracked actor: {tracked_actor}")

    def is_actor_started(self, tracked_actor: TrackedActor) -> bool:
        """Returns True if the actor has been started.

        Args:
            tracked_actor: Tracked actor object.
        """
        return tracked_actor in self._tracked_actors_to_ray_actors_resources

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

        return self._tracked_actors_to_ray_actors_resources[tracked_actor][1]

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
        <ray.air.execution._internal.tracked_actor_task.TrackedActorTask>` object.

        The ``TrackedActorTask`` object can be used to specify callbacks that are
        invoked when the task resolves, errors, or times out.

        Args:
            tracked_actor: Actor to schedule task on.
            method_name: Remote method name to invoke on the actor. If this is
                e.g. ``foo``, then ``actor.foo.remote(*args, **kwargs)`` will be
                scheduled.
            args: Arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.

        Raises:
            ValueError: If the ``tracked_actor`` is not managed by this event manager.

        """
        args = args or tuple()
        kwargs = kwargs or {}

        tracked_actor_task = TrackedActorTask(tracked_actor=tracked_actor)

        if tracked_actor not in self._tracked_actors_to_ray_actors_resources:
            # Actor is not started, yet
            if tracked_actor not in self._tracked_actors_to_attrs:
                raise ValueError(
                    f"Tracked actor is not managed by this event manager: "
                    f"{tracked_actor}"
                )

            # Cache tasks for future execution
            tracked_actor_task = TrackedActorTask(tracked_actor=tracked_actor)
            self._tracked_actors_to_pending_actor_tasks[tracked_actor].append(
                (tracked_actor_task, method_name, args, kwargs)
            )
        else:
            self._schedule_tracked_actor_task(
                tracked_actor_task=tracked_actor_task,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
            )

        return tracked_actor_task

    def _schedule_tracked_actor_task(
        self,
        tracked_actor_task: TrackedActorTask,
        method_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ) -> TrackedActorTask:
        tracked_actor = tracked_actor_task._tracked_actor
        ray_actor, _ = self._tracked_actors_to_ray_actors_resources[tracked_actor]

        try:
            remote_fn = getattr(ray_actor, method_name)
        except AttributeError as e:
            raise AttributeError(
                f"Remote function `{method_name}()` does not exist for this actor."
            ) from e

        future = remote_fn.remote(*args, **kwargs)

        self._futures_to_tracked_actor_tasks[future] = tracked_actor_task
        self._tracked_actors_to_futures[tracked_actor].add(future)

        return tracked_actor_task

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
        <ray.air.execution._internal.tracked_actor_task.TrackedActorTaskCollection>`
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

        tracked_actor_tasks = []
        for tracked_actor, args, kwargs in zip(tracked_actors, args_list, kwargs_list):
            tracked_actor_task = self.schedule_actor_task(
                tracked_actor=tracked_actor,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
            )
            tracked_actor_tasks.append(tracked_actor_task)

        return TrackedActorTaskCollection(tracked_actor_tasks)
