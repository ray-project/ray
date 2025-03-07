from collections import defaultdict
import copy
from dataclasses import dataclass
import logging
import sys
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayError, RayTaskError
from ray.rllib.utils.typing import T
from ray.util.annotations import DeveloperAPI


logger = logging.getLogger(__name__)


@DeveloperAPI
class ResultOrError:
    """A wrapper around a result or a RayError thrown during remote task/actor calls.

    This is used to return data from `FaultTolerantActorManager` that allows us to
    distinguish between RayErrors (remote actor related) and valid results.
    """

    def __init__(self, result: Any = None, error: Exception = None):
        """One and only one of result or error should be set.

        Args:
            result: The result of the computation. Note that None is a valid result if
                the remote function does not return anything.
            error: Alternatively, the error that occurred during the computation.
        """
        self._result = result
        self._error = (
            # Easier to handle if we show the user the original error.
            error.as_instanceof_cause()
            if isinstance(error, RayTaskError)
            else error
        )

    @property
    def ok(self):
        return self._error is None

    def get(self):
        """Returns the result or the error."""
        if self._error:
            return self._error
        else:
            return self._result


@DeveloperAPI
@dataclass
class CallResult:
    """Represents a single result from a call to an actor.

    Each CallResult contains the index of the actor that was called
    plus the result or error from the call.
    """

    actor_id: int
    result_or_error: ResultOrError
    tag: str

    @property
    def ok(self):
        """Passes through the ok property from the result_or_error."""
        return self.result_or_error.ok

    def get(self):
        """Passes through the get method from the result_or_error."""
        return self.result_or_error.get()


@DeveloperAPI
class RemoteCallResults:
    """Represents a list of results from calls to a set of actors.

    CallResults provides convenient APIs to iterate over the results
    while skipping errors, etc.

    .. testcode::
        :skipif: True

        manager = FaultTolerantActorManager(
            actors, max_remote_requests_in_flight_per_actor=2,
        )
        results = manager.foreach_actor(lambda w: w.call())

        # Iterate over all results ignoring errors.
        for result in results.ignore_errors():
            print(result.get())
    """

    class _Iterator:
        """An iterator over the results of a remote call."""

        def __init__(self, call_results: List[CallResult]):
            self._call_results = call_results

        def __iter__(self) -> Iterator[CallResult]:
            return self

        def __next__(self) -> CallResult:
            if not self._call_results:
                raise StopIteration
            return self._call_results.pop(0)

    def __init__(self):
        self.result_or_errors: List[CallResult] = []

    def add_result(self, actor_id: int, result_or_error: ResultOrError, tag: str):
        """Add index of a remote actor plus the call result to the list.

        Args:
            actor_id: ID of the remote actor.
            result_or_error: The result or error from the call.
            tag: A description to identify the call.
        """
        self.result_or_errors.append(CallResult(actor_id, result_or_error, tag))

    def __iter__(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results."""
        # Shallow copy the list.
        return self._Iterator(copy.copy(self.result_or_errors))

    def __len__(self) -> int:
        return len(self.result_or_errors)

    def ignore_errors(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results, skipping all errors."""
        return self._Iterator([r for r in self.result_or_errors if r.ok])

    def ignore_ray_errors(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results, skipping only Ray errors.

        Similar to ignore_errors, but only skips Errors raised because of
        remote actor problems (often get restored automatcially).
        This is useful for callers that want to handle application errors differently
        from Ray errors.
        """
        return self._Iterator(
            [r for r in self.result_or_errors if not isinstance(r.get(), RayError)]
        )


@DeveloperAPI
class FaultAwareApply:
    @DeveloperAPI
    def ping(self) -> str:
        """Ping the actor. Can be used as a health check.

        Returns:
            "pong" if actor is up and well.
        """
        return "pong"

    @DeveloperAPI
    def apply(
        self,
        func: Callable[[Any, Optional[Any], Optional[Any]], T],
        *args,
        **kwargs,
    ) -> T:
        """Calls the given function with this Actor instance.

        A generic interface for applying arbitrary member functions on a
        remote actor.

        Args:
            func: The function to call, with this actor as first
                argument, followed by args, and kwargs.
            args: Optional additional args to pass to the function call.
            kwargs: Optional additional kwargs to pass to the function call.

        Returns:
            The return value of the function call.
        """
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            # Actor should be recreated by Ray.
            if self.config.restart_failed_env_runners:
                logger.exception(f"Worker exception caught during `apply()`: {e}")
                # Small delay to allow logs messages to propagate.
                time.sleep(self.config.delay_between_env_runner_restarts_s)
                # Kill this worker so Ray Core can restart it.
                sys.exit(1)
            # Actor should be left dead.
            else:
                raise e


@DeveloperAPI
class FaultTolerantActorManager:
    """A manager that is aware of the healthiness of remote actors.

    .. testcode::

        import time
        import ray
        from ray.rllib.utils.actor_manager import FaultTolerantActorManager

        @ray.remote
        class MyActor:
            def apply(self, fn):
                return fn(self)

            def do_something(self):
                return True

        actors = [MyActor.remote() for _ in range(3)]
        manager = FaultTolerantActorManager(
            actors, max_remote_requests_in_flight_per_actor=2,
        )

        # Synchronous remote calls.
        results = manager.foreach_actor(lambda actor: actor.do_something())
        # Print results ignoring returned errors.
        print([r.get() for r in results.ignore_errors()])

        # Asynchronous remote calls.
        manager.foreach_actor_async(lambda actor: actor.do_something())
        time.sleep(2)  # Wait for the tasks to finish.
        for r in manager.fetch_ready_async_reqs():
            # Handle result and errors.
            if r.ok:
                print(r.get())
            else:
                print("Error: {}".format(r.get()))
    """

    @dataclass
    class _ActorState:
        """State of a single actor."""

        # Num of outstanding async requests for this actor.
        num_in_flight_async_requests: int = 0
        # Whether this actor is in a healthy state.
        is_healthy: bool = True

    def __init__(
        self,
        actors: Optional[List[ActorHandle]] = None,
        max_remote_requests_in_flight_per_actor: int = 2,
        init_id: int = 0,
    ):
        """Construct a FaultTolerantActorManager.

        Args:
            actors: A list of ray remote actors to manage on. These actors must have an
                ``apply`` method which takes a function with only one parameter (the
                actor instance itself).
            max_remote_requests_in_flight_per_actor: The maximum number of remote
                requests that can be in flight per actor. Any requests made to the pool
                that cannot be scheduled because the limit has been reached will be
                dropped. This only applies to the asynchronous remote call mode.
            init_id: The initial ID to use for the next remote actor. Default is 0.
        """
        # For round-robin style async requests, keep track of which actor to send
        # a new func next (current).
        self._next_id = self._current_actor_id = init_id

        # Actors are stored in a map and indexed by a unique (int) ID.
        self._actors: Dict[int, ActorHandle] = {}
        self._remote_actor_states: Dict[int, self._ActorState] = {}
        self._restored_actors = set()
        self.add_actors(actors or [])

        # Maps outstanding async requests to the IDs of the actor IDs that
        # are executing them.
        self._in_flight_req_to_actor_id: Dict[ray.ObjectRef, int] = {}

        self._max_remote_requests_in_flight_per_actor = (
            max_remote_requests_in_flight_per_actor
        )

        # Useful metric.
        self._num_actor_restarts = 0

    @DeveloperAPI
    def actor_ids(self) -> List[int]:
        """Returns a list of all worker IDs (healthy or not)."""
        return list(self._actors.keys())

    @DeveloperAPI
    def healthy_actor_ids(self) -> List[int]:
        """Returns a list of worker IDs that are healthy."""
        return [k for k, v in self._remote_actor_states.items() if v.is_healthy]

    @DeveloperAPI
    def add_actors(self, actors: List[ActorHandle]):
        """Add a list of actors to the pool.

        Args:
            actors: A list of ray remote actors to be added to the pool.
        """
        for actor in actors:
            self._actors[self._next_id] = actor
            self._remote_actor_states[self._next_id] = self._ActorState()
            self._next_id += 1

    @DeveloperAPI
    def remove_actor(self, actor_id: int) -> ActorHandle:
        """Remove an actor from the pool.

        Args:
            actor_id: ID of the actor to remove.

        Returns:
            Handle to the actor that was removed.
        """
        actor = self._actors[actor_id]

        # Remove the actor from the pool.
        del self._actors[actor_id]
        del self._remote_actor_states[actor_id]
        self._restored_actors.discard(actor_id)
        self._remove_async_state(actor_id)

        return actor

    @DeveloperAPI
    def num_actors(self) -> int:
        """Return the total number of actors in the pool."""
        return len(self._actors)

    @DeveloperAPI
    def num_healthy_actors(self) -> int:
        """Return the number of healthy remote actors."""
        return sum(s.is_healthy for s in self._remote_actor_states.values())

    @DeveloperAPI
    def total_num_restarts(self) -> int:
        """Return the number of remote actors that have been restarted."""
        return self._num_actor_restarts

    @DeveloperAPI
    def num_outstanding_async_reqs(self) -> int:
        """Return the number of outstanding async requests."""
        return len(self._in_flight_req_to_actor_id)

    @DeveloperAPI
    def is_actor_healthy(self, actor_id: int) -> bool:
        """Whether a remote actor is in healthy state.

        Args:
            actor_id: ID of the remote actor.

        Returns:
            True if the actor is healthy, False otherwise.
        """
        if actor_id not in self._remote_actor_states:
            raise ValueError(f"Unknown actor id: {actor_id}")
        return self._remote_actor_states[actor_id].is_healthy

    @DeveloperAPI
    def set_actor_state(self, actor_id: int, healthy: bool) -> None:
        """Update activate state for a specific remote actor.

        Args:
            actor_id: ID of the remote actor.
            healthy: Whether the remote actor is healthy.
        """
        if actor_id not in self._remote_actor_states:
            raise ValueError(f"Unknown actor id: {actor_id}")

        was_healthy = self._remote_actor_states[actor_id].is_healthy
        # Set from unhealthy to healthy -> Add to restored set.
        if not was_healthy and healthy:
            self._restored_actors.add(actor_id)
        # Set from healthy to unhealthy -> Remove from restored set.
        elif was_healthy and not healthy:
            self._restored_actors.discard(actor_id)

        self._remote_actor_states[actor_id].is_healthy = healthy

        if not healthy:
            # Remove any async states.
            self._remove_async_state(actor_id)

    @DeveloperAPI
    def clear(self):
        """Clean up managed actors."""
        for actor in self._actors.values():
            ray.kill(actor)
        self._actors.clear()
        self._remote_actor_states.clear()
        self._restored_actors.clear()
        self._in_flight_req_to_actor_id.clear()

    @DeveloperAPI
    def foreach_actor(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]], str, List[str]],
        *,
        kwargs: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        healthy_only: bool = True,
        remote_actor_ids: Optional[List[int]] = None,
        timeout_seconds: Optional[float] = None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> RemoteCallResults:
        """Calls the given function with each actor instance as arg.

        Automatically marks actors unhealthy if they crash during the remote call.

        Args:
            func: A single Callable applied to all specified remote actors or a list
                of Callables, that get applied on the list of specified remote actors.
                In the latter case, both list of Callables and list of specified actors
                must have the same length. Alternatively, you can use the name of the
                remote method to be called, instead, or a list of remote method names.
            kwargs: An optional single kwargs dict or a list of kwargs dict matching the
                list of provided `func` or `remote_actor_ids`. In the first case (single
                dict), use `kwargs` on all remote calls. The latter case (list of
                dicts) allows you to define individualized kwarg dicts per actor.
            healthy_only: If True, applies `func` only to actors currently tagged
                "healthy", otherwise to all actors. If `healthy_only=False` and
                `mark_healthy=True`, will send `func` to all actors and mark those
                actors "healthy" that respond to the request within `timeout_seconds`
                and are currently tagged as "unhealthy".
            remote_actor_ids: Apply func on a selected set of remote actors. Use None
                (default) for all actors.
            timeout_seconds: Time to wait (in seconds) for results. Set this to 0.0 for
                fire-and-forget. Set this to None (default) to wait infinitely (i.e. for
                synchronous execution).
            return_obj_refs: whether to return ObjectRef instead of actual results.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of the context of this manager.
            mark_healthy: Whether to mark all those actors healthy again that are
                currently marked unhealthy AND that returned results from the remote
                call (within the given `timeout_seconds`).
                Note that actors are NOT set unhealthy, if they simply time out
                (only if they return a RayActorError).
                Also not that this setting is ignored if `healthy_only=True` (b/c this
                setting only affects actors that are currently tagged as unhealthy).

        Returns:
            The list of return values of all calls to `func(actor)`. The values may be
            actual data returned or exceptions raised during the remote call in the
            format of RemoteCallResults.
        """
        remote_actor_ids = remote_actor_ids or self.actor_ids()
        if healthy_only:
            func, kwargs, remote_actor_ids = self._filter_by_healthy_state(
                func=func, kwargs=kwargs, remote_actor_ids=remote_actor_ids
            )

        # Send out remote requests.
        remote_calls = self._call_actors(
            func=func,
            kwargs=kwargs,
            remote_actor_ids=remote_actor_ids,
        )

        # Collect remote request results (if available given timeout and/or errors).
        _, remote_results = self._fetch_result(
            remote_actor_ids=remote_actor_ids,
            remote_calls=remote_calls,
            tags=[None] * len(remote_calls),
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

        return remote_results

    @DeveloperAPI
    def foreach_actor_async(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]], str, List[str]],
        tag: Optional[str] = None,
        *,
        kwargs: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        healthy_only: bool = True,
        remote_actor_ids: Optional[List[int]] = None,
    ) -> int:
        """Calls given functions against each actors without waiting for results.

        Args:
            func: A single Callable applied to all specified remote actors or a list
                of Callables, that get applied on the list of specified remote actors.
                In the latter case, both list of Callables and list of specified actors
                must have the same length. Alternatively, you can use the name of the
                remote method to be called, instead, or a list of remote method names.
            tag: A tag to identify the results from this async call.
            kwargs: An optional single kwargs dict or a list of kwargs dict matching the
                list of provided `func` or `remote_actor_ids`. In the first case (single
                dict), use `kwargs` on all remote calls. The latter case (list of
                dicts) allows you to define individualized kwarg dicts per actor.
            healthy_only: If True, applies `func` only to actors currently tagged
                "healthy", otherwise to all actors. If `healthy_only=False` and
                later, `self.fetch_ready_async_reqs()` is called with
                `mark_healthy=True`, will send `func` to all actors and mark those
                actors "healthy" that respond to the request within `timeout_seconds`
                and are currently tagged as "unhealthy".
            remote_actor_ids: Apply func on a selected set of remote actors.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of the context of this manager.

        Returns:
            The number of async requests that are actually fired.
        """
        # TODO(avnishn, jungong): so thinking about this a bit more, it would be the
        #  best if we can attach multiple tags to an async all, like basically this
        #  parameter should be tags:
        #  For sync calls, tags would be ().
        #  For async call users, they can attached multiple tags for a single call, like
        #  ("rollout_worker", "sync_weight").
        #  For async fetch result, we can also specify a single, or list of tags. For
        #  example, ("eval", "sample") will fetch all the sample() calls on eval
        #  workers.
        if not remote_actor_ids:
            remote_actor_ids = self.actor_ids()

        num_calls = (
            len(func)
            if isinstance(func, list)
            else len(kwargs)
            if isinstance(kwargs, list)
            else len(remote_actor_ids)
        )

        # Perform round-robin assignment of all provided calls for any number of our
        # actors. Note that this way, some actors might receive more than 1 request in
        # this call.
        if num_calls != len(remote_actor_ids):
            remote_actor_ids = [
                (self._current_actor_id + i) % self.num_actors()
                for i in range(num_calls)
            ]
            # Update our round-robin pointer.
            self._current_actor_id += num_calls
            self._current_actor_id %= self.num_actors()

        if healthy_only:
            func, kwargs, remote_actor_ids = self._filter_by_healthy_state(
                func=func, kwargs=kwargs, remote_actor_ids=remote_actor_ids
            )

        num_calls_to_make: Dict[int, int] = defaultdict(lambda: 0)
        # Drop calls to actors that are too busy.
        if isinstance(func, list):
            assert len(func) == len(remote_actor_ids)
            limited_func = []
            limited_kwargs = []
            limited_remote_actor_ids = []
            for i, (f, raid) in enumerate(zip(func, remote_actor_ids)):
                num_outstanding_reqs = self._remote_actor_states[
                    raid
                ].num_in_flight_async_requests
                if (
                    num_outstanding_reqs + num_calls_to_make[raid]
                    < self._max_remote_requests_in_flight_per_actor
                ):
                    num_calls_to_make[raid] += 1
                    k = kwargs[i] if isinstance(kwargs, list) else (kwargs or {})
                    limited_func.append(f)
                    limited_kwargs.append(k)
                    limited_remote_actor_ids.append(raid)
        else:
            limited_func = func
            limited_kwargs = kwargs
            limited_remote_actor_ids = []
            for raid in remote_actor_ids:
                num_outstanding_reqs = self._remote_actor_states[
                    raid
                ].num_in_flight_async_requests
                if (
                    num_outstanding_reqs + num_calls_to_make[raid]
                    < self._max_remote_requests_in_flight_per_actor
                ):
                    num_calls_to_make[raid] += 1
                    limited_remote_actor_ids.append(raid)

        if not limited_remote_actor_ids:
            return 0

        remote_calls = self._call_actors(
            func=limited_func,
            kwargs=limited_kwargs,
            remote_actor_ids=limited_remote_actor_ids,
        )

        # Save these as outstanding requests.
        for id, call in zip(limited_remote_actor_ids, remote_calls):
            self._remote_actor_states[id].num_in_flight_async_requests += 1
            self._in_flight_req_to_actor_id[call] = (tag, id)

        return len(remote_calls)

    @DeveloperAPI
    def fetch_ready_async_reqs(
        self,
        *,
        tags: Union[str, List[str], Tuple[str]] = (),
        timeout_seconds: Optional[float] = 0.0,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> RemoteCallResults:
        """Get results from outstanding async requests that are ready.

        Automatically mark actors unhealthy if they fail to respond.

        Note: If tags is an empty tuple then results from all ready async requests are
        returned.

        Args:
            timeout_seconds: ray.get() timeout. Default is 0, which only fetched those
                results (immediately) that are already ready.
            tags: A tag or a list of tags to identify the results from this async call.
            return_obj_refs: Whether to return ObjectRef instead of actual results.
            mark_healthy: Whether to mark all those actors healthy again that are
                currently marked unhealthy AND that returned results from the remote
                call (within the given `timeout_seconds`).
                Note that actors are NOT set to unhealthy, if they simply time out,
                meaning take a longer time to fulfil the remote request. We only ever
                mark an actor unhealthy, if they raise a RayActorError inside the remote
                request.
                Also note that this settings is ignored if the preceding
                `foreach_actor_async()` call used the `healthy_only=True` argument (b/c
                `mark_healthy` only affects actors that are currently tagged as
                unhealthy).

        Returns:
            A list of return values of all calls to `func(actor)` that are ready.
            The values may be actual data returned or exceptions raised during the
            remote call in the format of RemoteCallResults.
        """
        # Construct the list of in-flight requests filtered by tag.
        remote_calls, remote_actor_ids, valid_tags = self._filter_calls_by_tag(tags)
        ready, remote_results = self._fetch_result(
            remote_actor_ids=remote_actor_ids,
            remote_calls=remote_calls,
            tags=valid_tags,
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

        for obj_ref, result in zip(ready, remote_results):
            # Decrease outstanding request on this actor by 1.
            self._remote_actor_states[result.actor_id].num_in_flight_async_requests -= 1
            # Also, remove this call here from the in-flight list,
            # obj_refs may have already been removed when we disable an actor.
            if obj_ref in self._in_flight_req_to_actor_id:
                del self._in_flight_req_to_actor_id[obj_ref]

        return remote_results

    @staticmethod
    def handle_remote_call_result_errors(
        results_or_errors: RemoteCallResults,
        *,
        ignore_ray_errors: bool,
    ) -> None:
        """Checks given results for application errors and raises them if necessary.

        Args:
            results_or_errors: The results or errors to check.
            ignore_ray_errors: Whether to ignore RayErrors within the elements of
                `results_or_errors`.
        """
        for result_or_error in results_or_errors:
            # Good result.
            if result_or_error.ok:
                continue
            # RayError, but we ignore it.
            elif ignore_ray_errors:
                logger.exception(result_or_error.get())
            # Raise RayError.
            else:
                raise result_or_error.get()

    @DeveloperAPI
    def probe_unhealthy_actors(
        self,
        timeout_seconds: Optional[float] = None,
        mark_healthy: bool = False,
    ) -> List[int]:
        """Ping all unhealthy actors to try bringing them back.

        Args:
            timeout_seconds: Timeout in seconds (to avoid pinging hanging workers
                indefinitely).
            mark_healthy: Whether to mark all those actors healthy again that are
                currently marked unhealthy AND that respond to the `ping` remote request
                (within the given `timeout_seconds`).
                Note that actors are NOT set to unhealthy, if they simply time out,
                meaning take a longer time to fulfil the remote request. We only ever
                mark and actor unhealthy, if they return a RayActorError from the remote
                request.
                Also note that this settings is ignored if `healthy_only=True` (b/c this
                setting only affects actors that are currently tagged as unhealthy).

        Returns:
            A list of actor IDs that were restored by the `ping.remote()` call PLUS
            those actors that were previously restored via other remote requests.
            The cached set of such previously restored actors will be erased in this
            call.
        """
        # Collect recently restored actors (from `self._fetch_result` calls other than
        # the one triggered here via the `ping`).
        already_restored_actors = list(self._restored_actors)

        # Which actors are currently marked unhealthy?
        unhealthy_actor_ids = [
            actor_id
            for actor_id in self.actor_ids()
            if not self.is_actor_healthy(actor_id)
        ]
        # Some unhealthy actors -> `ping()` all of them to trigger a new fetch and
        # gather the just restored ones (b/c of a successful `ping` response).
        just_restored_actors = []
        if unhealthy_actor_ids:
            remote_results = self.foreach_actor(
                func=lambda actor: actor.ping(),
                remote_actor_ids=unhealthy_actor_ids,
                healthy_only=False,  # We specifically want to ping unhealthy actors.
                timeout_seconds=timeout_seconds,
                return_obj_refs=False,
                mark_healthy=mark_healthy,
            )
            just_restored_actors = [
                result.actor_id for result in remote_results if result.ok
            ]

        # Clear out previously restored actors (b/c of other successful request
        # responses, outside of this method).
        self._restored_actors.clear()

        # Return all restored actors (previously and just).
        return already_restored_actors + just_restored_actors

    def _call_actors(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]], str, List[str]],
        *,
        kwargs: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        remote_actor_ids: List[int] = None,
    ) -> List[ray.ObjectRef]:
        """Apply functions on a list of remote actors.

        Args:
            func: A single Callable applied to all specified remote actors or a list
                of Callables, that get applied on the list of specified remote actors.
                In the latter case, both list of Callables and list of specified actors
                must have the same length. Alternatively, you can use the name of the
                remote method to be called, instead, or a list of remote method names.
            kwargs: An optional single kwargs dict or a list of kwargs dict matching the
                list of provided `func` or `remote_actor_ids`. In the first case (single
                dict), use `kwargs` on all remote calls. The latter case (list of
                dicts) allows you to define individualized kwarg dicts per actor.
            remote_actor_ids: Apply func on this selected set of remote actors.

        Returns:
            A list of ObjectRefs returned from the remote calls.
        """
        if remote_actor_ids is None:
            remote_actor_ids = self.actor_ids()

        if isinstance(func, list):
            assert len(remote_actor_ids) == len(
                func
            ), "Funcs must have the same number of callables as actor indices."

        calls = []
        if isinstance(func, list):
            for i, (raid, f) in enumerate(zip(remote_actor_ids, func)):
                if isinstance(f, str):
                    calls.append(
                        getattr(self._actors[raid], f).remote(
                            **(
                                kwargs[i]
                                if isinstance(kwargs, list)
                                else (kwargs or {})
                            )
                        )
                    )
                else:
                    calls.append(self._actors[raid].apply.remote(f))
        elif isinstance(func, str):
            for i, raid in enumerate(remote_actor_ids):
                calls.append(
                    getattr(self._actors[raid], func).remote(
                        **(kwargs[i] if isinstance(kwargs, list) else (kwargs or {}))
                    )
                )
        else:
            for raid in remote_actor_ids:
                calls.append(self._actors[raid].apply.remote(func))

        return calls

    @DeveloperAPI
    def _fetch_result(
        self,
        *,
        remote_actor_ids: List[int],
        remote_calls: List[ray.ObjectRef],
        tags: List[str],
        timeout_seconds: Optional[float] = None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> Tuple[List[ray.ObjectRef], RemoteCallResults]:
        """Try fetching results from remote actor calls.

        Mark whether an actor is healthy or not accordingly.

        Args:
            remote_actor_ids: IDs of the actors these remote
                calls were fired against.
            remote_calls: List of remote calls to fetch.
            tags: List of tags used for identifying the remote calls.
            timeout_seconds: Timeout (in sec) for the ray.wait() call. Default is None,
                meaning wait indefinitely for all results.
            return_obj_refs: Whether to return ObjectRef instead of actual results.
            mark_healthy: Whether to mark certain actors healthy based on the results
                of these remote calls. Useful, for example, to make sure actors
                do not come back without proper state restoration.

        Returns:
            A list of ready ObjectRefs mapping to the results of those calls.
        """
        # Notice that we do not return the refs to any unfinished calls to the
        # user, since it is not safe to handle such remote actor calls outside the
        # context of this actor manager. These requests are simply dropped.
        timeout = float(timeout_seconds) if timeout_seconds is not None else None

        # This avoids calling ray.init() in the case of 0 remote calls.
        # This is useful if the number of remote workers is 0.
        if not remote_calls:
            return [], RemoteCallResults()

        readies, _ = ray.wait(
            remote_calls,
            num_returns=len(remote_calls),
            timeout=timeout,
            # Make sure remote results are fetched locally in parallel.
            fetch_local=not return_obj_refs,
        )

        # Remote data should already be fetched to local object store at this point.
        remote_results = RemoteCallResults()
        for ready in readies:
            # Find the corresponding actor ID for this remote call.
            actor_id = remote_actor_ids[remote_calls.index(ready)]
            tag = tags[remote_calls.index(ready)]

            # If caller wants ObjectRefs, return directly without resolving.
            if return_obj_refs:
                remote_results.add_result(actor_id, ResultOrError(result=ready), tag)
                continue

            # Try getting the ready results.
            try:
                result = ray.get(ready)

            # Any error type other than `RayError` happening during ray.get() ->
            # Throw exception right here (we don't know how to handle these non-remote
            # worker issues and should therefore crash).
            except RayError as e:
                # Return error to the user.
                remote_results.add_result(actor_id, ResultOrError(error=e), tag)

                # Mark the actor as unhealthy, take it out of service, and wait for
                # Ray Core to restore it.
                if self.is_actor_healthy(actor_id):
                    logger.error(
                        f"Ray error ({str(e)}), taking actor {actor_id} out of service."
                    )
                self.set_actor_state(actor_id, healthy=False)

            # If no errors, add result to `RemoteCallResults` to be returned.
            else:
                # Return valid result to the user.
                remote_results.add_result(actor_id, ResultOrError(result=result), tag)

                # Actor came back from an unhealthy state. Mark this actor as healthy
                # and add it to our healthy set.
                if mark_healthy and not self.is_actor_healthy(actor_id):
                    logger.warning(
                        f"Bringing previously unhealthy, now-healthy actor {actor_id} "
                        "back into service."
                    )
                    self.set_actor_state(actor_id, healthy=True)
                    self._num_actor_restarts += 1

        # Make sure, to-be-returned results are sound.
        assert len(readies) == len(remote_results)

        return readies, remote_results

    def _filter_by_healthy_state(
        self,
        *,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        kwargs: Optional[Union[Dict, List[Dict]]] = None,
        remote_actor_ids: List[int],
    ):
        """Filter out func and remote worker ids by actor state.

        Args:
            func: A single, or a list of Callables.
            kwargs: An optional single kwargs dict or a list of kwargs dicts matching
                the list of provided `func` or `remote_actor_ids`. In case of a single
                dict, uses `kwargs` on all remote calls. In case of a list of dicts,
                the given kwarg dicts are per actor `func` or per `remote_actor_ids`.
            remote_actor_ids: IDs of potential remote workers to apply func on.

        Returns:
            A tuple of (filtered func, filtered remote worker ids).
        """
        if isinstance(func, list):
            assert len(remote_actor_ids) == len(
                func
            ), "Func must have the same number of callables as remote actor ids."
            # We are given a list of functions to apply.
            # Need to filter the functions together with worker IDs.
            temp_func = []
            temp_remote_actor_ids = []
            temp_kwargs = []
            for i, (f, raid) in enumerate(zip(func, remote_actor_ids)):
                if self.is_actor_healthy(raid):
                    k = kwargs[i] if isinstance(kwargs, list) else (kwargs or {})
                    temp_func.append(f)
                    temp_kwargs.append(k)
                    temp_remote_actor_ids.append(raid)
            func = temp_func
            kwargs = temp_kwargs
            remote_actor_ids = temp_remote_actor_ids
        else:
            # Simply filter the worker IDs.
            remote_actor_ids = [i for i in remote_actor_ids if self.is_actor_healthy(i)]

        return func, kwargs, remote_actor_ids

    def _filter_calls_by_tag(
        self, tags: Union[str, List[str], Tuple[str]]
    ) -> Tuple[List[ray.ObjectRef], List[ActorHandle], List[str]]:
        """Return all the in flight requests that match the given tags, if any.

        Args:
            tags: A str or a list/tuple of str. If tags is empty, return all the in
                flight requests.

        Returns:
            A tuple consisting of a list of the remote calls that match the tag(s),
            a list of the corresponding remote actor IDs for these calls (same length),
            and a list of the tags corresponding to these calls (same length).
        """
        if isinstance(tags, str):
            tags = {tags}
        elif isinstance(tags, (list, tuple)):
            tags = set(tags)
        else:
            raise ValueError(
                f"tags must be either a str or a list/tuple of str, got {type(tags)}."
            )
        remote_calls = []
        remote_actor_ids = []
        valid_tags = []
        for call, (tag, actor_id) in self._in_flight_req_to_actor_id.items():
            # the default behavior is to return all ready results.
            if len(tags) == 0 or tag in tags:
                remote_calls.append(call)
                remote_actor_ids.append(actor_id)
                valid_tags.append(tag)

        return remote_calls, remote_actor_ids, valid_tags

    def _remove_async_state(self, actor_id: int):
        """Remove internal async state of for a given actor.

        This is called when an actor is removed from the pool or being marked
        unhealthy.

        Args:
            actor_id: The id of the actor.
        """
        # Remove any outstanding async requests for this actor.
        # Use `list` here to not change a looped generator while we mutate the
        # underlying dict.
        for id, req in list(self._in_flight_req_to_actor_id.items()):
            if id == actor_id:
                del self._in_flight_req_to_actor_id[req]

    def actors(self):
        # TODO(jungong) : remove this API once EnvRunnerGroup.remote_workers()
        #  and EnvRunnerGroup._remote_workers() are removed.
        return self._actors
