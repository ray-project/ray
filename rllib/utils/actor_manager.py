from collections import defaultdict
import copy
from dataclasses import dataclass
import logging
import sys
import time
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayError, RayTaskError
from ray.rllib.utils.typing import T
from ray.util.annotations import DeveloperAPI


logger = logging.getLogger(__name__)


@DeveloperAPI
class ResultOrError:
    """A wrapper around a result or an error.

    This is used to return data from FaultTolerantActorManager
    that allows us to distinguish between error and actual results.
    """

    def __init__(self, result: Any = None, error: Exception = None):
        """One and only one of result or error should be set.

        Args:
            result: The result of the computation.
            error: Alternatively, the error that occurred during the computation.
        """
        # Note(jungong) : None is a valid result if the remote function
        # does not return anything.
        self._result = result
        # Easier to handle if we show the user the original error.
        self._error = (
            error.as_instanceof_cause() if isinstance(error, RayTaskError) else error
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

    Example:
    >>> manager = FaultTolerantActorManager(
    ...     actors, max_remote_requests_in_flight_per_actor=2,
    ... )
    >>> results = manager.foreach_actor(lambda w: w.call())
    >>>
    >>> # Iterate over all results ignoring errors.
    >>> for result in results.ignore_errors():
    >>>     print(result.get())
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

    def ignore_errors(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results, skipping all errors."""
        return self._Iterator([r for r in self.result_or_errors if r.ok])

    def ignore_ray_errors(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results, skipping only Ray errors.

        Similar to ignore_errors, but only skips Errors raised because of
        remote actor problems (often get restored automatcially).
        This is useful for callers that wants to handle application errors differently.
        """
        return self._Iterator(
            [r for r in self.result_or_errors if not isinstance(r.get(), RayActorError)]
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
        """Calls the given function with this rollout worker instance.

        A generic interface for applying arbitray member functions on a
        remote actor.

        Args:
            func: The function to call, with this RolloutWorker as first
                argument, followed by args, and kwargs.
            args: Optional additional args to pass to the function call.
            kwargs: Optional additional kwargs to pass to the function call.

        Returns:
            The return value of the function call.
        """
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            if self.config.recreate_failed_workers:
                logger.exception("Worker exception, recreating: {}".format(e))
                # Small delay to allow logs messages to propagate.
                time.sleep(self.config.delay_between_worker_restarts_s)
                # Kill this worker so Ray Core can restart it.
                sys.exit(1)
            else:
                raise e


@DeveloperAPI
class FaultTolerantActorManager:
    """A manager that is aware of the healthiness of remote actors.

    Example:
    >>> import ray
    >>> from ray.rllib.utils.actor_manager import FaultTolerantActorManager
    >>>
    >>> @ray.remote
    ... class MyActor:
    ...    def apply(self, fn) -> Any:
    ...        return fn(self)
    ...
    ...    def do_something(self):
    ...        return True
    >>>
    >>> actors = [MyActor.remote() for _ in range(3)]
    >>> manager = FaultTolerantActorManager(
    ...     actors, max_remote_requests_in_flight_per_actor=2,
    ... )
    >>>
    >>> # Synchrnous remote calls.
    >>> results = manager.foreach_actor(lambda actor: actor.do_something())
    >>> # Print results ignoring returned errors.
    >>> print([r.get() for r in results.ignore_errors()])
    >>>
    >>> # Asynchronous remote calls.
    >>> manager.foreach_actor_async(lambda actor: actor.do_something())
    >>> time.sleep(2) # Wait for the tasks to finish.
    >>> for r in manager.fetch_ready_async_reqs()
    ...     # Handle result and errors.
    ...     if r.ok: print(r.get())
    ...     else print("Error: {}".format(r.get()))
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
        # For historic reasons, just start remote worker ID from 1, so they never
        # collide with local worker ID (0).
        self.__next_id = init_id

        # Actors are stored in a map and indexed by a unique id.
        self.__actors: Mapping[int, ActorHandle] = {}
        self.__remote_actor_states: Mapping[int, self._ActorState] = {}
        self.add_actors(actors or [])

        # Maps outstanding async requests to the ids of the actors that
        # are executing them.
        self.__in_flight_req_to_actor_id: Mapping[ray.ObjectRef, int] = {}

        self._max_remote_requests_in_flight_per_actor = (
            max_remote_requests_in_flight_per_actor
        )

        # Useful metric.
        self._num_actor_restarts = 0

    @DeveloperAPI
    def actors(self):
        """Access the underlying actors being managed.

        Warning (jungong): This API should almost never be used.
        It is only exposed for testing and backward compatibility reasons.
        Remote actors managed by this class should never be accessed directly.
        """
        # TODO(jungong) : remove this API once WorkerSet.remote_workers()
        # and WorkerSet._remote_workers() are removed.
        return self.__actors

    @DeveloperAPI
    def healthy_actor_ids(self) -> List[int]:
        """Returns a list of worker IDs that are healthy."""
        return [k for k, v in self.__remote_actor_states.items() if v.is_healthy]

    @DeveloperAPI
    def add_actors(self, actors: List[ActorHandle]):
        """Add a list of actors to the pool.

        Args:
            actors: A list of ray remote actors to be added to the pool.
        """
        for actor in actors:
            self.__actors[self.__next_id] = actor
            self.__remote_actor_states[self.__next_id] = self._ActorState()
            self.__next_id += 1

    def _remove_async_state(self, actor_id: int):
        """Remove internal async state of for a given actor.

        This is called when an actor is removed from the pool or being marked
        unhealthy.

        Args:
            actor_id: The id of the actor.
        """
        # Remove any outstanding async requests for this actor.
        reqs_to_be_removed = [
            req
            for req, id in self.__in_flight_req_to_actor_id.items()
            if id == actor_id
        ]
        for req in reqs_to_be_removed:
            del self.__in_flight_req_to_actor_id[req]

    @DeveloperAPI
    def remove_actor(self, actor_id: int) -> ActorHandle:
        """Remove an actor from the pool.

        Args:
            actor_id: ID of the actor to remove.

        Returns:
            Handle to the actor that was removed.
        """
        actor = self.__actors[actor_id]

        # Remove the actor from the pool.
        del self.__actors[actor_id]
        del self.__remote_actor_states[actor_id]
        self._remove_async_state(actor_id)

        return actor

    @DeveloperAPI
    def num_actors(self) -> int:
        """Return the total number of actors in the pool."""
        return len(self.__actors)

    @DeveloperAPI
    def num_healthy_actors(self) -> int:
        """Return the number of healthy remote actors."""
        return sum(s.is_healthy for s in self.__remote_actor_states.values())

    @DeveloperAPI
    def total_num_restarts(self) -> int:
        """Return the number of remote actors that have been restarted."""
        return self._num_actor_restarts

    @DeveloperAPI
    def num_outstanding_async_reqs(self) -> int:
        """Return the number of outstanding async requests."""
        return len(self.__in_flight_req_to_actor_id)

    @DeveloperAPI
    def is_actor_healthy(self, actor_id: int) -> bool:
        """Whether a remote actor is in healthy state.

        Args:
            actor_id: ID of the remote actor.

        Returns:
            True if the actor is healthy, False otherwise.
        """
        if actor_id not in self.__remote_actor_states:
            raise ValueError(f"Unknown actor id: {actor_id}")
        return self.__remote_actor_states[actor_id].is_healthy

    @DeveloperAPI
    def set_actor_state(self, actor_id: int, healthy: bool) -> None:
        """Update activate state for a specific remote actor.

        Args:
            actor_id: ID of the remote actor.
            healthy: Whether the remote actor is healthy.
        """
        if actor_id not in self.__remote_actor_states:
            raise ValueError(f"Unknown actor id: {actor_id}")
        self.__remote_actor_states[actor_id].is_healthy = healthy

        if not healthy:
            # Remove any async states.
            self._remove_async_state(actor_id)

    @DeveloperAPI
    def clear(self):
        """Clean up managed actors."""
        for actor in self.__actors.values():
            ray.kill(actor)
        self.__actors.clear()
        self.__remote_actor_states.clear()
        self.__in_flight_req_to_actor_id.clear()

    def __call_actors(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        *,
        remote_actor_ids: List[int] = None,
    ) -> List[ray.ObjectRef]:
        """Apply functions on a list of remote actors.

        Args:
            func: A single, or a list of Callables, that get applied on the list
                of specified remote actors.
            remote_actor_ids: Apply func on this selected set of remote actors.

        Returns:
            A list of ObjectRefs returned from the remote calls.
        """
        if isinstance(func, list):
            assert len(remote_actor_ids) == len(
                func
            ), "Funcs must have the same number of callables as actor indices."

        if remote_actor_ids is None:
            remote_actor_ids = list(self.__actors.keys())

        if isinstance(func, list):
            calls = [
                self.__actors[i].apply.remote(f) for i, f in zip(remote_actor_ids, func)
            ]
        else:
            calls = [self.__actors[i].apply.remote(func) for i in remote_actor_ids]

        return calls

    @DeveloperAPI
    def __fetch_result(
        self,
        *,
        remote_actor_ids: List[int],
        remote_calls: List[ray.ObjectRef],
        tags: List[str],
        timeout_seconds: int = None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> Tuple[List[ray.ObjectRef], RemoteCallResults]:
        """Try fetching results from remote actor calls.

        Mark whether an actor is healthy or not accordingly.

        Args:
            remote_actor_ids: IDs of the actors these remote
                calls were fired against.
            remote_calls: list of remote calls to fetch.
            tags: list of tags used for identifying the remote calls.
            timeout_seconds: timeout for the ray.wait() call. Default is None.
            return_obj_refs: whether to return ObjectRef instead of actual results.
            mark_healthy: whether to mark certain actors healthy based on the results
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

        ready, _ = ray.wait(
            remote_calls,
            num_returns=len(remote_calls),
            timeout=timeout,
            # Make sure remote results are fetched locally in parallel.
            fetch_local=not return_obj_refs,
        )

        # Remote data should already be fetched to local object store at this point.
        remote_results = RemoteCallResults()
        for r in ready:
            # Find the corresponding actor ID for this remote call.
            actor_id = remote_actor_ids[remote_calls.index(r)]
            tag = tags[remote_calls.index(r)]

            # If caller wants ObjectRefs, return directly without resolve them.
            if return_obj_refs:
                remote_results.add_result(actor_id, ResultOrError(result=r), tag)
                continue

            try:
                result = ray.get(r)
                remote_results.add_result(actor_id, ResultOrError(result=result), tag)

                if mark_healthy and not self.is_actor_healthy(actor_id):
                    # Yay, mark this actor as healthy.
                    logger.info(f"brining actor {actor_id} back into service.")
                    self.set_actor_state(actor_id, healthy=True)
                    self._num_actor_restarts += 1
            except Exception as e:
                # Return error to the user.
                remote_results.add_result(actor_id, ResultOrError(error=e), tag)

                # Mark the actor as unhealthy.
                # TODO(jungong): Using RayError here to preserve historical behavior.
                # It may very likely be better to use RayActorError here.
                if isinstance(e, RayError):
                    # Take this actor out of service and wait for Ray Core to
                    # restore it.
                    if self.is_actor_healthy(actor_id):
                        logger.error(
                            f"Ray error, taking actor {actor_id} out of service. "
                            f"{str(e)}"
                        )
                    self.set_actor_state(actor_id, healthy=False)
                else:
                    # ActorManager should not handle application level errors.
                    pass

        return ready, remote_results

    def _filter_func_and_remote_actor_id_by_state(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        remote_actor_ids: List[int],
    ):
        """Filter out func and remote worker ids by actor state.

        Args:
            func: A single, or a list of Callables.
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
            for f, i in zip(func, remote_actor_ids):
                if self.is_actor_healthy(i):
                    temp_func.append(f)
                    temp_remote_actor_ids.append(i)
            func = temp_func
            remote_actor_ids = temp_remote_actor_ids
        else:
            # Simply filter the worker IDs.
            remote_actor_ids = [i for i in remote_actor_ids if self.is_actor_healthy(i)]

        return func, remote_actor_ids

    @DeveloperAPI
    def foreach_actor(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        *,
        healthy_only=True,
        remote_actor_ids: List[int] = None,
        timeout_seconds=None,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> RemoteCallResults:
        """Calls the given function with each actor instance as arg.

        Automatically mark actors unhealthy if they fail to respond.

        Args:
            func: A single, or a list of Callables, that get applied on the list
                of specified remote actors.
            healthy_only: If True, applies func on known healthy actors only.
            remote_actor_ids: Apply func on a selected set of remote actors.
            timeout_seconds: Ray.get() timeout. Default is None.
                Note(jungong) : setting timeout_seconds to 0 effectively makes all the
                remote calls fire-and-forget, while setting timeout_seconds to None
                make them synchronous calls.
            return_obj_refs: whether to return ObjectRef instead of actual results.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of the context of this manager.
            mark_healthy: whether to mark certain actors healthy based on the results
                of these remote calls. Useful, for example, to make sure actors
                do not come back without proper state restoration.

        Returns:
            The list of return values of all calls to `func(actor)`. The values may be
            actual data returned or exceptions raised during the remote call in the
            format of RemoteCallResults.
        """
        remote_actor_ids = remote_actor_ids or list(self.__actors.keys())
        if healthy_only:
            func, remote_actor_ids = self._filter_func_and_remote_actor_id_by_state(
                func, remote_actor_ids
            )

        remote_calls = self.__call_actors(
            func=func,
            remote_actor_ids=remote_actor_ids,
        )

        _, remote_results = self.__fetch_result(
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
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        tag: str = None,
        *,
        healthy_only=True,
        remote_actor_ids: List[int] = None,
    ) -> int:
        """Calls given functions against each actors without waiting for results.

        Args:
            func: A single, or a list of Callables, that get applied on the list
                of specified remote actors.
            tag: A tag to identify the results from this async call.
            healthy_only: If True, applies func on known healthy actors only.
            remote_actor_ids: Apply func on a selected set of remote actors.
                Note, for fault tolerance reasons, these returned ObjectRefs should
                never be resolved with ray.get() outside of the context of this manager.

        Returns:
            The number of async requests that are actually fired.
        """
        # TODO(avnishn, jungong): so thinking about this a bit more, it would be the
        # best if we can attach multiple tags to an async all, like basically this
        # parameter should be tags:
        # for sync alls, tags would be ()
        # for async call users, they can attached multiple tags for a single call, like
        # ("rollout_worker", "sync_weight")
        # for async fetch result, we can also specify a single, or list of tags. for
        # example, ("eval", "sample") will fetch all the sample() calls on eval workers.
        remote_actor_ids = remote_actor_ids or list(self.__actors.keys())

        if healthy_only:
            func, remote_actor_ids = self._filter_func_and_remote_actor_id_by_state(
                func, remote_actor_ids
            )

        if isinstance(func, list) and len(func) != len(remote_actor_ids):
            raise ValueError(
                f"The number of functions specified {len(func)} must match "
                f"the number of remote actor indices {len(remote_actor_ids)}."
            )

        num_calls_to_make: Dict[int, int] = defaultdict(lambda: 0)
        # Drop calls to actors that are too busy.
        if isinstance(func, list):
            limited_func = []
            limited_remote_actor_ids = []
            for i, f in zip(remote_actor_ids, func):
                num_outstanding_reqs = self.__remote_actor_states[
                    i
                ].num_in_flight_async_requests
                if (
                    num_outstanding_reqs + num_calls_to_make[i]
                    < self._max_remote_requests_in_flight_per_actor
                ):
                    num_calls_to_make[i] += 1
                    limited_func.append(f)
                    limited_remote_actor_ids.append(i)
        else:
            limited_func = func
            limited_remote_actor_ids = []
            for i in remote_actor_ids:
                num_outstanding_reqs = self.__remote_actor_states[
                    i
                ].num_in_flight_async_requests
                if (
                    num_outstanding_reqs + num_calls_to_make[i]
                    < self._max_remote_requests_in_flight_per_actor
                ):
                    num_calls_to_make[i] += 1
                    limited_remote_actor_ids.append(i)

        remote_calls = self.__call_actors(
            func=limited_func,
            remote_actor_ids=limited_remote_actor_ids,
        )

        # Save these as outstanding requests.
        for id, call in zip(limited_remote_actor_ids, remote_calls):
            self.__remote_actor_states[id].num_in_flight_async_requests += 1
            self.__in_flight_req_to_actor_id[call] = (tag, id)

        return len(remote_calls)

    def __filter_calls_by_tag(
        self, tags
    ) -> Tuple[List[ray.ObjectRef], List[ActorHandle], List[str]]:
        """Return all the in flight requests that match the given tags.

        Args:
            tags: A str or a list of str. If tags is empty, return all the in flight

        Returns:
            A tuple of corresponding (remote_calls, remote_actor_ids, valid_tags)

        """
        if isinstance(tags, str):
            tags = {tags}
        elif isinstance(tags, (list, tuple)):
            tags = set(tags)
        else:
            raise ValueError(
                f"tags must be either a str or a list of str, got {type(tags)}."
            )
        remote_calls = []
        remote_actor_ids = []
        valid_tags = []
        for call, (tag, actor_id) in self.__in_flight_req_to_actor_id.items():
            # the default behavior is to return all ready results.
            if not len(tags) or tag in tags:
                remote_calls.append(call)
                remote_actor_ids.append(actor_id)
                valid_tags.append(tag)
        return remote_calls, remote_actor_ids, valid_tags

    @DeveloperAPI
    def fetch_ready_async_reqs(
        self,
        *,
        tags: Union[str, List[str]] = (),
        timeout_seconds: Union[None, int] = 0,
        return_obj_refs: bool = False,
        mark_healthy: bool = False,
    ) -> RemoteCallResults:
        """Get results from outstanding async requests that are ready.

        Automatically mark actors unhealthy if they fail to respond.

        Note: If tags is an empty tuple then results from all ready async requests are
        returned.

        Args:
            timeout_seconds: Ray.get() timeout. Default is 0 (only those that are
                already ready).
            tags: A tag or a list of tags to identify the results from this async call.
            return_obj_refs: whether to return ObjectRef instead of actual results.
            mark_healthy: whether to mark certain actors healthy based on the results
                of these remote calls. Useful, for example, to make sure actors
                do not come back without proper state restoration.

        Returns:
            A list of return values of all calls to `func(actor)` that are ready.
            The values may be actual data returned or exceptions raised during the
            remote call in the format of RemoteCallResults.
        """
        # Construct the list of in-flight requests filtered by tag.
        remote_calls, remote_actor_ids, valid_tags = self.__filter_calls_by_tag(tags)
        ready, remote_results = self.__fetch_result(
            remote_actor_ids=remote_actor_ids,
            remote_calls=remote_calls,
            tags=valid_tags,
            timeout_seconds=timeout_seconds,
            return_obj_refs=return_obj_refs,
            mark_healthy=mark_healthy,
        )

        for obj_ref, result in zip(ready, remote_results):
            # Decrease outstanding request on this actor by 1.
            self.__remote_actor_states[
                result.actor_id
            ].num_in_flight_async_requests -= 1
            # Also, remove this call here from the in-flight list,
            # obj_refs may have already been removed when we disable an actor.
            if obj_ref in self.__in_flight_req_to_actor_id:
                del self.__in_flight_req_to_actor_id[obj_ref]

        return remote_results

    @DeveloperAPI
    def probe_unhealthy_actors(
        self,
        timeout_seconds: Optional[int] = None,
        mark_healthy: bool = False,
    ) -> List[int]:
        """Ping all unhealthy actors to try bringing them back.

        Args:
            timeout_seconds: Timeout to avoid pinging hanging workers indefinitely.
            mark_healthy: Whether to mark actors healthy if they respond to the ping.

        Returns:
            A list of actor ids that are restored.
        """
        unhealthy_actor_ids = [
            actor_id
            for actor_id in self.actors().keys()
            if not self.is_actor_healthy(actor_id)
        ]
        if not unhealthy_actor_ids:
            # Great, nothing to do.
            return []

        remote_results = self.foreach_actor(
            func=lambda actor: actor.ping(),
            remote_actor_ids=unhealthy_actor_ids,
            healthy_only=False,  # We specifically want to ping unhealthy actors.
            timeout_seconds=timeout_seconds,
            mark_healthy=mark_healthy,
        )

        return [result.actor_id for result in remote_results if result.ok]
