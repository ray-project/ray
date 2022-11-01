from collections import defaultdict
import copy
from dataclasses import dataclass
import logging
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.util.annotations import DeveloperAPI


logger = logging.getLogger(__name__)


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
        assert (result is None) != (
            error is None
        ), "Must provide one of, and only one of, result or error."
        self._result = result
        self._error = error

    @property
    def ok(self):
        return self._result is not None

    def get(self):
        """Returns the result or the error."""
        return self._result or self._error


@dataclass
class CallResult:
    """Represents a single result from a call to an actor.

    Each CallResult contains the index of the actor that was called
    plus the result or error from the call.
    """

    actor_idx: int
    result_or_error: ResultOrError

    @property
    def ok(self):
        """Passes through the ok property from the result_or_error."""
        return self.result_or_error.ok

    def get(self):
        """Passes through the get method from the result_or_error."""
        return self.result_or_error.get()


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

    def add_result(self, actor_idx: int, result_or_error: ResultOrError):
        """Add index of a remote actor plus the call result to the list.

        Args:
            actor_idx: Index of the remote actor.
            result_or_error: The result or error from the call.
        """
        self.result_or_errors.append(CallResult(actor_idx, result_or_error))

    def __iter__(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results."""
        # Shallow copy the list.
        return self._Iterator(copy.copy(self.result_or_errors))

    def ignore_errors(self) -> Iterator[ResultOrError]:
        """Return an iterator over the results, skipping errors."""
        return self._Iterator([r for r in self.result_or_errors if r.ok])


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

    def __init__(
        self,
        actors: Optional[List[ActorHandle]] = None,
        max_remote_requests_in_flight_per_actor: Optional[int] = 2,
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
        """
        self.__actors = actors or []
        # Start with healthy state for all remote actors.
        self.__remote_actor_states = [True] * len(self.__actors)

        # Collection of outstanding async requests, mapping from actor index
        # to a list of pending requests.
        self.__remote_reqs_in_flight: Mapping[int, int] = defaultdict(lambda: 0)
        # Maps outstanding async requests to the indices of the actors that
        # are executing them.
        self.__in_flight_req_to_actor_idx: Mapping[ray.ObjectRef, int] = {}

        self._max_remote_requests_in_flight_per_actor = (
            max_remote_requests_in_flight_per_actor
        )

    @DeveloperAPI
    def add_actors(self, actors: List[ActorHandle]):
        """Add a list of actors to the pool.

        Args:
            actors: A list of ray remote actors to be added to the pool.
        """
        self.__actors.extend(actors)
        self.__remote_actor_states.extend([True] * len(actors))

    @DeveloperAPI
    def num_actors(self) -> int:
        """Return the total number of actors in the pool."""
        return len(self.__actors)

    @DeveloperAPI
    def num_healthy_actors(self) -> int:
        """Return the number of healthy remote actors."""
        return sum(self.__remote_actor_states)

    @DeveloperAPI
    def num_outstanding_async_reqs(self) -> int:
        """Return the number of outstanding async requests."""
        return len(self.__in_flight_req_to_actor_idx)

    @DeveloperAPI
    def is_actor_healthy(self, idx: int) -> bool:
        """Whether a remote actor is in healthy state.

        Args:
            idx: Index of the remote actor.

        Returns:
            True if the actor is healthy, False otherwise.
        """
        return self.__remote_actor_states[idx]

    @DeveloperAPI
    def set_actor_state(self, idx: int, healthy: bool) -> None:
        """Update activate state for a specific remote actor.

        Args:
            idx: Index of the remote actor.
            healthy: Whether the remote actor is healthy.
        """
        self.__remote_actor_states[idx] = healthy

    @DeveloperAPI
    def clear(self):
        """Clean up managed actors."""
        while self.__actors:
            del self.__actors[0]
        self.__actors.clear()
        self.__remote_actor_states.clear()
        self.__remote_reqs_in_flight.clear()
        self.__in_flight_req_to_actor_idx.clear()

    def __call_actors(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        *,
        remote_actor_indices: List[int] = None,
    ) -> List[ray.ObjectRef]:
        """Apply functions on a list of remote actors.

        Args:
            func: A single, or a list of Callables, that get applied on the list
                of specified remote actors.
            remote_actor_indices: Apply func on this selected set of remote actors.

        Returns:
            A list of ObjectRefs returned from the remote calls.
        """
        if isinstance(func, list):
            assert len(remote_actor_indices) == len(
                func
            ), "Funcs must have the same number of callables as actor indices."

        if remote_actor_indices is None:
            remote_actor_indices = list(range(self.num_actors()))

        if isinstance(func, list):
            calls = [
                self.__remote_actors[i].apply.remote(func)
                for i, func in zip(remote_actor_indices, func)
            ]
        else:
            calls = [self.__actors[i].apply.remote(func) for i in remote_actor_indices]

        return calls

    @DeveloperAPI
    def __fetch_result_and_mark_state(
        self,
        *,
        remote_actor_indices: List[int],
        remote_calls: List[ray.ObjectRef],
        timeout_seconds: int = None,
    ) -> Tuple[List[ray.ObjectRef], RemoteCallResults]:
        """Try fetching results from remote actor calls.

        Mark whether an actor is healthy or not accordingly.

        Args:
            remote_actor_indices: indices of the actors these remote
                calls were fired against.
            remote_calls: list of remote calls to fetch.
            timeout_seconds: timeout for the ray.wait() call. Default is None.

        Returns:
            A list of ready ObjectRefs mapping to the results of those calls.
        """
        # Notice that we do not return the refs to any unfinished calls to the
        # user, since it is not safe to handle such remote actor calls outside the
        # context of this actor manager. These requests are simply dropped.
        timeout = float(timeout_seconds) if timeout_seconds is not None else None
        ready, _ = ray.wait(
            remote_calls,
            num_returns=len(remote_calls),
            timeout=timeout,
            # Make sure remote results are fetched locally in parallel.
            fetch_local=True,
        )

        if timeout is not None and len(ready) < len(remote_calls):
            logger.error(
                f"Failed to wait for {len(remote_calls) - len(ready)} remote calls."
            )

        # Remote data should already be fetched to local object store at this point.
        remote_results = RemoteCallResults()
        for i, r in enumerate(ready):
            actor_idx = remote_actor_indices[i]
            try:
                result = ray.get(r)
                remote_results.add_result(actor_idx, ResultOrError(result=result))
                # Able to fetch return value. Mark this actor healthy if necessary.
                if not self.is_actor_healthy(actor_idx):
                    logger.info(f"brining actor {actor_idx} back into service.")
                self.set_actor_state(actor_idx, healthy=True)
            except Exception as e:
                # Return error to the user.
                remote_results.add_result(actor_idx, ResultOrError(error=e))

                if isinstance(e, RayActorError):
                    # Take this actor out of service and wait for Ray Core to
                    # restore it.
                    if self.is_actor_healthy(actor_idx):
                        logger.error(
                            f"Ray error, taking actor {actor_idx} out of service. "
                            f"{str(e)}"
                        )
                    self.set_actor_state(actor_idx, healthy=False)
                else:
                    # ActorManager should not handle application level errors.
                    pass

        return ready, remote_results

    @DeveloperAPI
    def foreach_actor(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        *,
        healthy_only=True,
        remote_actor_indices: List[int] = None,
        timeout_seconds=None,
    ) -> RemoteCallResults:
        """Calls the given function with each actor instance as arg.

        Args:
            func: A single, or a list of Callables, that get applied on the list
                of specified remote actors.
            healthy_only: If True, applies func on known healthy actors only.
            remote_actor_indices: Apply func on a selected set of remote actors.
            timeout_seconds: Ray.get() timeout. Default is None.
                Note(jungong) : setting timeout_seconds to 0 effectively makes all the
                remote calls fire-and-forget, while setting timeout_seconds to None
                make them synchronous calls.

        Returns:
            The list of return values of all calls to `func(actor)`. The values may be
            actual data returned or exceptions raised during the remote call in the
            format of RemoteCallResults.
        """
        remote_actor_indices = remote_actor_indices or list(range(len(self.__actors)))
        if healthy_only:
            remote_actor_indices = [
                i for i in remote_actor_indices if self.is_actor_healthy(i)
            ]

        remote_calls = self.__call_actors(
            func=func,
            remote_actor_indices=remote_actor_indices,
        )

        _, remote_results = self.__fetch_result_and_mark_state(
            remote_actor_indices=remote_actor_indices,
            remote_calls=remote_calls,
            timeout_seconds=timeout_seconds,
        )

        return remote_results

    @DeveloperAPI
    def foreach_actor_async(
        self,
        func: Union[Callable[[Any], Any], List[Callable[[Any], Any]]],
        *,
        healthy_only=True,
        remote_actor_indices: List[int] = None,
    ) -> int:
        """Calls given functions against each actors without waiting for results.

        Args:
            func: A single, or a list of Callables, that get applied on the list
                of specified remote actors.
            healthy_only: If True, applies func on known healthy actors only.
            remote_actor_indices: Apply func on a selected set of remote actors.

        Returns:
            The number of async requests that are actually fired.
        """
        remote_actor_indices = remote_actor_indices or list(range(len(self.__actors)))

        if healthy_only:
            remote_actor_indices = [
                i for i in remote_actor_indices if self.is_actor_healthy(i)
            ]

        if isinstance(func, list) and len(func) != len(remote_actor_indices):
            raise ValueError(
                f"The number of functions specified {len(func)} must match "
                f"the number of remote actor indices {len(remote_actor_indices)}."
            )

        num_calls_to_make: Dict[int, int] = defaultdict(lambda: 0)
        # Drop calls to actors that are too busy.
        if isinstance(func, list):
            limited_func = []
            limited_remote_actor_indices = []
            for i, f in zip(remote_actor_indices, func):
                if (
                    self.__remote_reqs_in_flight[i] + num_calls_to_make[i]
                    < self._max_remote_requests_in_flight_per_actor
                ):
                    num_calls_to_make[i] += 1
                    limited_func.append(f)
                    limited_remote_actor_indices.append(i)
        else:
            limited_func = func
            limited_remote_actor_indices = []
            for i in remote_actor_indices:
                if (
                    self.__remote_reqs_in_flight[i] + num_calls_to_make[i]
                    < self._max_remote_requests_in_flight_per_actor
                ):
                    num_calls_to_make[i] += 1
                    limited_remote_actor_indices.append(i)

        remote_calls = self.__call_actors(
            func=limited_func,
            remote_actor_indices=limited_remote_actor_indices,
        )

        # Save these as outstanding requests.
        for idx, call in zip(limited_remote_actor_indices, remote_calls):
            self.__remote_reqs_in_flight[idx] += 1
            self.__in_flight_req_to_actor_idx[call] = idx

        return len(remote_calls)

    @DeveloperAPI
    def fetch_ready_async_reqs(
        self,
        *,
        timeout_seconds: Union[None, int] = 0,
    ) -> RemoteCallResults:
        """Get results from outstanding async requests that are ready.

        Args:
            timeout_seconds: Ray.get() timeout. Default is 0 (only those that are
                already ready).

        Returns:
            A list of return values of all calls to `func(actor)` that are ready.
            The values may be actual data returned or exceptions raised during the
            remote call in the format of RemoteCallResults.
        """
        # Construct the list of in-flight requests.
        ready, remote_results = self.__fetch_result_and_mark_state(
            remote_actor_indices=list(self.__in_flight_req_to_actor_idx.values()),
            remote_calls=list(self.__in_flight_req_to_actor_idx.keys()),
            timeout_seconds=timeout_seconds,
        )

        for obj_ref, result in zip(ready, remote_results):
            # Decrease outstanding request on this actor by 1.
            self.__remote_reqs_in_flight[result.actor_idx] -= 1
            # Also, this call is done.
            del self.__in_flight_req_to_actor_idx[obj_ref]

        return remote_results
