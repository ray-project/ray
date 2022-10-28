from collections import defaultdict
import logging
from typing import Any, Callable, Iterator, List, Optional, Tuple, TypeVar

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.util.annotations import DeveloperAPI


logger = logging.getLogger(__name__)
# Generic type var for foreach_* methods.
T = TypeVar("T")


class ResultOrError:
    """A wrapper around a result or an error.
    
    This is used to return data from FaultTolerantActorManager
    that allows us to distinguish between error and actual results.

    Users of this class should always use ResultOrError.Result() and
    ResultOrError.Error() to construct instances of this class.
    """
    def __init__(self, result: Any = None, error: Exception = None):
        assert (result is None) != (error is None), (
            "Must provide one of, and only one of, result or error."
        )
        self.result = result
        self.error = error
        self.ok = (self.result is not None)

    @classmethod
    def Result(cls, result: Any):
        return cls(result=result)

    @classmethod
    def Error(cls, error: Exception):
        return cls(error=error)


class CallResults:
    def __init__(self):
        self.result_or_errors = []

    def add_result_or_error(self, actor_idx: int, result_or_error: ResultOrError):
        self.result_or_errors.append((actor_idx, result_or_error))

    def iter(self) -> Iterator[ResultOrError]:
        for i, r in self.result_or_errors:
            yield i, r

    def ignore_errors(self) -> Iterator[ResultOrError]:
        for i, r in self.result_or_errors:
            if not r.ok: continue
            yield i, r


class FaultTolerantActorManager:
    """A manager that is aware of the healthiness states of remote actors.

    Args:
        actors: A list of ray remote actors to manage on. These actors must have an
            ``apply`` method which takes a function with no parameters.
        max_remote_requests_in_flight_per_actor: The maximum number of remote
            requests that can be in flight per actor. Any requests made to the pool
            that cannot be scheduled because the limit has been reached will be dropped.
            This only applies to the async mode.
    """

    def __init__(
        self,
        actors: Optional[List[ActorHandle]] = None,
        max_remote_requests_in_flight_per_actor: Optional[int] = 2,
        ignore_worker_failure=True,
    ):
        self.__actors = actors or []
        # Start with active state for all remote actors.
        self.__remote_actor_states = [True] * len(self.__actors)

        # Collection of outstanding async requests.
        self.__remote_reqs_in_flight = defaultdict(lambda: 0)
        self.__in_flight_req_to_actor_idx = {}

        self._max_remote_requests_in_flight_per_actor = (
            max_remote_requests_in_flight_per_actor
        )
        self._ignore_worker_failure = ignore_worker_failure

    @DeveloperAPI
    def add_actors(self, actors: List[ActorHandle]):
        self.__actors.extend(actors)
        self.__remote_actor_states.extend([True] * len(actors))

    @DeveloperAPI
    def num_actors(self) -> int:
        return len(self.__actors)

    @DeveloperAPI
    def num_healthy_actors(self) -> int:
        # Return the number of healthy remote actors after this iteration.
        return len([s for s in self.__remote_actor_states if s])

    @DeveloperAPI
    def is_actor_healthy(self, i: int) -> bool:
        """Whether a remote actor is in healthy state."""
        return self.__remote_actor_states[i]

    @DeveloperAPI
    def set_actor_state(self, i: int, healthy: bool) -> None:
        """Update activate state for a specific remote actor."""
        self.__remote_actor_states[i] = healthy

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
        *,
        func: Optional[Callable[[T], T]] = None,
        funcs: Optional[List[Callable[[T], T]]] = None,
        remote_actor_indices: List[int] = None,
    ) -> List[ray.ObjectRef]:
        """Apply a function on a list of remote actors.

        Args:
            func: A Callable that gets applied on all remote actors.
            funcs: A list of Callables that get applied on their corresponding
                remote actors. Only one of func and funcs can be specified.
            remote_actor_indices: Apply func on a selected set of remote actors.
        """
        assert (func or funcs) and (not func or not funcs), (
            "Only one of func or funcs can be specified. "
            "And one of func or funcs must be specified."
        )
        if funcs:
            assert len(remote_actor_indices) == len(
                funcs
            ), "Funcs must have the same number of callables as actor indices."

        if remote_actor_indices is None:
            remote_actor_indices = list(range(self.num_actors()))

        if func:
            calls = [self.__actors[i].apply.remote(func) for i in remote_actor_indices]
        else:
            calls = [
                self.__remote_actors[i].apply.remote(func)
                for i, func in zip(remote_actor_indices, funcs)
            ]

        return calls

    @DeveloperAPI
    def __fetch_result_and_mark_state(
        self,
        *,
        remote_actor_indices: List[int],
        remote_calls: List[ray.ObjectRef],
        timeout_seconds: int = None,
    ) -> Tuple[List[ray.ObjectRef], List[T]]:
        """Try fetching results from remote actor calls.

        Mark whether a actor is healthy or not accordingly.

        Args:
            remote_actor_indices: indices of the actors these remote
                calls were fired against.
            remote_calls: list of remote calls to fetch.
            timeout_seconds: timeout for the ray.wait() call. Default is None.

        Returns:
            A Tuple of:
                The remote object refs that are ready within timeout.
                Results for these remote calls.
            Note that the list of object refs is alwasy the same length as the
            list of results.
            But some of the results may be Errors.
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

        if timeout and len(ready) < len(remote_calls):
            logger.error(
                f"Failed to wait for {len(remote_calls) - len(ready)} remote calls."
            )

        # Remote data should already be fetched to local object store at this point.
        remote_results = []
        for i, r in enumerate(ready):
            actor_idx = remote_actor_indices[i]
            try:
                remote_results.append(ray.get(r))
                # Able to fetch return value. Mark this actor active if necessary.
                if not self.is_actor_healthy(actor_idx):
                    print(f"brining actor {actor_idx} back into service.")
                self.set_actor_state(actor_idx, healthy=True)
            except RayActorError as e:
                remote_results.append(e)
                # Take this actor out of service and wait for Ray Core to recover it.
                if self.is_actor_healthy(actor_idx):
                    print(
                        f"Ray error, taking actor {actor_idx} out of service. {str(e)}"
                    )
                self.set_actor_state(actor_idx, healthy=False)
            except Exception as e:
                # Application error. Return Exception as-is to the caller.
                remote_results.append(e)

        return ready, remote_results

    @DeveloperAPI
    def foreach_actor(
        self,
        func: Callable[[T], T],
        *,
        healthy_only=True,
        remote_actor_indices: List[int] = None,
        timeout_seconds=None,
    ) -> List[T]:
        """Calls the given function with each actor instance as arg.

        Args:
            func: The function to call for each actor (as only arg).
            healthy_only: Apply func on known active actors only.
            remote_actor_indices: Apply func on a selected set of remote actors.
            timeout_seconds: Ray.get() timeout. Default is None.
                Note(jungong) : setting timeout_seconds to 0 effectively makes all the
                remote calls fire-and-forget, while setting timeout_seconds to None
                make them synchronous calls.

        Returns:
             The list of return values of all calls to `func(actor)`.
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

        if self._ignore_worker_failure:
            # Filter out all worker exceptions.
            remote_results = list(
                filter(lambda r: not isinstance(r, Exception), remote_results)
            )
        else:
            # Filter out Ray errors only. User exceptions will get passed up
            # for upstream to properly handle.
            remote_results = list(
                filter(lambda r: not isinstance(r, RayActorError), remote_results)
            )

        return remote_results

    @DeveloperAPI
    def foreach_actor_async(
        self,
        func: Callable[[T], T],
        *,
        healthy_only=True,
        remote_actor_indices: List[int] = None,
    ) -> int:
        """Calls the given function with each actor instance as arg.

        Args:
            func: The function to call for each actor (as only arg).
            healthy_only: Apply func on known active actors only.
            remote_actor_indices: Apply func on a selected set of remote actors.

        Returns:
            The number of async requests that are actually fired.
        """
        remote_actor_indices = remote_actor_indices or list(range(len(self.__actors)))

        if healthy_only:
            remote_actor_indices = [
                i for i in remote_actor_indices if self.is_actor_healthy(i)
            ]

        # Drop calls to actors that are too busy.
        remote_actor_indices = [
            i
            for i in remote_actor_indices
            if self.__remote_reqs_in_flight[i]
            < self._max_remote_requests_in_flight_per_actor
        ]

        remote_calls = self.__call_actors(
            func=func,
            remote_actor_indices=remote_actor_indices,
        )

        # Save these as outstanding requests.
        for idx, call in zip(remote_actor_indices, remote_calls):
            self.__remote_reqs_in_flight[idx] += 1
            self.__in_flight_req_to_actor_idx[call] = idx

        return len(remote_calls)

    @DeveloperAPI
    def fetch_ready_async_reqs(
        self,
        *,
        timeout_seconds=0,
    ) -> Tuple[List[int], List[T]]:
        """Get results from outstanding async requests that are ready.

        Args:
            timeout_seconds: Ray.get() timeout. Default is 0 (only those that are
                already ready).
            fetch_local: whether to fetch remote call results to local object
                store with ray.wait(). Default is True.

        Returns:
            A list of results successfully returned from outstanding remote calls,
            paired with the indices of the callee actors.
        """
        # Construct the list of in-flight requests.
        ready, remote_results = self.__fetch_result_and_mark_state(
            remote_actor_indices=list(self.__in_flight_req_to_actor_idx.values()),
            remote_calls=list(self.__in_flight_req_to_actor_idx.keys()),
            timeout_seconds=timeout_seconds,
        )

        actor_indices = []
        filtered_remote_results = []
        for call, result in zip(ready, remote_results):
            actor_idx = self.__in_flight_req_to_actor_idx[call]
            # Decrease outstanding request on this actor by 1.
            self.__remote_reqs_in_flight[actor_idx] -= 1
            # Also, this call is done.
            del self.__in_flight_req_to_actor_idx[call]

            if not isinstance(
                result, Exception if self._ignore_worker_failure else RayActorError
            ):
                actor_indices.append(actor_idx)
                filtered_remote_results.append(result)

        return actor_indices, filtered_remote_results
