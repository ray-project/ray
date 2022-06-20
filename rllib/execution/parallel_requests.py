import logging
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set, Union

import ray
from ray.actor import ActorHandle
from ray.rllib.utils.annotations import ExperimentalAPI

logger = logging.getLogger(__name__)


@ExperimentalAPI
def asynchronous_parallel_requests(
    remote_requests_in_flight: DefaultDict[ActorHandle, Set[ray.ObjectRef]],
    actors: List[ActorHandle],
    ray_wait_timeout_s: Optional[float] = None,
    max_remote_requests_in_flight_per_worker: int = 2,
    remote_fn: Optional[
        Callable[[Any, Optional[Any], Optional[Any]], Any]
    ] = lambda actor: actor.sample(),
    remote_args: Optional[List[List[Any]]] = None,
    remote_kwargs: Optional[List[Dict[str, Any]]] = None,
    return_result_obj_ref_ids: bool = False,
    num_requests_to_launch: Optional[int] = 1,
) -> Dict[ActorHandle, Any]:
    """Runs parallel and asynchronous rollouts on all remote workers.

    May use a timeout (if provided) on `ray.wait()` and returns only those
    samples that could be gathered in the timeout window. Allows a maximum
    of `max_remote_requests_in_flight_per_worker` remote calls to be in-flight
    per remote actor.

    Alternatively to calling `actor.sample.remote()`, the user can provide a
    `remote_fn()`, which will be applied to the actor(s) instead.

    Args:
        remote_requests_in_flight: Dict mapping actor handles to a set of
            their currently-in-flight pending requests (those we expect to
            ray.get results for next). If you have an RLlib Trainer that calls
            this function, you can use its `self.remote_requests_in_flight`
            property here.
        actors: The List of ActorHandles to perform the remote requests on.
        ray_wait_timeout_s: Timeout (in sec) to be used for the underlying
            `ray.wait()` calls. If None (default), never time out (block
            until at least one actor returns something).
        max_remote_requests_in_flight_per_worker: Maximum number of remote
            requests sent to each actor. 2 (default) is probably
            sufficient to avoid idle times between two requests.
        remote_fn: If provided, use `actor.apply.remote(remote_fn)` instead of
            `actor.sample.remote()` to generate the requests.
        remote_args: If provided, use this list (per-actor) of lists (call
            args) as *args to be passed to the `remote_fn`.
            E.g.: actors=[A, B],
            remote_args=[[...] <- *args for A, [...] <- *args for B].
        remote_kwargs: If provided, use this list (per-actor) of dicts
            (kwargs) as **kwargs to be passed to the `remote_fn`.
            E.g.: actors=[A, B],
            remote_kwargs=[{...} <- **kwargs for A, {...} <- **kwargs for B].
        return_result_obj_ref_ids: If True, return the object ref IDs of the ready
            results, otherwise return the actual results.
        num_requests_to_launch: Number of remote requests to launch on each of the
            actors.

    Returns:
        A dict mapping actor handles to the results received by sending requests
        to these actors.
        None, if no samples are ready.

    Examples:
        >>> # Define an RLlib Trainer.
        >>> trainer = ... # doctest: +SKIP
        >>> # 2 remote rollout workers (num_workers=2):
        >>> batches = asynchronous_parallel_requests( # doctest: +SKIP
        ...     trainer.remote_requests_in_flight, # doctest: +SKIP
        ...     actors=trainer.workers.remote_workers(), # doctest: +SKIP
        ...     ray_wait_timeout_s=0.1, # doctest: +SKIP
        ...     remote_fn=lambda w: time.sleep(1)  # doctest: +SKIP
        ... ) # doctest: +SKIP
        >>> print(len(batches)) # doctest: +SKIP
        ... 2
        >>> # Expect a timeout to have happened.
        >>> batches[0] is None and batches[1] is None
        ... True
    """

    if remote_args is not None:
        assert len(remote_args) == len(actors)
    if remote_kwargs is not None:
        assert len(remote_kwargs) == len(actors)

    # For faster hash lookup.
    actor_set = set(actors)

    # Collect all currently pending remote requests into a single set of
    # object refs.
    pending_remotes = set()
    # Also build a map to get the associated actor for each remote request.
    remote_to_actor = {}
    for actor, set_ in remote_requests_in_flight.items():
        # Only consider those actors' pending requests that are in
        # the given `actors` list.
        if actor in actor_set:
            pending_remotes |= set_
            for r in set_:
                remote_to_actor[r] = actor

    # Add new requests, if possible (if
    # `max_remote_requests_in_flight_per_worker` setting allows it).
    for actor_idx, actor in enumerate(actors):
        # Still room for another request to this actor.
        if (
            len(remote_requests_in_flight[actor])
            < max_remote_requests_in_flight_per_worker
        ):
            if remote_fn is not None:
                args = remote_args[actor_idx] if remote_args else []
                kwargs = remote_kwargs[actor_idx] if remote_kwargs else {}
                for _ in range(num_requests_to_launch):
                    if (
                        len(remote_requests_in_flight[actor])
                        >= max_remote_requests_in_flight_per_worker
                    ):
                        break
                    req = actor.apply.remote(remote_fn, *args, **kwargs)
                    # Add to our set to send to ray.wait().
                    pending_remotes.add(req)
                    # Keep our mappings properly updated.
                    remote_requests_in_flight[actor].add(req)
                    remote_to_actor[req] = actor
                assert len(pending_remotes) > 0
    # There must always be pending remote requests.

    pending_remote_list = list(pending_remotes)

    # No timeout: Block until at least one result is returned.
    if ray_wait_timeout_s is None:
        # First try to do a `ray.wait` w/o timeout for efficiency.
        ready, _ = ray.wait(
            pending_remote_list, num_returns=len(pending_remotes), timeout=0
        )
        # Nothing returned and `timeout` is None -> Fall back to a
        # blocking wait to make sure we can return something.
        if not ready:
            ready, _ = ray.wait(pending_remote_list, num_returns=1)
    # Timeout: Do a `ray.wait() call` w/ timeout.
    else:
        ready, _ = ray.wait(
            pending_remote_list,
            num_returns=len(pending_remotes),
            timeout=ray_wait_timeout_s,
        )

        # Return empty results if nothing ready after the timeout.
        if not ready:
            return {}

    # Remove in-flight records for ready refs.
    for obj_ref in ready:
        remote_requests_in_flight[remote_to_actor[obj_ref]].remove(obj_ref)

    results = ready if return_result_obj_ref_ids else ray.get(ready)
    assert len(ready) == len(results)

    # Return mapping from (ready) actors to their results.
    ret = defaultdict(list)

    for obj_ref, result in zip(ready, results):
        ret[remote_to_actor[obj_ref]].append(result)

    return ret


def wait_asynchronous_requests(
    remote_requests_in_flight: DefaultDict[ActorHandle, Set[ray.ObjectRef]],
    ray_wait_timeout_s: Optional[float] = None,
) -> Dict[ActorHandle, Any]:
    ready_requests = asynchronous_parallel_requests(
        remote_requests_in_flight=remote_requests_in_flight,
        actors=list(remote_requests_in_flight.keys()),
        ray_wait_timeout_s=ray_wait_timeout_s,
        max_remote_requests_in_flight_per_worker=float("inf"),
        remote_fn=None,
    )
    return ready_requests


class AsyncRequestsManager:
    """A manager for asynchronous requests to actors.

    Args:
        workers: A list of ray remote workers to operate on. These workers must have an
            `apply` method which takes a function and a list of arguments to that
            function.
        max_remote_requests_in_flight_per_worker: The maximum number of remote
            requests that can be in flight per actor. Any requests made to the pool
            that cannot be scheduled because the
            max_remote_requests_in_flight_per_worker per actor has been reached will
            be queued.
        ray_wait_timeout_s: The maximum amount of time to wait for inflight requests
            to be done and ready when calling
            AsyncRequestsManager.get_ready_results().

    Example:
        >>> import time
        >>> import ray
        >>> from ray.rllib.execution.parallel_requests_manager import (
        ...     AsyncRequestsManager)
        >>>
        >>> @ray.remote
        ... class MyActor:
        ...    def apply(self, fn, *args: List[Any], **kwargs: Dict[str, Any]) -> Any:
        ...        return fn(*args, **kwargs)
        ...
        ...    def task(self, a: int, b: int) -> Any:
        ...        time.sleep(0.5)
        ...        return a + b
        >>>
        >>> workers = [MyActor.remote() for _ in range(3)]
        >>> manager = AsyncRequestsManager(workers,
        ...                                max_remote_requests_in_flight_per_worker=2)
        >>> manager.call(lambda worker, a, b: worker.task(a, b), fn_args=[1, 2])
        >>> print(manager.get_ready())
        >>> manager.call(lambda worker, a, b: worker.task(a, b),
        ...                fn_kwargs={"a": 1, "b": 2})
        >>> time.sleep(2) # Wait for the tasks to finish.
        >>> print(manager.get_ready())
    """

    def __init__(
        self,
        workers: List[ActorHandle],
        max_remote_requests_in_flight_per_worker: int = 2,
        ray_wait_timeout_s: Optional[float] = 0.0,
        return_object_refs: bool = False,
    ):
        self._ray_wait_timeout_s = ray_wait_timeout_s
        self._return_object_refs = return_object_refs
        self._max_remote_requests_in_flight = max_remote_requests_in_flight_per_worker
        self._pending_to_actor = {}
        self._pending_remotes = []
        self._remote_requests_in_flight = defaultdict(set)

        self._all_workers = (
            list(workers) if not isinstance(workers, list) else workers.copy()
        )
        self._curr_actor_ptr = 0

    def call(
        self,
        remote_fn: Callable,
        *,
        actor: ActorHandle = None,
        fn_args: List[Any] = None,
        fn_kwargs: Dict[str, Any] = None,
    ) -> bool:
        """Call remote function on any available worker or `actor`, if provided.

        Args:
            remote_fn: The remote function to call.
            actor: The actor to call the remote function on.
            fn_args: The arguments to pass to the remote function.
            fn_kwargs: The keyword arguments to pass to the remote function.

        Returns:
            True if the remoted_fn was scheduled on an actor. False if it was unable
            to be scheduled.

        Raises:
            ValueError: If actor has not been added to the manager.
            ValueError: If there are no actors available to submit a request to.
        """
        if actor and actor not in self._all_workers:
            raise ValueError(
                f"Actor {actor} has not been added to the manager."
                f" You must call manager.add_worker(actor) first "
                f"before submitting requests to actor."
            )
        if fn_args is None:
            fn_args = []
        if fn_kwargs is None:
            fn_kwargs = {}

        def actor_available(a):
            return (
                len(self._remote_requests_in_flight[a])
                < self._max_remote_requests_in_flight
            )

        num_workers = len(self._all_workers)

        if not actor:  # If no actor is specified, use a random actor.
            for _ in range(num_workers):
                if actor_available(self._all_workers[self._curr_actor_ptr]):
                    actor = self._all_workers[self._curr_actor_ptr]
                    self._curr_actor_ptr = (self._curr_actor_ptr + 1) % num_workers
                    break
                self._curr_actor_ptr = (self._curr_actor_ptr + 1) % num_workers
            if not actor:  # No actors available to schedule the request on.
                return False
        else:
            if not actor_available(actor):
                return False
        req = actor.apply.remote(remote_fn, *fn_args, **fn_kwargs)
        self._remote_requests_in_flight[actor].add(req)
        self._pending_to_actor[req] = actor
        self._pending_remotes.append(req)
        return True

    def call_on_all_available(
        self,
        remote_fn: Callable,
        *,
        fn_args: List[Any] = None,
        fn_kwargs: Dict[str, Any] = None,
    ) -> int:
        """ "Call remote_fn on all available workers

        Args:
            remote_fn: The remote function to call
            fn_args: The arguments to pass to the remote function
            fn_kwargs: The keyword arguments to pass to the remote function

        Returns:
            The number of remote calls of remote_fn that were able to be launched.
        """
        num_launched = 0
        for worker in self._all_workers:
            launched = self.call(
                remote_fn, actor=worker, fn_args=fn_args, fn_kwargs=fn_kwargs
            )
            num_launched += int(launched)
        return num_launched

    def get_ready(self) -> Dict[ActorHandle, List[Any]]:
        """Get results that are ready to be returned.

        Returns:
            A dictionary of actor handles to lists of returns from tasks that were
            previously submitted to this actor pool that are now ready to be returned.
            If self._return_object_refs is True, return only the object store
            references, not the actual return values.
        """
        ready_requests_dict = defaultdict(list)
        ready_requests, self._pending_remotes = ray.wait(
            self._pending_remotes,
            timeout=self._ray_wait_timeout_s,
            num_returns=len(self._pending_remotes),
        )
        if not self._return_object_refs:
            objs = ray.get(ready_requests)
        else:
            objs = ready_requests
        for req, obj in zip(ready_requests, objs):
            actor = self._pending_to_actor[req]
            self._remote_requests_in_flight[actor].remove(req)
            ready_requests_dict[actor].append(obj)
            del self._pending_to_actor[req]
        del ready_requests
        return dict(ready_requests_dict)

    def add_workers(self, new_workers: Union[List[ActorHandle], ActorHandle]) -> None:
        """Add a new worker to the manager

        Args:
            new_workers: The actors to add

        """
        if isinstance(new_workers, ActorHandle):
            new_workers = [new_workers]
        for new_worker in new_workers:
            if new_worker not in self._all_workers:
                self._all_workers.append(new_worker)

    def remove_workers(
        self,
        workers: Union[List[ActorHandle], ActorHandle],
        remove_in_flight_requests: bool = False,
    ) -> None:
        """Make workers unschedulable and remove them from this manager.

        Args:
            workers: The actors to remove.
            remove_in_flight_requests: If True, will remove the actor completely from
                this manager, even all of its in-flight requests. Useful for removing
                a worker after some detected failure.
        """
        if isinstance(workers, ActorHandle):
            workers = [workers]
        workers_to_remove = set(workers)
        self._all_workers[:] = [
            el for el in self._all_workers if el not in workers_to_remove
        ]
        if self._all_workers and (self._curr_actor_ptr >= len(self._all_workers)):
            # Move current pointer to the new tail of the list.
            self._curr_actor_ptr = len(self._all_workers) - 1
        elif not self._all_workers:
            self._curr_actor_ptr = 0
        # Remove all in-flight requests as well such that nothing related to the
        # removed workers remains in this manager.
        if remove_in_flight_requests is True:
            for worker in workers_to_remove:
                # Get all remote requests currently in-flight for this worker ..
                if worker in self._remote_requests_in_flight:
                    # .. and remove all of them from our internal maps/lists.
                    reqs = self._remote_requests_in_flight[worker]
                    del self._remote_requests_in_flight[worker]
                    for req in reqs:
                        del self._pending_to_actor[req]
                        if req in self._pending_remotes:
                            self._pending_remotes.remove(req)

    def get_manager_statistics(self) -> Dict[str, Any]:
        """Get statistics about the the manager

        Some of the statistics include the number of actors that are available,
        the number of pending inflight requests, and the number of pending requests
        to be scheduled on the available actors.

        Returns:
            A dictionary of statistics about the manager.
        """
        return {
            "num_pending_inflight_requests": len(self._pending_remotes),
        }

    @property
    def workers(self):
        return frozenset(self._all_workers)
