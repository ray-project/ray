import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray.actor import ActorHandle
from ray.rllib.utils.deprecation import deprecation_warning
from ray.util import log_once

logger = logging.getLogger(__name__)


class AsyncRequestsManager:
    """A manager for asynchronous requests to actors.

    Args:
        workers: A list of ray remote workers to operate on. These workers must have an
            ``apply`` method which takes a function and a list of arguments to that
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
        >>> import time # doctest: +SKIP
        >>> import ray # doctest: +SKIP
        >>> from ray.rllib.execution.parallel_requests import AsyncRequestsManager # doctest: +SKIP
        >>>
        >>> @ray.remote # doctest: +SKIP
        ... class MyActor: # doctest: +SKIP
        ...    def apply(self, fn, *args: List[Any], **kwargs: Dict[str, Any]) -> Any: # doctest: +SKIP
        ...        return fn(*args, **kwargs) # doctest: +SKIP
        ...
        ...    def task(self, a: int, b: int) -> Any: # doctest: +SKIP
        ...        time.sleep(0.5) # doctest: +SKIP
        ...        return a + b # doctest: +SKIP
        >>>
        >>> workers = [MyActor.remote() for _ in range(3)] # doctest: +SKIP
        >>> manager = AsyncRequestsManager(workers, # doctest: +SKIP
        ...                                max_remote_requests_in_flight_per_worker=2) # doctest: +SKIP
        >>> manager.call(lambda worker, a, b: worker.task(a, b), fn_args=[1, 2]) # doctest: +SKIP
        >>> print(manager.get_ready()) # doctest: +SKIP
        >>> manager.call(lambda worker, a, b: worker.task(a, b), # doctest: +SKIP
        ...                fn_kwargs={"a": 1, "b": 2}) # doctest: +SKIP
        >>> time.sleep(2) # Wait for the tasks to finish. # doctest: +SKIP
        >>> print(manager.get_ready()) # doctest: +SKIP
    """  # noqa: E501

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
        if log_once("multi_gpu_learner_thread_deprecation_warning"):
            deprecation_warning(
                old="ray.rllib.execution.multi_gpu_learner_thread."
                "MultiGPULearnerThread"
            )

    def call(
        self,
        remote_fn: Callable,
        *,
        actor: ActorHandle = None,
        fn_args: List[Any] = None,
        fn_kwargs: Dict[str, Any] = None,
    ) -> bool:
        """Call remote function on any available Actor or - if provided - on `actor`.

        Args:
            remote_fn: The remote function to call. The function must have a signature
                of: [RolloutWorker, args, kwargs] and return Any.
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
        """Call `remote_fn` on all available workers.

        Available workers are those that have less than the maximum requests currently
        in-flight. The max. requests is set via the constructor's
        `max_remote_requests_in_flight_per_worker` argument.

        Args:
            remote_fn: The remote function to call. The function must have a signature
                of: [RolloutWorker, args, kwargs] and return Any.
            fn_args: The arguments to pass to the remote function.
            fn_kwargs: The keyword arguments to pass to the remote function.

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
