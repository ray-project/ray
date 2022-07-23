import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray.actor import ActorHandle
from ray.rllib import SampleBatch
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


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
        >>> import time # doctest: +SKIP
        >>> import ray # doctest: +SKIP
        >>> from ray.rllib.execution.parallel_requests import ( # doctest: +SKIP
        ...    AsyncRequestsManager # doctest: +SKIP
        >>> # doctest: +SKIP
        >>> @ray.remote # doctest: +SKIP
        ... class MyActor: # doctest: +SKIP
        ...    def apply(self, fn, *args, **kwargs): # doctest: +SKIP
        ...        return fn(*args, **kwargs) # doctest: +SKIP
        ...    # doctest: +SKIP
        ...    def task(self, a, b): # doctest: +SKIP
        ...        time.sleep(0.5) # doctest: +SKIP
        ...        return a + b # doctest: +SKIP
        >>>
        >>> workers = [MyActor.remote() for _ in range(3)] # doctest: +SKIP
        >>> manager = AsyncRequestsManager(workers, # doctest: +SKIP
        ...     max_remote_requests_in_flight_per_worker=2) # doctest: +SKIP
        >>> manager.call(lambda worker, a, b: worker.task(a, b), # doctest: +SKIP
        ...     fn_args=[1, 2]) # doctest: +SKIP
        >>> print(manager.get_ready()) # doctest: +SKIP
        >>> manager.call(lambda worker, a, b: worker.task(a, b), # doctest: +SKIP
        ...                fn_kwargs={"a": 1, "b": 2}) # doctest: +SKIP
        >>> time.sleep(2) # doctest: +SKIP
        >>> print(manager.get_ready()) # doctest: +SKIP
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
        """Call remote function on any available Actor or - if provided - on `actor`.

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


@ExperimentalAPI
def synchronous_parallel_sample(
    *,
    worker_set: WorkerSet,
    max_agent_steps: Optional[int] = None,
    max_env_steps: Optional[int] = None,
    concat: bool = True,
) -> Union[List[SampleBatchType], SampleBatchType]:
    """Runs parallel and synchronous rollouts on all remote workers.

    Waits for all workers to return from the remote calls.

    If no remote workers exist (num_workers == 0), use the local worker
    for sampling.

    Alternatively to calling `worker.sample.remote()`, the user can provide a
    `remote_fn()`, which will be applied to the worker(s) instead.

    Args:
        worker_set: The WorkerSet to use for sampling.
        remote_fn: If provided, use `worker.apply.remote(remote_fn)` instead
            of `worker.sample.remote()` to generate the requests.
        max_agent_steps: Optional number of agent steps to be included in the
            final batch.
        max_env_steps: Optional number of environment steps to be included in the
            final batch.
        concat: Whether to concat all resulting batches at the end and return the
            concat'd batch.

    Returns:
        The list of collected sample batch types (one for each parallel
        rollout worker in the given `worker_set`).

    Examples:
        >>> # Define an RLlib trainer.
        >>> trainer = ... # doctest: +SKIP
        >>> # 2 remote workers (num_workers=2):
        >>> batches = synchronous_parallel_sample(trainer.workers) # doctest: +SKIP
        >>> print(len(batches)) # doctest: +SKIP
        2
        >>> print(batches[0]) # doctest: +SKIP
        SampleBatch(16: ['obs', 'actions', 'rewards', 'dones'])
        >>> # 0 remote workers (num_workers=0): Using the local worker.
        >>> batches = synchronous_parallel_sample(trainer.workers) # doctest: +SKIP
        >>> print(len(batches)) # doctest: +SKIP
        1
    """
    # Only allow one of `max_agent_steps` or `max_env_steps` to be defined.
    assert not (max_agent_steps is not None and max_env_steps is not None)

    agent_or_env_steps = 0
    max_agent_or_env_steps = max_agent_steps or max_env_steps or None
    all_sample_batches = []

    # Stop collecting batches as soon as one criterium is met.
    while (max_agent_or_env_steps is None and agent_or_env_steps == 0) or (
        max_agent_or_env_steps is not None
        and agent_or_env_steps < max_agent_or_env_steps
    ):
        # No remote workers in the set -> Use local worker for collecting
        # samples.
        if not worker_set.remote_workers():
            sample_batches = [worker_set.local_worker().sample()]
        # Loop over remote workers' `sample()` method in parallel.
        else:
            sample_batches = ray.get(
                [worker.sample.remote() for worker in worker_set.remote_workers()]
            )
        # Update our counters for the stopping criterion of the while loop.
        for b in sample_batches:
            if max_agent_steps:
                agent_or_env_steps += b.agent_steps()
            else:
                agent_or_env_steps += b.env_steps()
        all_sample_batches.extend(sample_batches)

    if concat is True:
        full_batch = SampleBatch.concat_samples(all_sample_batches)
        # Discard collected incomplete episodes in episode mode.
        # if max_episodes is not None and episodes >= max_episodes:
        #    last_complete_ep_idx = len(full_batch) - full_batch[
        #        SampleBatch.DONES
        #    ].reverse().index(1)
        #    full_batch = full_batch.slice(0, last_complete_ep_idx)
        return full_batch
    else:
        return all_sample_batches
