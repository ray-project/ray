import logging
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set

import ray
from ray.actor import ActorHandle
from ray.rllib.utils.annotations import ExperimentalAPI

logger = logging.getLogger(__name__)


@ExperimentalAPI
def asynchronous_parallel_requests(
    remote_requests_in_flight: DefaultDict[ActorHandle, Set[ray.ObjectRef]],
    actors: List[ActorHandle],
    ray_wait_timeout_s: Optional[float] = None,
    max_remote_requests_in_flight_per_actor: int = 2,
    remote_fn: Optional[Callable[[Any, Optional[Any], Optional[Any]], Any]] = None,
    remote_args: Optional[List[List[Any]]] = None,
    remote_kwargs: Optional[List[Dict[str, Any]]] = None,
) -> Dict[ActorHandle, Any]:
    """Runs parallel and asynchronous rollouts on all remote workers.

    May use a timeout (if provided) on `ray.wait()` and returns only those
    samples that could be gathered in the timeout window. Allows a maximum
    of `max_remote_requests_in_flight_per_actor` remote calls to be in-flight
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
        max_remote_requests_in_flight_per_actor: Maximum number of remote
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

    Returns:
        A dict mapping actor handles to the results received by sending requests
        to these actors.
        None, if no samples are ready.

    Examples:
        >>> import time
        >>> from ray.rllib.execution.parallel_requests
        ...     import asynchronous_parallel_sample
        >>> # Define an RLlib Trainer.
        >>> trainer = ... # doctest: +SKIP
        >>> # 2 remote rollout workers (num_workers=2):
        >>> batches = asynchronous_parallel_sample( # doctest: +SKIP
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
    # `max_remote_requests_in_flight_per_actor` setting allows it).
    for actor_idx, actor in enumerate(actors):
        # Still room for another request to this actor.
        if (
            len(remote_requests_in_flight[actor])
            < max_remote_requests_in_flight_per_actor
        ):
            if remote_fn is None:
                req = actor.sample.remote()
            else:
                args = remote_args[actor_idx] if remote_args else []
                kwargs = remote_kwargs[actor_idx] if remote_kwargs else {}
                req = actor.apply.remote(remote_fn, *args, **kwargs)
            # Add to our set to send to ray.wait().
            pending_remotes.add(req)
            # Keep our mappings properly updated.
            remote_requests_in_flight[actor].add(req)
            remote_to_actor[req] = actor

    # There must always be pending remote requests.
    assert len(pending_remotes) > 0
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

    # Do one ray.get().
    results = ray.get(ready)
    assert len(ready) == len(results)

    # Return mapping from (ready) actors to their results.
    ret = {}
    for obj_ref, result in zip(ready, results):
        ret[remote_to_actor[obj_ref]] = result

    return ret
