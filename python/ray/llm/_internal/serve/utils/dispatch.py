import pickle
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray.serve._private.common import RequestMetadata
from ray.serve.handle import DeploymentHandle

# Timeout in seconds for waiting for deployment replicas to be populated
DISPATCH_REPLICA_POPULATION_TIMEOUT_S = 30


def dispatch(
    handle: DeploymentHandle,
    method_name: str,
    args: Union[Any, Callable[[Any], Any]] = None,
    kwargs: Union[Dict[str, Any], Callable[[Any], Dict[str, Any]]] = None,
    combine: Optional[Callable[[List[Any]], Any]] = None,
) -> Any:
    """
    Dispatches a method call to all replicas of the given handle.

    This is useful for dispatching a control plane message such as kv-cache
    reset or weight update to all replicas of the given handle.

    NOTE: This API is experimental and may later be promoted to a public API in
    Ray Serve directly. For now, it is only available in Ray LLM and is
    intended to enable control plane operations during RL training which is
    required when orchestrating trianing and inference loops.

    Args:
        handle: The DeploymentHandle to dispatch to.
        method_name: The name of the method to call on the deployment.
        args: The arguments to pass to the method. Can be a list/tuple of args,
              or a callable that takes the replica object and returns args.
        kwargs: The keyword arguments to pass to the method. Can be a dict,
                or a callable that takes the replica object and returns kwargs.
        combine: An optional callable that takes the list of results from all
            replicas and returns an aggregated result. If not provided, returns
            the list of results. The default combine function is to return the
            list of results.

    Returns:
        The result of the method call to all replicas. If combine is provided,
        returns the aggregated result. Otherwise, returns the list of results.
    """
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}

    if not handle.is_initialized:
        # If the handle is not initialized, we initialize it here.
        # We enforce running the router in a separate loop to ensure it can
        # update its replica set asynchronously while we might be blocking or
        # waiting.
        handle._init(_run_router_in_separate_loop=True)

    # Wait for the router to be populated with replicas
    # We add a timeout to prevent infinite hanging
    start_time = time.time()
    while not handle.running_replicas_populated():
        if time.time() - start_time > DISPATCH_REPLICA_POPULATION_TIMEOUT_S:
            raise TimeoutError(
                "Timed out waiting for deployment replicas to be populated."
            )
        time.sleep(0.1)

    router = handle._router
    if router is None:
        raise RuntimeError("DeploymentHandle router is None.")

    # Access the request router and replica set
    # Handle different potential internal structures
    request_router = None
    if hasattr(router, "_asyncio_router"):
        request_router = router._asyncio_router._request_router
    elif hasattr(router, "_request_router"):
        request_router = router._request_router

    if request_router is None:
        raise RuntimeError("Request router not initialized. No replicas accessible.")

    replica_set = request_router._replica_id_set

    # Execute calls
    futures = []

    # We copy the set to avoid modification during iteration if that happens
    replicas = list(replica_set)

    for replica in replicas:
        actor_name = replica.to_full_id_str()
        try:
            actor_handle = ray.get_actor(actor_name, namespace="serve")
        except ValueError:
            # Actor might be dead or not found
            continue

        # Prepare args
        call_args = args
        call_kwargs = kwargs

        if callable(args):
            call_args = args(replica)
        if callable(kwargs):
            call_kwargs = kwargs(replica)

        if not isinstance(call_args, (list, tuple)):
            raise ValueError(f"args must be a list or tuple, got {type(call_args)}")

        if not isinstance(call_kwargs, dict):
            # Fallback if callable returned something else or initial was not dict
            # But initial default is dict.
            if call_kwargs is None:
                call_kwargs = {}
            else:
                raise ValueError(f"kwargs must be a dict, got {type(call_kwargs)}")

        # Prepare Metadata
        request_id = f"dispatch-{uuid.uuid4()}"
        dummy_rm = RequestMetadata(
            request_id=request_id,
            internal_request_id=request_id,
            call_method=method_name,
        )

        pickled_rm = pickle.dumps(dummy_rm)

        # Fire remote call
        # We collect futures to wait for them
        futures.append(
            actor_handle.handle_request.remote(pickled_rm, *call_args, **call_kwargs)
        )

    # Wait for all calls to complete
    results = []
    if futures:
        results = ray.get(futures)

    if combine:
        return combine(results)
    return results
