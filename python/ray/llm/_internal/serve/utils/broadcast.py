import pickle
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.common import RequestMetadata
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

# Timeout in seconds for waiting for deployment replicas to be populated
BROADCAST_REPLICA_POPULATION_TIMEOUT_S = 30


def broadcast(
    handle: DeploymentHandle,
    method_name: str,
    args: Union[Any, Callable[[Any], Any]] = None,
    kwargs: Union[Dict[str, Any], Callable[[Any], Dict[str, Any]]] = None,
    combine: Optional[Callable[[List[Any]], Any]] = None,
) -> Any:
    """
    Broadcasts a method call to all replicas of the given handle.

    This is useful for broadcasting a control plane message such as kv-cache
    reset or weight update to all replicas of the given handle.

    NOTE: This API is experimental and may later be promoted to a public API in
    Ray Serve directly. For now, it is only available in Ray LLM and is
    intended to enable control plane operations during RL training which is
    required when orchestrating trianing and inference loops.

    Args:
        handle: The DeploymentHandle to broadcast to.
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

    router = handle._router
    if router is None:
        raise RuntimeError("DeploymentHandle router is None.")

    # Wait for both the replica set AND the request router to be populated.
    # `running_replicas_populated()` flips when DEPLOYMENT_TARGETS long-poll
    # arrives; `request_router` becomes non-None only after DEPLOYMENT_CONFIG
    # long-poll arrives and sets `_request_router_class`. These are independent
    # long-polls, so polling only the former races with the latter.
    #
    # In normal request flow this is hidden because `assign_request` awaits
    # `_request_router_initialized` before routing — but `broadcast()` bypasses
    # `assign_request` and pokes `_replica_id_set` directly, so it has to
    # synchronize itself.
    def _get_request_router():
        if hasattr(router, "_asyncio_router"):
            return router._asyncio_router.request_router
        if hasattr(router, "request_router"):
            return router.request_router
        return None

    start_time = time.time()
    while not handle.running_replicas_populated() or _get_request_router() is None:
        if time.time() - start_time > BROADCAST_REPLICA_POPULATION_TIMEOUT_S:
            raise TimeoutError(
                "Timed out waiting for deployment router/replicas to initialize."
            )
        time.sleep(0.1)

    request_router = _get_request_router()

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
        request_id = f"broadcast-{uuid.uuid4()}"
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
