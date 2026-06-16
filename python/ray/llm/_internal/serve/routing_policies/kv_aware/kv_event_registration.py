import logging

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    resolve_kv_event_source_endpoint,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


async def maybe_register_kv_event_worker(
    llm_config: LLMConfig, engine_block_size: int, max_num_batched_tokens: int
) -> None:
    """Register this replica's KV-event endpoint with the deployment's selection
    service, when KV-aware routing is set up.

    The engine binds its KV-event PUB at a node-routable endpoint; the
    deployment-scoped ``KVRouterActor`` (owning the Dynamo selection service)
    connects out to that endpoint and indexes the replica's KV events. The
    replica runs no Dynamo runtime or event bridge -- it only advertises where
    its events are published, plus the engine facts the selection service needs
    to treat it as a schedulable worker.
    """
    kv_events_endpoint = resolve_kv_event_source_endpoint(llm_config)
    if kv_events_endpoint is None:
        return

    replica_id = serve.get_replica_context().replica_id
    kv_router_actor = serve.get_deployment_actor(KV_ROUTER_ACTOR_NAME)
    await kv_router_actor.register_kv_event_worker.remote(
        get_worker_id(replica_id.unique_id),
        replica_id.to_full_id_str(),
        engine_block_size,
        kv_events_endpoint,
        max_num_batched_tokens,
        llm_config.engine_kwargs.get("data_parallel_rank") or 0,
    )
    logger.info(
        "Registered replica %s KV events at %s with the selection service.",
        replica_id.to_full_id_str(),
        kv_events_endpoint,
    )
