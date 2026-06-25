"""Helpers for wiring KV-aware routing into an LLM deployment."""

import logging

import ray
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_INDEXER_THREADS,
    KV_INDEXER_THREADS_KEY,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
    derive_kv_event_block_size,
    is_kv_aware_routing,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.config import DeploymentActorConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _maybe_setup_kv_aware_routing(
    deployment_options: dict, llm_config: LLMConfig
) -> None:
    """Set up KV-aware routing when the deployment's request router is a
    KVAwareRouter.

    Attaches the KVRouterActor, which owns the deployment's global KV radix
    tree, and enables the engine KV events that feed it.
    """
    if not is_kv_aware_routing(deployment_options.get("request_router_config")):
        if llm_config.engine_kwargs.get("kv_events_config") is not None:
            logger.warning(
                "engine_kwargs['kv_events_config'] is set but the deployment's "
                "request router is not a KVAwareRouter, so the engine's KV events "
                "will not be consumed. To use them, configure KVAwareRouter via "
                "deployment_config.request_router_config."
            )
        return

    deployment_options["deployment_actors"] = [
        *deployment_options.get("deployment_actors", []),
        DeploymentActorConfig(
            name=KV_ROUTER_ACTOR_NAME,
            actor_class=ray.remote(KVRouterActor),
            actor_options={"num_cpus": 0},
            init_kwargs={
                "block_size": derive_kv_event_block_size(llm_config.engine_kwargs),
                "indexer_threads": llm_config.experimental_configs.get(
                    KV_INDEXER_THREADS_KEY, DEFAULT_KV_INDEXER_THREADS
                ),
            },
        ),
    ]

    configure_kv_events_for_kv_routing(llm_config)
