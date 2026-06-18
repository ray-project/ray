"""Helpers for wiring KV-aware routing into an LLM deployment."""

import ray
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    KVAwareRouter,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_events import (
    configure_kv_events_for_kv_routing,
    derive_kv_event_block_size,
)
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig


def _maybe_setup_kv_aware_routing(
    deployment_options: dict, llm_config: LLMConfig
) -> None:
    """Set up KV-aware routing when the deployment's request router is a
    KVAwareRouter.

    Attaches the KVRouterActor, which owns the deployment's global KV radix
    tree, and enables the engine KV events that feed it.
    """
    request_router_config = deployment_options.get("request_router_config")
    if isinstance(request_router_config, dict):
        request_router_config = RequestRouterConfig(**request_router_config)
    if not isinstance(request_router_config, RequestRouterConfig):
        return
    if not issubclass(request_router_config.get_request_router_class(), KVAwareRouter):
        return

    deployment_options["deployment_actors"] = [
        *deployment_options.get("deployment_actors", []),
        DeploymentActorConfig(
            name=KV_ROUTER_ACTOR_NAME,
            actor_class=ray.remote(KVRouterActor),
            actor_options={"num_cpus": 0},
            init_kwargs={
                "block_size": derive_kv_event_block_size(llm_config.engine_kwargs)
            },
        ),
    ]

    configure_kv_events_for_kv_routing(llm_config)
