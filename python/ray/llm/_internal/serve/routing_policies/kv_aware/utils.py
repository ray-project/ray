"""Helpers for wiring KV-aware routing into an LLM deployment."""

import ray
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    KVAwareRouter,
)
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig


def _maybe_setup_kv_aware_routing(deployment_options: dict) -> None:
    """Set up KV-aware routing when the deployment's request router is a
    KVAwareRouter.

    Currently attaches the KVRouterActor, which owns the deployment's global KV
    radix tree for KV-aware request scoring.
    """
    request_router_config = deployment_options.get("request_router_config")
    if isinstance(request_router_config, dict):
        request_router_config = RequestRouterConfig(**request_router_config)
    if not isinstance(request_router_config, RequestRouterConfig):
        return
    if not issubclass(request_router_config.get_request_router_class(), KVAwareRouter):
        return

    # TODO (jeffreywang): KVRouterActor requires init_kwargs such as block_size.
    deployment_options["deployment_actors"] = [
        *deployment_options.get("deployment_actors", []),
        DeploymentActorConfig(
            name=KV_ROUTER_ACTOR_NAME,
            actor_class=ray.remote(KVRouterActor),
            actor_options={"num_cpus": 0},
        ),
    ]
