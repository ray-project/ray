"""Helpers for wiring KV-aware routing into an LLM deployment."""

import logging

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _maybe_setup_kv_aware_routing(
    deployment_options: dict, llm_config: LLMConfig
) -> None:
    """Set up KV-aware routing when the deployment's request router is a
    KVAwareRouter.

    The KVTokenTracker that owns the deployment's global KV radix tree is built
    inside the LLMRouter ingress replica (see core/ingress/router.py and
    kv_aware/kv_token_tracker.py), so nothing is attached here; this only enables
    the engine KV events that feed it.
    """
    if not is_kv_aware(llm_config):
        if llm_config.engine_kwargs.get("kv_events_config") is not None:
            logger.warning(
                "engine_kwargs['kv_events_config'] is set but the deployment's "
                "request router is not a KVAwareRouter, so the engine's KV events "
                "will not be consumed. To use them, configure KVAwareRouter via "
                "deployment_config.request_router_config."
            )
        return

    # Keep the engine's token-tracking gate which reads llm_config consistent
    # with the router resolved here from the merged deployment options.  Direct
    # P/D selects in LLMRouter itself, so its deployment intentionally has no
    # request_router_config to copy.
    if "request_router_config" in deployment_options:
        llm_config.deployment_config["request_router_config"] = deployment_options[
            "request_router_config"
        ]

    configure_kv_events_for_kv_routing(llm_config)
