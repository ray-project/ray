"""Development/RL-focused ingress with control plane endpoints.

This module provides DevIngress, an extension of OpenAiIngress that adds
control plane endpoints for managing engine lifecycle. These endpoints
are useful for RL training workflows where engines need to be put to sleep
during training and woken up for inference.

Endpoints:
    POST /sleep: Put engine to sleep (frees GPU memory)
    POST /wakeup: Wake up engine from sleep
    GET /is_sleeping: Check if engine is sleeping
    POST /pause: Pause generation (keeps weights in GPU)
    POST /resume: Resume generation after pause
    GET /is_paused: Check if engine is paused
    POST /reset_prefix_cache: Reset the KV prefix cache
"""

import pprint
from typing import Dict

from ray import serve
from ray.llm._internal.common.dict_utils import (
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.serve.core.ingress.builder import LLMServingArgs
from ray.llm._internal.serve.core.ingress.ingress import (
    DEFAULT_ENDPOINTS,
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.ingress.mixins import (
    CacheManagerIngressMixin,
    PausableIngressMixin,
    SleepableIngressMixin,
)
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.deployment import Application

logger = get_logger(__name__)


# Endpoint map for DevIngress - includes all default endpoints plus control plane
DEV_ENDPOINTS = {
    **CacheManagerIngressMixin.ENDPOINTS,
    **PausableIngressMixin.ENDPOINTS,
    **SleepableIngressMixin.ENDPOINTS,
    **DEFAULT_ENDPOINTS,
}


class DevIngress(
    OpenAiIngress,
    SleepableIngressMixin,
    PausableIngressMixin,
    CacheManagerIngressMixin,
):
    """OpenAI-compatible ingress with additional control plane endpoints.

    This ingress extends the standard OpenAI endpoints with control plane
    operations for managing engine lifecycle. These are useful for:
    - RL training: Put engines to sleep during training, wake up for rollouts
    - Memory management: Free GPU memory between inference workloads
    - Benchmarking: Reset prefix cache between benchmark rounds

    Control plane endpoints provided by mixins:
    - SleepableIngressMixin: /sleep, /wakeup, /is_sleeping
    - PausableIngressMixin: /pause, /resume, /is_paused
    - CacheManagerIngressMixin: /reset_prefix_cache

    WARNING: These endpoints are intended for development and trusted
    environments. Consider access control in production deployments.
    """

    pass


def build_dev_openai_app(builder_config: Dict) -> Application:
    """Build an OpenAI compatible app with dev/control plane endpoints.

    This is similar to build_openai_app but uses DevIngress with
    additional control plane endpoints:
    - /sleep, /wakeup, /is_sleeping (sleep mode - offloads weights to CPU)
    - /pause, /resume, /is_paused (pause mode - keeps weights in GPU)
    - /reset_prefix_cache (cache management)

    Args:
        builder_config: Configuration conforming to LLMServingArgs.
            See LLMServingArgs for details on the expected structure.

    Returns:
        The configured Ray Serve Application.

    Example:
        config = {
            "llm_configs": [llm_config],
            "ingress_deployment_config": {}
        }
        app = build_dev_openai_app(config)
        serve.run(app)
    """
    config = LLMServingArgs.model_validate(builder_config)
    llm_configs = config.llm_configs

    llm_deployments = [build_llm_deployment(c) for c in llm_configs]

    ingress_cls_config = config.ingress_cls_config
    default_ingress_options = DevIngress.get_deployment_options(llm_configs)

    ingress_options = maybe_apply_llm_deployment_config_defaults(
        default_ingress_options, config.ingress_deployment_config
    )

    ingress_cls = make_fastapi_ingress(DevIngress, endpoint_map=DEV_ENDPOINTS)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=llm_deployments, **ingress_cls_config.ingress_extra_kwargs
    )
