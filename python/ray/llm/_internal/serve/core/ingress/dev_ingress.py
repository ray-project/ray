"""Development/RL-focused ingress with control plane endpoints.

This module provides DevIngress, an extension of OpenAiIngress that adds
control plane endpoints for managing engine lifecycle. These endpoints
are useful for RL training workflows where engines need to be put to sleep
during training and woken up for inference.

Endpoints:
    POST /sleep: Put engine to sleep (frees GPU memory)
    POST /wakeup: Wake up engine from sleep
    POST /is_sleeping: Check if engine is sleeping
    POST /reset_prefix_cache: Reset the KV prefix cache
"""

import pprint
from typing import Dict

from starlette.responses import Response

from ray import serve
from ray.llm._internal.common.dict_utils import (
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.serve.core.configs.openai_api_models import (
    IsSleepingRequest,
    IsSleepingResponse,
    ResetPrefixCacheRequest,
    SleepRequest,
    WakeupRequest,
)
from ray.llm._internal.serve.core.ingress.builder import LLMServingArgs
from ray.llm._internal.serve.core.ingress.ingress import (
    DEFAULT_ENDPOINTS,
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.utils.dispatch import dispatch
from ray.serve.deployment import Application

logger = get_logger(__name__)


# Endpoint map for DevIngress - includes all default endpoints plus control plane
DEV_ENDPOINTS = {
    "reset_prefix_cache": lambda app: app.post("/reset_prefix_cache"),
    "sleep": lambda app: app.post("/sleep"),
    "wakeup": lambda app: app.post("/wakeup"),
    "is_sleeping": lambda app: app.post("/is_sleeping"),
    **DEFAULT_ENDPOINTS,
}


class DevIngress(OpenAiIngress):
    """OpenAI-compatible ingress with additional control plane endpoints.

    This ingress extends the standard OpenAI endpoints with control plane
    operations for managing engine lifecycle. These are useful for:
    - RL training: Put engines to sleep during training, wake up for rollouts
    - Memory management: Free GPU memory between inference workloads
    - Benchmarking: Reset prefix cache between benchmark rounds

    WARNING: These endpoints are intended for development and trusted
    environments. Consider access control in production deployments.
    """

    async def _dispatch_to_replicas(
        self, model: str, method: str, kwargs: dict | None = None
    ) -> Response:
        """Helper to dispatch a command to all replicas and return a 200 response.

        Args:
            model: The model ID or None to use default.
            method: The method name to call on each replica.
            kwargs: Optional kwargs to pass to the method.

        Returns:
            200 OK response.
        """
        model_id = await self._get_model_id(model)
        handle = self._get_configured_serve_handle(model_id)
        dispatch(handle, method, kwargs=kwargs)
        return Response(status_code=200)

    async def reset_prefix_cache(self, body: ResetPrefixCacheRequest) -> Response:
        """Reset the KV prefix cache on all replicas for the specified model.

        Args:
            body: Request containing the model ID.

        Returns:
            200 OK on success.
        """
        logger.info("Resetting prefix cache for model: %s", body.model)
        return await self._dispatch_to_replicas(body.model, "reset_prefix_cache")

    async def sleep(self, body: SleepRequest) -> Response:
        """Put the engine to sleep on all replicas for the specified model.

        This offloads model weights to CPU and discards KV cache, freeing
        GPU memory. The engine cannot process requests while sleeping.

        Args:
            body: Request containing the model ID and engine-specific options.

        Returns:
            200 OK on success.
        """
        logger.info(
            "Putting model %s to sleep with options: %s", body.model, body.options
        )
        return await self._dispatch_to_replicas(
            body.model, "sleep", kwargs=body.options
        )

    async def wakeup(self, body: WakeupRequest) -> Response:
        """Wake up the engine from sleep on all replicas for the specified model.

        Args:
            body: Request containing the model ID and engine-specific options.

        Returns:
            200 OK on success.
        """
        logger.info("Waking up model %s with options: %s", body.model, body.options)
        return await self._dispatch_to_replicas(
            body.model, "wakeup", kwargs=body.options
        )

    async def is_sleeping(self, body: IsSleepingRequest) -> IsSleepingResponse:
        """Check if the engine is sleeping for the specified model.

        This checks the sleep status across all replicas. Returns True if
        ANY replica is sleeping (uses logical OR across replicas).

        Args:
            body: Request containing the model ID.

        Returns:
            IsSleepingResponse with is_sleeping boolean.
        """
        model_id = await self._get_model_id(body.model)
        handle = self._get_configured_serve_handle(model_id)
        # Check sleeping status across all replicas - return True if any is sleeping
        results = dispatch(handle, "is_sleeping")
        is_sleeping_result = any(results) if results else False
        return IsSleepingResponse(is_sleeping=is_sleeping_result)


def build_dev_openai_app(builder_config: Dict) -> Application:
    """Build an OpenAI compatible app with dev/control plane endpoints.

    This is similar to build_openai_app but uses DevIngress with
    additional control plane endpoints (/sleep, /wakeup, /is_sleeping,
    /reset_prefix_cache).

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
