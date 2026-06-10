import pprint
from typing import Optional, Type

from ray import serve
from ray.llm._internal.common.dict_utils import (
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.serve.constants import (
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
    derive_kv_block_size,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    KVAwareRouter,
)
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.deployment import Application

logger = get_logger(__name__)


DEFAULT_DEPLOYMENT_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "health_check_period_s": DEFAULT_HEALTH_CHECK_PERIOD_S,
    "health_check_timeout_s": DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    "autoscaling_config": {
        "target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    },
}


def _get_deployment_name(llm_config: LLMConfig) -> str:
    return llm_config.model_id.replace("/", "--").replace(".", "_")


def _maybe_attach_kv_router_actor(
    deployment_options: dict, llm_config: LLMConfig
) -> None:
    """Attach the KVRouterActor which maintains the global KV radix tree
    when the deployment's request router is a KVAwareRouter.
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
            actor_class=KVRouterActor,
            init_kwargs={"block_size": derive_kv_block_size(llm_config.engine_kwargs)},
            actor_options={"num_cpus": 0},
        ),
    ]


def build_llm_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    deployment_cls: Optional[Type[LLMServer]] = None,
) -> Application:
    """Build an LLMServer deployment.

    Args:
        llm_config: The LLMConfig to build the deployment.
        name_prefix: The prefix to add to the deployment name.
        bind_kwargs: The optional extra kwargs to pass to the deployment.
            Used for customizing the deployment.
        override_serve_options: The optional serve options to override the
            default options.
        deployment_cls: The deployment class to use. Defaults to LLMServer.

    Returns:
        The Ray Serve Application for the LLMServer deployment.
    """
    deployment_cls = deployment_cls or llm_config.server_cls or LLMServer
    name_prefix = name_prefix or f"{deployment_cls.__name__}:"
    bind_kwargs = bind_kwargs or {}

    deployment_options = deployment_cls.get_deployment_options(llm_config)

    # Set the name of the deployment config to map to the model ID.
    deployment_name = deployment_options.get("name", _get_deployment_name(llm_config))

    if name_prefix:
        deployment_options["name"] = name_prefix + deployment_name

    if override_serve_options:
        deployment_options.update(override_serve_options)

    deployment_options = maybe_apply_llm_deployment_config_defaults(
        DEFAULT_DEPLOYMENT_OPTIONS, deployment_options
    )

    _maybe_attach_kv_router_actor(deployment_options, llm_config)

    logger.info("============== Deployment Options ==============")
    logger.info(pprint.pformat(deployment_options))

    return serve.deployment(deployment_cls, **deployment_options).bind(
        llm_config=llm_config, **bind_kwargs
    )
