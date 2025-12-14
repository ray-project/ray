import pprint
from typing import Optional

from sglang_engine import SGLangServer

from ray import serve
from ray.llm._internal.common.dict_utils import deep_merge_dicts
from ray.llm._internal.serve.constants import (
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_MAX_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import LLMServingArgs
from ray.llm._internal.serve.core.ingress.ingress import make_fastapi_ingress
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.deployment import Application

logger = get_logger(__name__)

DEFAULT_DEPLOYMENT_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "health_check_period_s": DEFAULT_HEALTH_CHECK_PERIOD_S,
    "health_check_timeout_s": DEFAULT_HEALTH_CHECK_TIMEOUT_S,
}


def _get_deployment_name(llm_config: LLMConfig) -> str:
    return llm_config.model_id.replace("/", "--").replace(".", "_")


def build_sglang_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
) -> Application:
    """Build a SGLang Server deployment.

    Args:
        llm_config: The LLMConfig to build the deployment.
        name_prefix: The prefix to add to the deployment name.
        bind_kwargs: The optional extra kwargs to pass to the deployment.
            Used for customizing the deployment.
        override_serve_options: The optional serve options to override the
            default options.

    Returns:
        The Ray Serve Application for SGLang deployment.
    """
    deployment_cls = SGLangServer
    name_prefix = name_prefix or f"{deployment_cls.__name__}:"
    bind_kwargs = bind_kwargs or {}

    deployment_options = deployment_cls.get_deployment_options(llm_config)

    # Set the name of the deployment config to map to the model ID.
    deployment_name = deployment_options.get("name", _get_deployment_name(llm_config))

    if name_prefix:
        deployment_options["name"] = name_prefix + deployment_name

    if override_serve_options:
        deployment_options.update(override_serve_options)

    deployment_options = deep_merge_dicts(
        DEFAULT_DEPLOYMENT_OPTIONS, deployment_options
    )

    logger.info("============== Deployment Options ==============")
    logger.info(pprint.pformat(deployment_options))

    return serve.deployment(deployment_cls, **deployment_options).bind(
        _llm_config=llm_config, **bind_kwargs
    )


def build_sglang_openai_app(builder_config: dict) -> Application:
    """Build an OpenAI compatible app with the llm deployment setup from
    the given builder configuration.

    Args:
        builder_config: The configuration for the builder. It has to conform
            to the LLMServingArgs pydantic model.

    Returns:
        The configured Ray Serve Application router.
    """

    builder_config = LLMServingArgs.model_validate(builder_config)
    llm_configs = builder_config.llm_configs

    llm_deployments = [build_sglang_deployment(c) for c in llm_configs]

    ingress_cls_config = builder_config.ingress_cls_config
    ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(llm_configs)

    if builder_config.ingress_deployment_config:
        ingress_options = deep_merge_dicts(
            ingress_options, builder_config.ingress_deployment_config
        )

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=llm_deployments, **ingress_cls_config.ingress_extra_kwargs
    )
