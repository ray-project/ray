import os
import pprint
from typing import Any, Dict, List, Optional, Type, Union

from fastapi import FastAPI
from pydantic import Field, field_validator, model_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import (
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.common.utils.import_utils import load_class
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import to_model_metadata
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import (
    build_llm_deployment,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.deployment import Application

logger = get_logger(__name__)


def _build_direct_streaming_llm_deployment(llm_config: LLMConfig) -> Application:
    """Build the LLMServer deployment with late-bound ASGI ingress enabled."""
    server_cls = llm_config.server_cls or LLMServer
    return build_llm_deployment(
        llm_config,
        deployment_cls=serve.ingress(FastAPI())(server_cls),
    )


def _build_openai_ingress_request_router(*, llm_deployment_name: str) -> Application:
    """Build the ingress request router peer for OpenAI compatible LLM apps.

    The returned Application is attached to the ingress application with
    ``Application._with_ingress_request_router``.
    """
    from ray.llm._internal.serve.core.ingress.router import LLMRouter

    logger.info("Creating 1 ingress request router replica (LLMRouter)")

    # Late-bind by deployment name to avoid pulling the LLMServer Application
    # into the router's recursive build.
    return serve.deployment(
        LLMRouter,
        num_replicas=1,
        max_ongoing_requests=1000,
    ).bind(llm_deployment_name=llm_deployment_name)


class IngressClsConfig(BaseModelExtended):
    ingress_cls: Union[str, Type[OpenAiIngress]] = Field(
        default=OpenAiIngress,
        description="The class name of the ingress to use. It can be in form of `module_name.class_name` or `module_name:class_name` or the class itself. The class constructor should take the following arguments: `(llm_deployments: Dict[str, DeploymentHandle], model_cards: Dict[str, ModelCard], lora_paths: Optional[Dict[str, str]] = None, **extra_kwargs)` where the dicts are keyed by base model ID.",
    )

    ingress_extra_kwargs: Optional[dict] = Field(
        default_factory=dict,
        description="""The kwargs to bind to the ingress deployment. This will be passed to the ingress class constructor.""",
    )

    @field_validator("ingress_cls")
    @classmethod
    def validate_class(
        cls, value: Union[str, Type[OpenAiIngress]]
    ) -> Type[OpenAiIngress]:
        if isinstance(value, str):
            return load_class(value)
        return value


class LLMServingArgs(BaseModelExtended):
    llm_configs: List[Union[str, dict, LLMConfig]] = Field(
        description="A list of LLMConfigs, or dicts representing LLMConfigs, or paths to yaml files defining LLMConfigs.",
    )
    ingress_cls_config: Union[dict, IngressClsConfig] = Field(
        default_factory=IngressClsConfig,
        description="The configuration for the ingress class. It can be a dict representing the ingress class configuration, or an IngressClsConfig object.",
    )
    ingress_deployment_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="""
            The Ray @server.deployment options for the ingress server.
        """,
    )

    @field_validator("ingress_cls_config")
    @classmethod
    def _validate_ingress_cls_config(
        cls, value: Union[dict, IngressClsConfig]
    ) -> IngressClsConfig:
        if isinstance(value, dict):
            return IngressClsConfig.model_validate(value)
        return value

    @field_validator("llm_configs")
    @classmethod
    def _validate_llm_configs(
        cls, value: List[Union[str, dict, LLMConfig]]
    ) -> List[LLMConfig]:
        llm_configs = []
        for config in value:
            if isinstance(config, str):
                if not os.path.exists(config):
                    raise ValueError(
                        f"Could not load model config from {config}, as the file does not exist."
                    )
                llm_configs.append(LLMConfig.from_file(config))
            elif isinstance(config, dict):
                llm_configs.append(LLMConfig.model_validate(config))
            elif isinstance(config, LLMConfig):
                llm_configs.append(config)
            else:
                raise TypeError(f"Invalid LLMConfig type: {type(config)}")
        return llm_configs

    @model_validator(mode="after")
    def _validate_model_ids(self):
        """Validate that model IDs are unique and at least one model is configured."""
        if len({m.model_id for m in self.llm_configs}) != len(self.llm_configs):
            raise ValueError("Duplicate models found. Make sure model ids are unique.")

        if len(self.llm_configs) == 0:
            raise ValueError(
                "List of models is empty. Maybe some parameters cannot be parsed into the LLMConfig config."
            )
        return self


def _validate_direct_streaming_builder_config(
    builder_config: LLMServingArgs,
) -> None:
    if len(builder_config.llm_configs) > 1:
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING currently supports exactly one "
            "LLM config. Multi-model direct streaming requires composing multiple "
            "LLMServer deployments into the main application graph, which is not "
            "supported yet."
        )

    if builder_config.ingress_deployment_config:
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING does not support "
            "ingress_deployment_config because LLMServer is used directly as "
            "the ingress deployment. Configure LLMServer through each "
            "LLMConfig.deployment_config instead."
        )

    ingress_cls_config = builder_config.ingress_cls_config
    if (
        ingress_cls_config.ingress_cls != OpenAiIngress
        or ingress_cls_config.ingress_extra_kwargs
    ):
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING does not support "
            "ingress_cls_config because LLMServer is used directly as the "
            "ingress deployment."
        )


def build_openai_app(builder_config: dict) -> Application:
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

    # Direct streaming attaches LLMRouter as the ingress request router and
    # uses the LLMServer deployment itself as the ingress app, so it returns
    # before the regular OpenAiIngress wiring.
    if RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING:
        _validate_direct_streaming_builder_config(builder_config)
        direct_deployment = _build_direct_streaming_llm_deployment(llm_configs[0])
        logger.info(
            "Direct streaming enabled: "
            "LLMServer=ingress, LLMRouter=ingress_request_router"
        )
        return direct_deployment._with_ingress_request_router(
            _build_openai_ingress_request_router(
                llm_deployment_name=direct_deployment._bound_deployment.name,
            )
        )

    llm_deployments = {c.model_id: build_llm_deployment(c) for c in llm_configs}
    model_cards = {c.model_id: to_model_metadata(c.model_id, c) for c in llm_configs}
    lora_paths = {
        c.model_id: c.lora_config.dynamic_lora_loading_path
        for c in llm_configs
        if c.lora_config is not None
    }

    ingress_cls_config = builder_config.ingress_cls_config
    default_ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(
        llm_configs
    )

    ingress_options = maybe_apply_llm_deployment_config_defaults(
        default_ingress_options, builder_config.ingress_deployment_config
    )

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=llm_deployments,
        model_cards=model_cards,
        lora_paths=lora_paths,
        **ingress_cls_config.ingress_extra_kwargs,
    )
