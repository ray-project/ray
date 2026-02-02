import os
import pprint
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import Field, field_validator, model_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import (
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.common.utils.import_utils import load_class
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import (
    build_llm_deployment,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.deployment import Application

logger = get_logger(__name__)


class IngressClsConfig(BaseModelExtended):
    ingress_cls: Union[str, Type[OpenAiIngress]] = Field(
        default=OpenAiIngress,
        description="The class name of the ingress to use. It can be in form of `module_name.class_name` or `module_name:class_name` or the class itself. The class constructor should take the following arguments: `(llm_deployments: List[DeploymentHandle], **extra_kwargs)` where `llm_deployments` is a list of DeploymentHandle objects from `LLMServer` deployments.",
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

    llm_deployments = [build_llm_deployment(c) for c in llm_configs]

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
        llm_deployments=llm_deployments, **ingress_cls_config.ingress_extra_kwargs
    )
