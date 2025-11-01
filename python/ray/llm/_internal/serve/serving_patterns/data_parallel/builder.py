import pprint
from typing import Any, Optional, Union

from pydantic import Field, field_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import deep_merge_dicts
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import IngressClsConfig
from ray.llm._internal.serve.core.ingress.ingress import (
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_rank_assigner import (
    _DPRankAssigner,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    DPServer,
)
from ray.serve.deployment import Application

logger = get_logger(__name__)


def build_dp_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    override_serve_options: Optional[dict] = None,
) -> Application:
    """Build a data parallel attention LLM deployment.

    Args:
        llm_config: The LLM configuration.
        name_prefix: The prefix to add to the deployment name.
        override_serve_options: The optional serve options to override the
            default options.

    Returns:
        The Ray Serve Application for the data parallel attention LLM deployment.
    """
    dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)

    # TODO(rui): figure out a better way to pass in dp_size_per_node.
    # NOTE: we cannot use engine_kwargs.data_parallel_size_local to specify
    # the number of ranks per node because that has special semantics in vLLM.
    # When we make serve's rank asignment node affinity aware, then we won't
    # need this hack to make the ranks orginally distributed across nodes.
    dp_size_per_node = llm_config.experimental_configs.get("dp_size_per_node")
    if dp_size_per_node is None:
        raise ValueError(
            "dp_size_per_node must be set in experimental_configs for DP deployment."
        )

    dp_rank_assigner = _DPRankAssigner.bind(
        dp_size=dp_size, dp_size_per_node=dp_size_per_node
    )

    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        bind_kwargs={"dp_rank_assigner": dp_rank_assigner},
        override_serve_options=override_serve_options,
        deployment_cls=DPServer,
    )


class DPOpenAiServingArgs(BaseModelExtended):
    """Schema for DP OpenAI serving args."""

    llm_config: Union[str, dict, LLMConfig] = Field(
        description="The LLM configuration",
    )
    ingress_cls_config: Union[dict, IngressClsConfig] = Field(
        default_factory=IngressClsConfig,
        description="The configuration for the ingress class.",
    )
    ingress_deployment_config: Optional[dict] = Field(
        default_factory=dict,
        description="The Ray @server.deployment options for the ingress server.",
    )

    @field_validator("llm_config")
    @classmethod
    def _validate_llm_config(cls, value: Any) -> LLMConfig:
        if isinstance(value, str):
            return LLMConfig.from_file(value)
        elif isinstance(value, dict):
            return LLMConfig.model_validate(value)
        elif isinstance(value, LLMConfig):
            return value
        else:
            raise TypeError(f"Invalid LLMConfig type: {type(value)}")

    @field_validator("ingress_cls_config")
    @classmethod
    def _validate_ingress_cls_config(cls, value: Any) -> IngressClsConfig:
        if isinstance(value, dict):
            return IngressClsConfig.model_validate(value)
        return value


def build_dp_openai_app(builder_config: dict) -> Application:
    """Build an OpenAI compatible app with the DP attention deployment
    setup from the given builder configuration.

    Args:
        builder_config: The configuration for the builder. It has to conform
            to the DPOpenAiServingArgs pydantic model.

    Returns:
        The configured Ray Serve Application.
    """

    builder_config = DPOpenAiServingArgs.model_validate(builder_config)
    llm_config = builder_config.llm_config

    dp_deployment = build_dp_deployment(llm_config)

    ingress_cls_config = builder_config.ingress_cls_config
    ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(
        [llm_config]
    )

    if builder_config.ingress_deployment_config:
        ingress_options = deep_merge_dicts(
            ingress_options, builder_config.ingress_deployment_config
        )

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[dp_deployment],
        **ingress_cls_config.ingress_extra_kwargs,
    )
