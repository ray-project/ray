"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""
from typing import Any, Optional, Union

from pydantic import Field, field_validator, model_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import (
    deep_merge_dicts,
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
    load_class,
)
from ray.llm._internal.serve.core.ingress.ingress import (
    make_fastapi_ingress,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDProxyServer,
)
from ray.serve.deployment import Application
from ray.serve.llm import (
    LLMConfig,
    build_dp_deployment,
    build_llm_deployment,
)


class ProxyClsConfig(BaseModelExtended):
    proxy_cls: Union[str, type[PDProxyServer]] = Field(
        default=PDProxyServer,
        description="The proxy class or the class module path to use.",
    )

    proxy_extra_kwargs: Optional[dict] = Field(
        default_factory=dict,
        description="The kwargs to bind to the proxy deployment. This will be passed to the proxy class constructor.",
    )

    @field_validator("proxy_cls")
    @classmethod
    def validate_class(
        cls, value: Union[str, type[PDProxyServer]]
    ) -> type[PDProxyServer]:
        if isinstance(value, str):
            return load_class(value)
        return value


class PDServingArgs(BaseModelExtended):
    """Schema for P/D serving args."""

    prefill_config: Union[str, dict, LLMConfig]
    decode_config: Union[str, dict, LLMConfig]
    proxy_cls_config: Union[dict, ProxyClsConfig] = Field(
        default_factory=ProxyClsConfig,
        description="The configuration for the proxy class.",
    )
    proxy_deployment_config: Optional[dict] = Field(
        default_factory=dict,
        description="The Ray @server.deployment options for the proxy server.",
    )
    ingress_cls_config: Union[dict, IngressClsConfig] = Field(
        default_factory=IngressClsConfig,
        description="The configuration for the ingress class.",
    )
    ingress_deployment_config: Optional[dict] = Field(
        default_factory=dict,
        description="The Ray @server.deployment options for the ingress.",
    )

    @field_validator("prefill_config", "decode_config")
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

    @field_validator("proxy_cls_config")
    @classmethod
    def _validate_proxy_cls_config(
        cls, value: Union[dict, ProxyClsConfig]
    ) -> ProxyClsConfig:
        if isinstance(value, dict):
            return ProxyClsConfig.model_validate(value)
        return value

    @field_validator("ingress_cls_config")
    @classmethod
    def _validate_ingress_cls_config(
        cls, value: Union[dict, IngressClsConfig]
    ) -> IngressClsConfig:
        if isinstance(value, dict):
            return IngressClsConfig.model_validate(value)
        return value

    @model_validator(mode="after")
    def _validate_model_ids(self):
        """Validate that prefill and decode configs use the same model ID."""
        if self.prefill_config.model_id != self.decode_config.model_id:
            raise ValueError("P/D model id mismatch")
        return self

    @model_validator(mode="after")
    def _validate_kv_transfer_config(self):
        """Validate that kv_transfer_config is set for both prefill and decode configs."""
        for config in [self.prefill_config, self.decode_config]:
            if config.engine_kwargs.get("kv_transfer_config") is None:
                raise ValueError(
                    "kv_transfer_config is required for P/D disaggregation"
                )
        return self


def build_pd_openai_app(pd_serving_args: dict) -> Application:
    """Build a deployable application utilizing prefill/decode disaggregation."""
    pd_config = PDServingArgs.model_validate(pd_serving_args)

    # Determine the builder function for prefill and decode deployments independently based on data parallelism.
    prefill_dp_size = pd_config.prefill_config.engine_kwargs.get(
        "data_parallel_size", 1
    )
    decode_dp_size = pd_config.decode_config.engine_kwargs.get("data_parallel_size", 1)

    prefill_builder = (
        build_dp_deployment if prefill_dp_size > 1 else build_llm_deployment
    )
    decode_builder = build_dp_deployment if decode_dp_size > 1 else build_llm_deployment

    prefill_deployment = prefill_builder(
        pd_config.prefill_config, name_prefix="Prefill:"
    )
    decode_deployment = decode_builder(pd_config.decode_config, name_prefix="Decode:")

    # Get the default deployment options from the PDProxyServer class based on the prefill and decode configs.
    proxy_cls_config = pd_config.proxy_cls_config

    pd_proxy_server_options = proxy_cls_config.proxy_cls.get_deployment_options(
        pd_config.prefill_config, pd_config.decode_config
    )

    # Override if the proxy deployment config is provided.
    if pd_config.proxy_deployment_config:
        pd_proxy_server_options = deep_merge_dicts(
            pd_proxy_server_options, pd_config.proxy_deployment_config
        )

    proxy_server_deployment = (
        serve.deployment(proxy_cls_config.proxy_cls)
        .options(**pd_proxy_server_options)
        .bind(
            prefill_server=prefill_deployment,
            decode_server=decode_deployment,
            **proxy_cls_config.proxy_extra_kwargs,
        )
    )

    ingress_cls_config = pd_config.ingress_cls_config
    default_ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(
        [pd_config.prefill_config, pd_config.decode_config]
    )

    ingress_options = maybe_apply_llm_deployment_config_defaults(
        default_ingress_options, pd_config.ingress_deployment_config
    )

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)
    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[proxy_server_deployment],
        **ingress_cls_config.ingress_extra_kwargs,
    )
