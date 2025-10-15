"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""
from typing import Any, Optional, Union

from pydantic import Field, field_validator, model_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import deep_merge_dicts
from ray.llm._internal.serve.deployments.prefill_decode_disagg.pd import PDProxyServer
from ray.llm._internal.serve.deployments.routers.router import (
    IngressClsConfig,
    load_class,
    make_fastapi_ingress,
)
from ray.serve.deployment import Application
from ray.serve.llm import (
    LLMConfig,
    build_llm_deployment,
)


class ProxyClsConfig(BaseModelExtended):
    proxy_cls: Union[str, type[PDProxyServer]] = Field(
        default=PDProxyServer,
        description="The class name of the proxy class.",
    )

    proxy_extra_kwargs: Optional[dict] = Field(
        default_factory=dict,
        description="The kwargs to bind to the proxy deployment. This will be passed to the proxy class constructor.",
    )

    @field_validator("proxy_cls")
    def validate_class(
        self, value: Union[str, type[PDProxyServer]]
    ) -> type[PDProxyServer]:
        if isinstance(value, str):
            return load_class(value)
        return value


class PDServingArgs(BaseModelExtended):
    """Schema for P/D serving args."""

    prefill_config: Union[str, dict, LLMConfig]
    decode_config: Union[str, dict, LLMConfig]
    proxy_cls_config: dict = Field(
        default_factory=Union[dict, ProxyClsConfig],
        description="The configuration for the proxy class.",
    )
    proxy_deployment_config: Union[dict, ProxyClsConfig] = Field(
        default_factory=dict,
        description="The Ray @server.deployment options for the proxy server.",
    )
    ingress_deployment_config: dict = Field(
        default_factory=dict,
        description="The Ray @server.deployment options for the ingress.",
    )
    ingress_cls_config: Union[dict, IngressClsConfig] = Field(
        default_factory=dict,
        description="The configuration for the ingress class.",
    )

    def _validate_llm_config(self, value: Any) -> LLMConfig:
        if isinstance(value, str):
            return LLMConfig.from_file(value)
        elif isinstance(value, dict):
            return LLMConfig.model_validate(value)
        elif isinstance(value, LLMConfig):
            return value
        else:
            raise ValueError(f"Invalid LLMConfig: {value}")

    @field_validator("prefill_config")
    def validate_prefill_config(self, value: Any) -> LLMConfig:
        return self._validate_llm_config(value)

    @field_validator("decode_config")
    def validate_decode_config(self, value: Any) -> LLMConfig:
        return self._validate_llm_config(value)

    @model_validator(mode="after")
    def validate_model_ids(self):
        if self.prefill_config.model_id != self.decode_config.model_id:
            raise ValueError("P/D model id mismatch")
        return self

    # validate that kv_transfer_config is set for both prefill and decode configs
    @model_validator(mode="after")
    def validate_kv_transfer_config(self):
        for config in [self.prefill_config, self.decode_config]:
            if config.engine_kwargs.get("kv_transfer_config") is None:
                raise ValueError(
                    "kv_transfer_config is required for P/D disaggregation"
                )
        return self


def build_pd_with_ingress(pd_serving_args: dict) -> Application:
    """Build a deployable application utilizing prefill/decode disaggregation."""
    pd_config = PDServingArgs.model_validate(pd_serving_args)

    prefill_deployment = build_llm_deployment(
        pd_config.prefill_config, name_prefix="Prefill:"
    )
    decode_deployment = build_llm_deployment(
        pd_config.decode_config, name_prefix="Decode:"
    )

    # Get the default deployment options from the PDProxyServer class based on the prefill and decode configs.
    proxy_cls_config = pd_config.proxy_cls_config

    pd_proxy_server_options = proxy_cls_config.proxy_cls.get_deployment_options(
        pd_config.prefill_config, pd_config.decode_config
    )

    # Override if the proxy deployment config is provided.
    if pd_config.proxy_deployment_config:
        deep_merge_dicts(pd_proxy_server_options, pd_config.proxy_deployment_config)

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
    ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(
        [pd_config.prefill_config, pd_config.decode_config]
    )

    if pd_config.ingress_deployment_config:
        deep_merge_dicts(ingress_options, pd_config.ingress_deployment_config)

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)
    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[proxy_server_deployment],
        **ingress_cls_config.ingress_extra_kwargs,
    )
