"""Using Ray Serve to deploy LLM models with P/D disaggregation.

3-tier graph: ingress -> PDDecodeServer (decode config + engine) -> PDPrefillServer.
"""

import warnings
from typing import Any, Optional, Union

from pydantic import Field, field_validator, model_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import (
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
    PDDecodeServer,
    PDPrefillServer,
    PDProxyServer,  # TODO(Kourosh): Remove in Ray 2.56.
)
from ray.serve.deployment import Application
from ray.serve.llm import (
    LLMConfig,
    build_dp_deployment,
    build_llm_deployment,
)

# ---------------------------------------------------------------------------
# Deprecated: ProxyClsConfig
# TODO(Kourosh): Remove in Ray 2.56.
# ---------------------------------------------------------------------------


class ProxyClsConfig(BaseModelExtended):
    """Deprecated. Unused proxy configuration kept for backwards compatibility."""

    proxy_cls: Union[str, type] = Field(
        default=PDProxyServer,
        description="Deprecated.",
    )

    proxy_extra_kwargs: Optional[dict] = Field(
        default_factory=dict,
        description="Deprecated.",
    )

    @field_validator("proxy_cls")
    @classmethod
    def validate_class(cls, value):
        if isinstance(value, str):
            return load_class(value)
        return value


# ---------------------------------------------------------------------------
# PDServingArgs
# ---------------------------------------------------------------------------


class PDServingArgs(BaseModelExtended):
    """Schema for P/D serving args.

    Defines the prefill and decode LLMConfigs plus ingress options.
    The deprecated ``proxy_cls_config`` and ``proxy_deployment_config``
    fields are accepted for backwards compatibility but ignored.
    """

    prefill_config: Union[str, dict, LLMConfig]
    decode_config: Union[str, dict, LLMConfig]

    # TODO(Kourosh): Remove in Ray 2.56.
    # Deprecated proxy fields — accepted for backwards compat, ignored at build time.
    proxy_cls_config: Optional[Union[dict, ProxyClsConfig]] = Field(
        default=None,
        description="Deprecated. Accepted but ignored.",
    )
    proxy_deployment_config: Optional[dict] = Field(
        default=None,
        description="Deprecated. Accepted but ignored.",
    )

    ingress_cls_config: Union[dict, IngressClsConfig] = Field(
        default_factory=IngressClsConfig,
        description="The configuration for the ingress class.",
    )
    ingress_deployment_config: Optional[dict] = Field(
        default_factory=dict,
        description="The Ray @serve.deployment options for the ingress.",
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
        cls, value: Optional[Union[dict, ProxyClsConfig]]
    ) -> Optional[ProxyClsConfig]:
        if value is not None:
            warnings.warn(
                "proxy_cls_config is deprecated and ignored.",
                DeprecationWarning,
                stacklevel=2,
            )
            if isinstance(value, dict):
                return ProxyClsConfig.model_validate(value)
        return value

    @field_validator("proxy_deployment_config")
    @classmethod
    def _validate_proxy_deployment_config(cls, value: Optional[dict]) -> Optional[dict]:
        if value is not None:
            warnings.warn(
                "proxy_deployment_config is deprecated and ignored.",
                DeprecationWarning,
                stacklevel=2,
            )
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


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


def build_pd_openai_app(pd_serving_args: dict) -> Application:
    """Build a deployable application utilizing prefill/decode disaggregation.

    3-tier graph: ingress -> PDDecodeServer -> PDPrefillServer.
    """
    pd_config = PDServingArgs.model_validate(pd_serving_args)

    prefill_dp_size = pd_config.prefill_config.engine_kwargs.get(
        "data_parallel_size", 1
    )
    decode_dp_size = pd_config.decode_config.engine_kwargs.get("data_parallel_size", 1)
    prefill_builder = (
        build_dp_deployment if prefill_dp_size > 1 else build_llm_deployment
    )
    decode_builder = build_dp_deployment if decode_dp_size > 1 else build_llm_deployment

    prefill_deployment = prefill_builder(
        pd_config.prefill_config,
        name_prefix="Prefill:",
        deployment_cls=PDPrefillServer,
    )

    decode_deployment = decode_builder(
        pd_config.decode_config,
        name_prefix="Decode:",
        bind_kwargs={"prefill_server": prefill_deployment},
        deployment_cls=PDDecodeServer,
    )

    # -- Ingress: binds to decode only (the "model" the client sees) --
    ingress_cls_config = pd_config.ingress_cls_config
    default_ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(
        [pd_config.decode_config]
    )

    ingress_options = maybe_apply_llm_deployment_config_defaults(
        default_ingress_options, pd_config.ingress_deployment_config
    )

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)
    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[decode_deployment],
        **ingress_cls_config.ingress_extra_kwargs,
    )
