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
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import to_model_metadata
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
    _build_direct_streaming_llm_deployment,
    _build_openai_ingress_request_router,
    _validate_direct_streaming_ingress_config,
    load_class,
)
from ray.llm._internal.serve.core.ingress.ingress import (
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
    build_dp_deployment,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    DPPDDecodeServer,
    DPPDPrefillServer,
    PDDecodeServer,
    PDPrefillServer,
    PDProxyServer,  # TODO(Kourosh): Deprecate, remove in Ray 2.58.
)
from ray.serve.deployment import Application

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Deprecated: ProxyClsConfig
# TODO(Kourosh): Deprecate, remove in Ray 2.58.
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

    # TODO(Kourosh): Deprecated, remove in Ray 2.58.
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
                "proxy_cls_config is deprecated and ignored. "
                "The proxy has been replaced by PDDecodeServer which "
                "orchestrates prefill and decode directly. "
                "See PDDecodeServer and PDPrefillServer.",
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
                "proxy_deployment_config is deprecated and ignored. "
                "The proxy has been replaced by PDDecodeServer which "
                "orchestrates prefill and decode directly. "
                "See PDDecodeServer and PDPrefillServer.",
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
    def _validate_same_engine(self):
        """Prefill and decode must use the same ``llm_engine``.

        The decode orchestrator drives both sides through one connector protocol;
        a mixed pair (e.g. prefill vLLM + decode SGLang) passes the per-side
        transfer checks but has no compatible P/D wiring and fails at runtime.
        Reject it up front.
        """
        if self.prefill_config.llm_engine != self.decode_config.llm_engine:
            raise ValueError(
                "P/D prefill and decode must use the same llm_engine "
                f"(got prefill={self.prefill_config.llm_engine!r}, "
                f"decode={self.decode_config.llm_engine!r})."
            )
        return self

    @model_validator(mode="after")
    def _validate_transfer_config(self):
        """Each engine needs its own PD transfer config.

        vLLM requires ``kv_transfer_config``; SGLang requires
        ``disaggregation_transfer_backend``.
        """
        for config in [self.prefill_config, self.decode_config]:
            if config.llm_engine == "SGLang":
                if not config.engine_kwargs.get("disaggregation_transfer_backend"):
                    raise ValueError(
                        "disaggregation_transfer_backend is required for SGLang "
                        "P/D disaggregation"
                    )
            elif config.engine_kwargs.get("kv_transfer_config") is None:
                raise ValueError(
                    "kv_transfer_config is required for P/D disaggregation"
                )
        return self

    @model_validator(mode="after")
    def _reject_sglang_data_parallel(self):
        """SGLang P/D with data_parallel_size>1 is not supported yet.

        DP P/D uses DPPD{Prefill,Decode}Server, whose gang scheduling comes from
        DPServer.get_deployment_options / __init__ — both read engine-config
        fields (accelerator, placement_bundles) the minimal SGLangEngineConfig
        does not carry. Rather than silently drop gang scheduling, fail fast.
        Tracked as a follow-up (see RFC "Out of Scope").
        """
        for label, config in (
            ("prefill_config", self.prefill_config),
            ("decode_config", self.decode_config),
        ):
            if config.llm_engine != "SGLang":
                continue
            # SGLang's own engine kwarg is dp_size, not vLLM's
            # data_parallel_size; check both so neither name bypasses the guard.
            for key in ("data_parallel_size", "dp_size"):
                dp_size = config.engine_kwargs.get(key, 1)
                if isinstance(dp_size, int) and dp_size > 1:
                    raise NotImplementedError(
                        f"SGLang P/D disaggregation does not support "
                        f"{key}>1 yet (got {dp_size} on {label}). "
                        f"Use {key}=1."
                    )
        return self

    @model_validator(mode="after")
    def _set_sglang_disaggregation_mode(self):
        """Auto-set disaggregation_mode so users never set it by hand."""
        if self.prefill_config.llm_engine == "SGLang":
            self.prefill_config.engine_kwargs.setdefault(
                "disaggregation_mode", "prefill"
            )
        if self.decode_config.llm_engine == "SGLang":
            self.decode_config.engine_kwargs.setdefault("disaggregation_mode", "decode")
        return self

    @model_validator(mode="after")
    def _default_decode_sglang_bootstrap_port_base(self):
        """Shift decode's SGLang bootstrap port base off prefill's default so a
        colocated P+D pair doesn't collide (mirrors the NIXL/MoRIIO shifts).

        The decode engine runs a (mostly unused) bootstrap server too; pinning a
        distinct port avoids a same-node bind clash on 8998.
        """
        if self.decode_config.llm_engine != "SGLang":
            return self
        from ray.llm._internal.serve.engines.sglang.kv_transfer.pd_connector import (
            BOOTSTRAP_PORT_BASE_KEY,
            DEFAULT_BOOTSTRAP_PORT_BASE,
        )

        # Shift the decode BASE (not the final port): the connector adds a
        # per-replica offset on top, so colocated decode replicas still get
        # distinct ports. The +1000 stride is well above any realistic
        # tp_size*pp_size offset. Mirrors _default_decode_moriio_port_base.
        self.decode_config.experimental_configs.setdefault(
            BOOTSTRAP_PORT_BASE_KEY, DEFAULT_BOOTSTRAP_PORT_BASE + 1000
        )
        return self

    @model_validator(mode="after")
    def _default_decode_nixl_port_base(self):
        """Shift decode's NIXL base off prefill's default (20000) so colocated replicas don't collide."""
        self.decode_config.experimental_configs.setdefault(
            "NIXL_SIDE_CHANNEL_PORT_BASE", 22000
        )
        return self

    @model_validator(mode="after")
    def _default_decode_moriio_port_base(self):
        """Shift decode's MoRIIO handshake/notify bases off prefill's defaults.

        Mirrors ``_default_decode_nixl_port_base``: a colocated P+D pair on one
        node would otherwise share MoRIIO's default handshake/notify ports. Only
        applies when the decode config uses the MoRIIO connector. The +1000
        stride is well above any realistic tp_size*pp_size offset added on top.
        """
        kv_transfer_config = (
            self.decode_config.engine_kwargs.get("kv_transfer_config") or {}
        )
        if kv_transfer_config.get("kv_connector") != "MoRIIOConnector":
            return self

        from ray.llm._internal.serve.engines.vllm.kv_transfer.moriio import (
            DEFAULT_HANDSHAKE_PORT_BASE,
            DEFAULT_NOTIFY_PORT_BASE,
            HANDSHAKE_PORT_BASE_KEY,
            NOTIFY_PORT_BASE_KEY,
        )

        self.decode_config.experimental_configs.setdefault(
            HANDSHAKE_PORT_BASE_KEY, DEFAULT_HANDSHAKE_PORT_BASE + 1000
        )
        self.decode_config.experimental_configs.setdefault(
            NOTIFY_PORT_BASE_KEY, DEFAULT_NOTIFY_PORT_BASE + 1000
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

    if RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING:
        _validate_direct_streaming_ingress_config(
            pd_config.ingress_deployment_config,
            pd_config.ingress_cls_config,
        )

    prefill_dp_size = pd_config.prefill_config.engine_kwargs.get(
        "data_parallel_size", 1
    )
    decode_dp_size = pd_config.decode_config.engine_kwargs.get("data_parallel_size", 1)
    prefill_builder = (
        build_dp_deployment if prefill_dp_size > 1 else build_llm_deployment
    )

    # When DP > 1, use combined DP+PD server classes that inherit from both
    # the PD server and DPServer (for gang scheduling, DP master info, etc.).
    prefill_cls = DPPDPrefillServer if prefill_dp_size > 1 else PDPrefillServer
    decode_cls = DPPDDecodeServer if decode_dp_size > 1 else PDDecodeServer

    prefill_deployment = prefill_builder(
        pd_config.prefill_config,
        name_prefix="Prefill:",
        deployment_cls=prefill_cls,
    )

    if RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING:
        # Direct streaming makes decode the ASGI ingress, so it must be built
        # with the ASGI wrapper while still receiving the prefill backend.
        decode_deployment = _build_direct_streaming_llm_deployment(
            pd_config.decode_config,
            name_prefix="Decode:",
            bind_kwargs={"prefill_server": prefill_deployment},
            deployment_cls=decode_cls,
        )
        logger.info(
            "Direct streaming enabled for PD: "
            f"{decode_cls.__name__}=ingress, LLMRouter=ingress_request_router"
        )
        return decode_deployment._with_ingress_request_router(
            _build_openai_ingress_request_router(server=decode_deployment)
        )

    decode_builder = build_dp_deployment if decode_dp_size > 1 else build_llm_deployment
    decode_deployment = decode_builder(
        pd_config.decode_config,
        name_prefix="Decode:",
        bind_kwargs={"prefill_server": prefill_deployment},
        deployment_cls=decode_cls,
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
    # Prefill and decode share the same model_id (validated in PDServingArgs).
    # Ingress binds to decode only (the "model" the client sees).
    model_id = pd_config.decode_config.model_id
    lora_config = pd_config.decode_config.lora_config
    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments={model_id: decode_deployment},
        model_cards={model_id: to_model_metadata(model_id, pd_config.decode_config)},
        lora_paths=(
            {model_id: lora_config.dynamic_lora_loading_path}
            if lora_config is not None
            else {}
        ),
        **ingress_cls_config.ingress_extra_kwargs,
    )
