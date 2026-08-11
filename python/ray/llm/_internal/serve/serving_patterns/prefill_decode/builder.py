"""Using Ray Serve to deploy LLM models with P/D disaggregation.

3-tier graph: ingress -> PDDecodeServer (decode config + engine) -> PDPrefillServer.
"""

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
)
from ray.llm._internal.serve.core.ingress.ingress import (
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_EVENTS_PORT_BASE,
    DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET,
    DEFAULT_KV_TOKEN_PORT_BASE,
    KV_EVENTS_PORT_BASE_KEY,
    KV_TOKEN_PORT_BASE_KEY,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
    build_dp_deployment,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    DPPDDecodeServer,
    DPPDPrefillServer,
    PDDecodeServer,
    PDPrefillServer,
)
from ray.serve.deployment import Application

logger = get_logger(__name__)


_MAX_TCP_PORT = 65535


def _max_configured_replicas(llm_config: LLMConfig) -> int:
    """Return the largest configured Serve replica count for one P/D leg."""
    deployment_config = llm_config.deployment_config or {}
    num_replicas = deployment_config.get("num_replicas")
    if isinstance(num_replicas, int) and num_replicas > 0:
        return num_replicas
    if num_replicas not in (None, "auto"):
        raise ValueError("P/D num_replicas must be a positive int or 'auto'")

    autoscaling_config = deployment_config.get("autoscaling_config")
    if autoscaling_config is None:
        if num_replicas == "auto":
            raise ValueError("P/D num_replicas='auto' requires autoscaling_config")
        return 1
    if isinstance(autoscaling_config, dict):
        max_replicas = autoscaling_config.get("max_replicas")
    else:
        max_replicas = getattr(autoscaling_config, "max_replicas", None)
    if not isinstance(max_replicas, int) or max_replicas < 1:
        raise ValueError("P/D autoscaling_config.max_replicas must be a positive int")
    return max_replicas


def _port_span(llm_config: LLMConfig) -> int:
    """Reserve a port lane for every configured replica and DP rank."""
    data_parallel_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
    if not isinstance(data_parallel_size, int) or data_parallel_size < 1:
        raise ValueError("P/D data_parallel_size must be a positive int")
    return _max_configured_replicas(llm_config) * data_parallel_size


def _port_range(name: str, base: Any, span: int) -> tuple[str, int, int]:
    try:
        start = int(base)
    except (TypeError, ValueError) as e:
        raise ValueError(f"{name} must be an integer TCP port") from e
    end = start + span - 1
    if start < 1 or end > _MAX_TCP_PORT:
        raise ValueError(
            f"{name} range {start}-{end} is outside the valid TCP port range"
        )
    return name, start, end


def _validate_disjoint_port_ranges(ranges: list[tuple[str, int, int]]) -> None:
    for index, (name, start, end) in enumerate(ranges):
        for other_name, other_start, other_end in ranges[index + 1 :]:
            if start <= other_end and other_start <= end:
                raise ValueError(
                    f"P/D port ranges overlap: {name} ({start}-{end}) and "
                    f"{other_name} ({other_start}-{other_end})"
                )


def _configure_pd_kv_port_ranges(
    prefill_config: LLMConfig, decode_config: LLMConfig
) -> None:
    """Assign and validate non-overlapping P/D KV event/token port ranges."""
    p_span = _port_span(prefill_config)
    d_span = _port_span(decode_config)
    p_experimental = prefill_config.experimental_configs
    d_experimental = decode_config.experimental_configs

    p_event_base = p_experimental.get(
        KV_EVENTS_PORT_BASE_KEY, DEFAULT_KV_EVENTS_PORT_BASE
    )
    p_token_base = p_experimental.get(
        KV_TOKEN_PORT_BASE_KEY, DEFAULT_KV_TOKEN_PORT_BASE
    )
    p_ranges = [
        _port_range("prefill KV events", p_event_base, p_span),
        _port_range(
            "prefill KV event replay",
            int(p_event_base) + DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET,
            p_span,
        ),
        _port_range("prefill prompt tokens", p_token_base, p_span),
    ]

    # Respect user-provided decode bases. Defaults start after every prefill
    # lane, then put decode prompt-token sockets after decode's replay lane.
    # This scales with configured autoscaling and DP capacity instead of a
    # fixed offset that silently overlaps as the fleet grows.
    next_available_port = max(port_range[2] for port_range in p_ranges) + 1
    d_event_base = d_experimental.setdefault(
        KV_EVENTS_PORT_BASE_KEY, next_available_port
    )
    d_token_base = d_experimental.setdefault(
        KV_TOKEN_PORT_BASE_KEY,
        int(d_event_base) + DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET + d_span,
    )
    d_ranges = [
        _port_range("decode KV events", d_event_base, d_span),
        _port_range(
            "decode KV event replay",
            int(d_event_base) + DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET,
            d_span,
        ),
        _port_range("decode prompt tokens", d_token_base, d_span),
    ]
    _validate_disjoint_port_ranges(p_ranges + d_ranges)


class PDServingArgs(BaseModelExtended):
    """Schema for P/D serving args.

    Defines the prefill and decode LLMConfigs plus ingress options.
    """

    prefill_config: Union[str, dict, LLMConfig]
    decode_config: Union[str, dict, LLMConfig]

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
        # P/D direct routing selects the pair in LLMRouter, so both fleets need
        # KV events and prompt-token receivers even though neither relies on a
        # KVAwareRouter deployment policy for its final dispatch.
        pd_config.prefill_config.experimental_configs["pd_kv_aware"] = True
        pd_config.decode_config.experimental_configs["pd_kv_aware"] = True
        pd_config.decode_config.experimental_configs.setdefault(
            "pending_decode_load_scale", 1.0
        )
        pd_config.decode_config.experimental_configs.setdefault(
            "pd_ticket_ttl_s", 120.0
        )
        _configure_pd_kv_port_ranges(pd_config.prefill_config, pd_config.decode_config)

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
            _build_openai_ingress_request_router(
                server=decode_deployment,
                llm_config=pd_config.decode_config,
                prefill_server=prefill_deployment,
                prefill_llm_config=pd_config.prefill_config,
            )
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
