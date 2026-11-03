import logging
from typing import Any, Dict, Optional

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_EVENTS_PORT_BASE,
    DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET,
    DEFAULT_KV_TOKEN_PORT_BASE,
    KV_EVENTS_PORT_BASE_KEY,
    KV_TOKEN_METADATA_KEY,
    KV_TOKEN_PORT_BASE_KEY,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


def configure_kv_events_for_kv_routing(llm_config: LLMConfig) -> None:
    """Enable engine KV-cache events for a KV-aware-routed deployment."""
    engine_kwargs = llm_config.engine_kwargs
    if engine_kwargs.get("enable_prefix_caching") is False:
        logger.warning(
            "KV-aware routing is configured but enable_prefix_caching is False; "
            "the engine will not emit KV-cache events."
        )

    llm_config.update_engine_kwargs(
        kv_events_config={
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": _default_kv_events_endpoint(llm_config),
            # Bind a replay (ROUTER) socket so the selection service can recover
            # the events it missed before its SUB connected (slow-joiner gap).
            "replay_endpoint": _default_kv_events_replay_endpoint(llm_config),
        }
    )

    _enable_storage_tier_kv_events(llm_config)
    _configure_runtime_env_for_kv_routing(llm_config)


def _tiering_extra_config(engine_kwargs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The extra config of a top-level ``OffloadingConnector`` running
    ``TieringOffloadingSpec``, the only spec that reads ``secondary_tiers``.

    Returns None for any other connector or spec, and for malformed configs,
    which are left for vLLM's own validation to report.
    """
    kv_transfer_config = engine_kwargs.get("kv_transfer_config")
    if (
        not isinstance(kv_transfer_config, dict)
        or kv_transfer_config.get("kv_connector") != "OffloadingConnector"
    ):
        return None
    extra_config = kv_transfer_config.get("kv_connector_extra_config")
    if (
        not isinstance(extra_config, dict)
        or extra_config.get("spec_name") != "TieringOffloadingSpec"
        or not isinstance(extra_config.get("secondary_tiers"), list)
    ):
        return None
    return extra_config


def _enable_storage_tier_kv_events(llm_config: LLMConfig) -> None:
    """Publish KV events from local filesystem offload tiers.

    vLLM's tiering offload spec emits ``STORAGE`` events from a filesystem tier
    only when the tier sets ``enable_kv_events``. Default it on so the router
    tracks disk-resident prefixes the same way it tracks CPU-offloaded ones. An
    explicit value is kept. Object-store tiers are left alone: their storage is
    usually not local to the replica, and the router indexes a tier's events as
    local unless the tier sets ``locality="REMOTE"``.
    """

    def needs_default(tier: Any) -> bool:
        return (
            isinstance(tier, dict)
            and tier.get("type") == "fs"
            and "enable_kv_events" not in tier
        )

    extra_config = _tiering_extra_config(llm_config.engine_kwargs)
    if extra_config is None:
        return
    tiers = extra_config["secondary_tiers"]
    if not any(needs_default(tier) for tier in tiers):
        return
    tiers = [
        {**tier, "enable_kv_events": True} if needs_default(tier) else tier
        for tier in tiers
    ]
    llm_config.update_engine_kwargs(
        kv_transfer_config={
            **llm_config.engine_kwargs["kv_transfer_config"],
            "kv_connector_extra_config": {**extra_config, "secondary_tiers": tiers},
        }
    )


def enable_native_kv_offload_events(vllm_config: Any) -> None:
    """Make vLLM's native offload events usable by the KV router.

    Applies whenever the engine runs vLLM's ``OffloadingConnector``, whether it
    was enabled through ``kv_offloading_size`` or configured directly in
    ``kv_transfer_config``.
    """
    kv_transfer_config = vllm_config.kv_transfer_config
    if (
        kv_transfer_config is None
        or kv_transfer_config.kv_connector != "OffloadingConnector"
    ):
        return

    kv_transfer_config.kv_connector_extra_config["self_describing_kv_events"] = True


def _configure_runtime_env_for_kv_routing(llm_config: LLMConfig) -> None:
    """Configure vLLM process-wide settings required by KV-aware routing.

    The KV router's global indexer chains and dedups blocks by the engines'
    block hashes, so identical content must hash identically on every
    replica. vLLM salts its block-hash chain root per process unless
    ``PYTHONHASHSEED`` is set, so pin it deployment-wide.

    Native offloading must use ``OffloadingConnector`` because it emits the
    self-describing CPU-tier events consumed by the KV router.
    """
    runtime_env = dict(llm_config.runtime_env or {})
    env_vars = dict(runtime_env.get("env_vars") or {})
    env_vars.setdefault("PYTHONHASHSEED", "0")
    env_vars["VLLM_USE_SIMPLE_KV_OFFLOAD"] = "0"
    runtime_env["env_vars"] = env_vars
    llm_config.runtime_env = runtime_env


def assign_replica_kv_events_endpoint(llm_config: LLMConfig) -> None:
    """Pin the engine's KV-events endpoint to a per-replica port.

    Replicas of a deployment share one ``engine_kwargs``, so colocated
    replicas would otherwise bind the same ZMQ port. Offsets the configured
    base port so the engine binds at base + node-local replica rank.
    """
    if not is_kv_aware(llm_config):
        return
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return
    updated = dict(kv_events_config)
    endpoint = updated.pop("endpoint")
    replay_endpoint = updated.pop("replay_endpoint")
    # vllm.distributed.kv_events.ZmqEventPublisher adds dp_rank itself. Subtract it here so the
    # final port uses the node-local replica rank, even when multiple DP groups share a node.
    dp_rank = llm_config.engine_kwargs.get("data_parallel_rank") or 0
    offset = _get_replica_rank() - dp_rank
    updated["endpoint"] = _get_offset_endpoint_port(endpoint, offset)
    updated["replay_endpoint"] = _get_offset_endpoint_port(replay_endpoint, offset)
    llm_config.update_engine_kwargs(kv_events_config=updated)


def resolve_kv_event_source_endpoint(llm_config: LLMConfig) -> Optional[str]:
    """This replica's node-routable KV-events endpoint, for the selection
    service to connect out to.

    The engine's KV-events endpoint at the replica's node IP; ``None`` when
    KV-cache events are not enabled.
    """
    if not is_kv_aware(llm_config):
        return None
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return None
    return _get_node_routable_endpoint(llm_config, kv_events_config["endpoint"])


def resolve_kv_event_block_size(vllm_config: Any) -> int:
    """Token span of the KV blocks the engine hashes and publishes.

    The KV router must register this span, or it drops the engine's KV
    events. Under decode context parallelism (DCP) it is
    ``block_size * decode_context_parallel_size``, not the per-rank
    ``cache_config.block_size``.

    Uses the engine-reported ``cache_config.effective_attention_block_size``
    when available, else ``block_size * decode_context_parallel_size``.
    """
    cache_config = vllm_config.cache_config
    effective_block_size = getattr(cache_config, "effective_attention_block_size", None)
    if effective_block_size:
        return effective_block_size
    dcp_size = (
        getattr(vllm_config.parallel_config, "decode_context_parallel_size", 1) or 1
    )
    return cache_config.block_size * dcp_size


def get_kv_event_routing_stats(
    llm_config: LLMConfig, block_size: int, max_num_batched_tokens: int
) -> Dict[str, Any]:
    """Returns this replica's routing-stats payload advertising its KV-events
    endpoint and the engine's resolved KV-cache block size."""
    if not is_kv_aware(llm_config):
        return {}
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return {}
    kv_event_metadata = {
        "endpoint": _get_node_routable_endpoint(
            llm_config, kv_events_config["endpoint"]
        ),
        "block_size": block_size,
        "max_num_batched_tokens": max_num_batched_tokens,
        "dp_rank": llm_config.engine_kwargs.get("data_parallel_rank") or 0,
        "replay_endpoint": _get_node_routable_endpoint(
            llm_config, kv_events_config["replay_endpoint"]
        ),
    }
    return {"kv_event_metadata": kv_event_metadata}


def get_token_channel_endpoints(
    llm_config: LLMConfig,
) -> Optional[tuple[str, str]]:
    """Return (bind_endpoint, advertised_endpoint) for prompt-token ZMQ.

    This is a per-LLMServer-replica token channel, not a vLLM engine socket. Use
    the Serve replica's node-local rank to avoid collisions among colocated
    replicas, independent of vLLM data-parallel rank.
    """
    if not is_kv_aware(llm_config):
        return None
    port = _default_prompt_token_port(llm_config) + _get_replica_rank()
    return f"tcp://*:{port}", f"tcp://{ray.util.get_node_ip_address()}:{port}"


def get_prompt_token_routing_stats(endpoint: Optional[str]) -> Dict[str, Any]:
    """Return routing stats advertising this replica's prompt-token endpoint."""
    if not endpoint:
        return {}
    return {KV_TOKEN_METADATA_KEY: {"endpoint": endpoint}}


def _get_node_routable_endpoint(llm_config: LLMConfig, endpoint: str) -> str:
    """Rewrite a wildcard-bound engine endpoint to this replica's node IP.

    Offsets by ``data_parallel_rank`` to match the engine's own per-rank offset,
    so the selection service dials the right socket.
    """
    dp_rank = llm_config.engine_kwargs.get("data_parallel_rank")
    if dp_rank is not None:
        endpoint = _get_offset_endpoint_port(endpoint, dp_rank)
    port = endpoint.rsplit(":", 1)[1]
    return f"tcp://{ray.util.get_node_ip_address()}:{port}"


def _get_replica_rank() -> int:
    return serve.get_replica_context().rank.local_rank


def _default_kv_events_endpoint(llm_config: LLMConfig) -> str:
    port_base = int(
        llm_config.experimental_configs.get(
            KV_EVENTS_PORT_BASE_KEY, DEFAULT_KV_EVENTS_PORT_BASE
        )
    )
    return f"tcp://*:{port_base}"


def _default_kv_events_replay_endpoint(llm_config: LLMConfig) -> str:
    port_base = int(
        llm_config.experimental_configs.get(
            KV_EVENTS_PORT_BASE_KEY, DEFAULT_KV_EVENTS_PORT_BASE
        )
    )
    return f"tcp://*:{port_base + DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET}"


def _default_prompt_token_port(llm_config: LLMConfig) -> int:
    return int(
        llm_config.experimental_configs.get(
            KV_TOKEN_PORT_BASE_KEY, DEFAULT_KV_TOKEN_PORT_BASE
        )
    )


def _get_offset_endpoint_port(endpoint: str, offset: int) -> str:
    """Offset a TCP endpoint's port with vLLM's ZmqEventPublisher convention."""
    base, port = endpoint.rsplit(":", 1)
    return f"{base}:{int(port) + offset}"
