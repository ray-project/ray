import logging
from typing import Any, Dict, Optional

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_EVENTS_PORT_BASE,
    KV_EVENTS_PORT_BASE_KEY,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


def configure_kv_events_for_kv_routing(llm_config: LLMConfig) -> None:
    """
    Enable engine KV-cache events for a KV-aware-routed deployment.
    """
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
        }
    )

    _pin_block_hash_seed(llm_config)


def _pin_block_hash_seed(llm_config: LLMConfig) -> None:
    """Make engine block hashes content-deterministic across replicas.

    The KV router's global indexer chains and dedups blocks by the engines'
    block hashes, so identical content must hash identically on every
    replica. vLLM salts its block-hash chain root per process unless
    ``PYTHONHASHSEED`` is set, so pin it deployment-wide.
    """
    runtime_env = dict(llm_config.runtime_env or {})
    env_vars = dict(runtime_env.get("env_vars") or {})
    env_vars.setdefault("PYTHONHASHSEED", "0")
    runtime_env["env_vars"] = env_vars
    llm_config.runtime_env = runtime_env


def assign_replica_kv_events_endpoint(llm_config: LLMConfig) -> None:
    """Pin the engine's KV-events endpoint to a per-replica port.

    Replicas of a deployment share one ``engine_kwargs``, so colocated
    replicas would otherwise bind the same ZMQ port. Offsets the configured
    base port by the replica's rank.
    """
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return
    base_endpoint = kv_events_config["endpoint"]
    if llm_config.engine_kwargs.get("data_parallel_rank") is not None:
        endpoint = base_endpoint
    else:
        endpoint = _offset_endpoint_port(base_endpoint, _replica_rank())
    llm_config.update_engine_kwargs(
        kv_events_config={**kv_events_config, "endpoint": endpoint}
    )


def resolve_kv_event_source_endpoint(llm_config: LLMConfig) -> Optional[str]:
    """This replica's node-routable KV-events endpoint, for the selection
    service to connect out to.

    The engine's KV-events endpoint at the replica's node IP; ``None`` when
    KV-cache events are not enabled.
    """
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return None
    return _engine_event_connect_endpoint(llm_config, kv_events_config)


def kv_event_routing_stats(
    llm_config: LLMConfig, max_num_batched_tokens: int
) -> Dict[str, Any]:
    """This replica's routing-stats payload advertising its KV-events endpoint.

    Surfaced to Serve via ``record_routing_stats`` and propagated to the
    deployment's ``KVRouterActor`` through ``LongPoll``: the actor's selection
    service connects out to ``endpoint`` and indexes the replica's KV events.
    ``max_num_batched_tokens`` and ``dp_rank`` are the engine-resolved facts the
    selection service needs to treat the replica as a schedulable worker.

    Empty when KV-cache events are not enabled (nothing to advertise).
    """
    endpoint = resolve_kv_event_source_endpoint(llm_config)
    if endpoint is None:
        return {}
    return {
        "kv_events": {
            "endpoint": endpoint,
            "max_num_batched_tokens": max_num_batched_tokens,
            "dp_rank": llm_config.engine_kwargs.get("data_parallel_rank") or 0,
        }
    }


def _engine_event_connect_endpoint(
    llm_config: LLMConfig, kv_events_config: Dict[str, Any]
) -> str:
    """The node-routable endpoint the engine's KV events are consumable from."""
    endpoint = kv_events_config["endpoint"]
    dp_rank = llm_config.engine_kwargs.get("data_parallel_rank")
    if dp_rank is not None:
        endpoint = _offset_endpoint_port(endpoint, dp_rank)
    port = endpoint.rsplit(":", 1)[1]
    # The engine binds the wildcard host; the selection service (which may run on
    # another node) connects to this replica's node-routable endpoint.
    return f"tcp://{ray.util.get_node_ip_address()}:{port}"


def _replica_rank() -> int:
    """This replica's rank on its node (ports are node-local)."""
    return serve.get_replica_context().rank.local_rank


def _default_kv_events_endpoint(llm_config: LLMConfig) -> str:
    port_base = int(
        llm_config.experimental_configs.get(
            KV_EVENTS_PORT_BASE_KEY, DEFAULT_KV_EVENTS_PORT_BASE
        )
    )
    return f"tcp://*:{port_base}"


def _offset_endpoint_port(endpoint: str, offset: int) -> str:
    """Offset a TCP endpoint's port with vLLM's ZmqEventPublisher convention."""
    base, port = endpoint.rsplit(":", 1)
    return f"{base}:{int(port) + offset}"


def derive_kv_event_block_size(engine_kwargs: Dict[str, Any]) -> int:
    """The engine's KV-cache block size, resolved at build time.

    The selection service indexes blocks at this size; it must match what the
    engine emits.
    """
    # This module loads in non-engine processes (Serve controller, router actor)
    # that must not import vLLM at module scope.
    from vllm.config import CacheConfig

    return CacheConfig(block_size=engine_kwargs.get("block_size")).block_size
