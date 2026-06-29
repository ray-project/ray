import logging
from typing import Any, Dict, Optional

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_KV_EVENTS_PORT_BASE,
    DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET,
    KV_EVENTS_PORT_BASE_KEY,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
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
            # Bind a replay (ROUTER) socket so the selection service can recover
            # the events it missed before its SUB connected (slow-joiner gap).
            "replay_endpoint": _default_kv_events_replay_endpoint(llm_config),
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
    if not is_kv_aware(llm_config.deployment_config.get("request_router_config")):
        return
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return
    updated = dict(kv_events_config)
    endpoint = updated.pop("endpoint")
    replay_endpoint = updated.pop("replay_endpoint")
    # With data parallelism the engine offsets ports by dp_rank under ZmqEventPublisher;
    # otherwise offset by the replica's node-local rank so colocated replicas don't collide.
    if llm_config.engine_kwargs.get("data_parallel_rank") is not None:
        offset = 0
    else:
        offset = _get_replica_rank()
    updated["endpoint"] = _get_offset_endpoint_port(endpoint, offset)
    updated["replay_endpoint"] = _get_offset_endpoint_port(replay_endpoint, offset)
    llm_config.update_engine_kwargs(kv_events_config=updated)


def resolve_kv_event_source_endpoint(llm_config: LLMConfig) -> Optional[str]:
    """This replica's node-routable KV-events endpoint, for the selection
    service to connect out to.

    The engine's KV-events endpoint at the replica's node IP; ``None`` when
    KV-cache events are not enabled.
    """
    if not is_kv_aware(llm_config.deployment_config.get("request_router_config")):
        return None
    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    if kv_events_config is None:
        return None
    return _get_node_routable_endpoint(llm_config, kv_events_config["endpoint"])


def get_kv_event_routing_stats(
    llm_config: LLMConfig, block_size: int, max_num_batched_tokens: int
) -> Dict[str, Any]:
    """Returns this replica's routing-stats payload advertising its KV-events
    endpoint and the engine's resolved KV-cache block size."""
    if not is_kv_aware(llm_config.deployment_config.get("request_router_config")):
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


def _get_offset_endpoint_port(endpoint: str, offset: int) -> str:
    """Offset a TCP endpoint's port with vLLM's ZmqEventPublisher convention."""
    base, port = endpoint.rsplit(":", 1)
    return f"{base}:{int(port) + offset}"
