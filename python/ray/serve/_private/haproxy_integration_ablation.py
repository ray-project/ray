import json
import os
from typing import Any, Dict, Iterable, Set

HAPROXY_INTEGRATION_ABLATION_CONFIG_PATH = (
    "/tmp/serve-haproxy-integration-ablation.json"
)

L1_CONTROLLER_TARGET_GROUP_BROADCAST = "L1"
L2_MANAGER_TARGET_GROUP_SUBSCRIPTION = "L2"
L3_CONTROLLER_FALLBACK_BROADCAST = "L3"
L4_MANAGER_FALLBACK_SUBSCRIPTION = "L4"
L5_MANAGER_POST_READY_UPDATE_APPLY = "L5"
L6_GRACEFUL_RELOAD_HANDOFF = "L6"
L7_RELOAD_OPTIMIZATION_BUNDLE = "L7"
L7A_RELOAD_SOCKET_TRANSFER = "L7a"
L7B_SERVER_STATE_PERSISTENCE = "L7b"
L8_DRAIN_UNDRAIN_INTEGRATION = "L8"

RUNTIME_ACTIVATED_CUTS = frozenset(
    {
        L1_CONTROLLER_TARGET_GROUP_BROADCAST,
        L2_MANAGER_TARGET_GROUP_SUBSCRIPTION,
        L3_CONTROLLER_FALLBACK_BROADCAST,
        L4_MANAGER_FALLBACK_SUBSCRIPTION,
        L5_MANAGER_POST_READY_UPDATE_APPLY,
    }
)


def read_haproxy_integration_ablation_config() -> Dict[str, Any]:
    path = HAPROXY_INTEGRATION_ABLATION_CONFIG_PATH
    if not os.path.exists(path):
        return {}

    try:
        with open(path) as f:
            payload = json.load(f)
    except Exception:
        return {}

    if not isinstance(payload, dict):
        return {}

    return payload


def normalize_cut_set(cut_set: Iterable[str] | None) -> Set[str]:
    normalized = set(cut_set or [])
    if L7_RELOAD_OPTIMIZATION_BUNDLE in normalized:
        normalized.add(L7A_RELOAD_SOCKET_TRANSFER)
        normalized.add(L7B_SERVER_STATE_PERSISTENCE)
    return normalized


def read_haproxy_integration_cut_set() -> Set[str]:
    payload = read_haproxy_integration_ablation_config()
    return normalize_cut_set(payload.get("cut_set"))


def read_direct_server_routing_enabled() -> bool:
    payload = read_haproxy_integration_ablation_config()
    return bool(payload.get("direct_server_routing", False))


def read_route_once_direct_backend_jump_enabled() -> bool:
    payload = read_haproxy_integration_ablation_config()
    return bool(payload.get("route_once_direct_backend_jump", False))
