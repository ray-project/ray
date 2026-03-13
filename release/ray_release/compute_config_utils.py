from typing import Dict, List

from ray_release.exception import MixedSchemaError


def is_new_schema(cluster_compute: dict) -> bool:
    """Detect whether a compute config dict uses the new Anyscale schema.

    New schema uses 'head_node' / 'worker_nodes'.
    Legacy schema uses 'head_node_type' / 'worker_node_types'.
    All new-schema fields are optional (the SDK provides defaults), so a
    config with neither legacy nor new node keys is treated as new-schema.
    Partial presence is accepted — e.g. a config with only 'head_node' but
    no 'worker_nodes' is treated as new-schema.
    Raises MixedSchemaError if both legacy and new schema keys are present.
    """
    has_new = "head_node" in cluster_compute or "worker_nodes" in cluster_compute
    has_legacy = (
        "head_node_type" in cluster_compute or "worker_node_types" in cluster_compute
    )
    if has_new and has_legacy:
        raise MixedSchemaError(
            "Compute config contains both legacy-schema and new-schema keys. "
            "Use only one schema."
        )
    return not has_legacy


def get_head_node_config(cluster_compute: Dict) -> Dict:
    """Return the head node config sub-dict, regardless of schema version."""
    if is_new_schema(cluster_compute):
        return cluster_compute.get("head_node") or {}
    return cluster_compute.get("head_node_type") or {}


def get_worker_node_configs(cluster_compute: Dict) -> List[Dict]:
    """Return the list of worker node config dicts, regardless of schema version."""
    if is_new_schema(cluster_compute):
        return cluster_compute.get("worker_nodes") or []
    return cluster_compute.get("worker_node_types") or []


def get_worker_min_count(worker: dict, new_schema: bool, default: int = 0) -> int:
    """Return the minimum node/worker count for a worker group dict."""
    if new_schema:
        return worker.get("min_nodes", default)
    return worker.get("min_workers", default)


def get_worker_max_count(worker: dict, new_schema: bool, default: int = 0) -> int:
    """Return the maximum node/worker count for a worker group dict."""
    if new_schema:
        return worker.get("max_nodes", default)
    return worker.get("max_workers", default)
