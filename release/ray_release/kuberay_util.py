from ray_release.compute_config_utils import (
    get_head_node_config,
    get_worker_max_count,
    get_worker_min_count,
    get_worker_node_configs,
    is_new_schema,
)


def convert_cluster_compute_to_kuberay_compute_config(compute_config: dict) -> dict:
    """Convert cluster compute config to KubeRay compute config format.
    Args:
        compute_config: Original cluster compute configuration dict.
    Returns:
        Dict containing KubeRay-formatted compute configuration.
    """
    new_schema = is_new_schema(compute_config)
    head_node_resources = get_head_node_config(compute_config).get("resources", {})

    kuberay_worker_nodes = []
    for i, w in enumerate(get_worker_node_configs(compute_config)):
        worker_node_config = {
            "group_name": w.get("name") or f"worker-group-{i}-{w.get('instance_type', 'unknown')}",
            "min_nodes": get_worker_min_count(w, new_schema),
            "max_nodes": get_worker_max_count(w, new_schema),
        }
        if w.get("resources", {}):
            worker_node_config["resources"] = w["resources"]
        kuberay_worker_nodes.append(worker_node_config)

    config = {
        "head_node": {},
        "worker_nodes": kuberay_worker_nodes,
    }
    if head_node_resources:
        config["head_node"]["resources"] = head_node_resources
    return config
