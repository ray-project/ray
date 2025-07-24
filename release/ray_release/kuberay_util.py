def convert_cluster_compute_to_kuberay_compute_config(compute_config: dict) -> dict:
    """Convert cluster compute config to KubeRay compute config format.
    Args:
        compute_config: Original cluster compute configuration dict.
    Returns:
        Dict containing KubeRay-formatted compute configuration.
    """
    worker_node_types = compute_config["worker_node_types"]
    head_node_resources = compute_config.get("head_node_type", {}).get("resources", {})

    kuberay_worker_nodes = []
    for worker_node_type in worker_node_types:
        worker_node_config = {
            "group_name": worker_node_type.get("name"),
            "min_nodes": worker_node_type.get("min_workers"),
            "max_nodes": worker_node_type.get("max_workers"),
        }
        if worker_node_type.get("resources", {}):
            worker_node_config["resources"] = worker_node_type.get("resources", {})
        kuberay_worker_nodes.append(worker_node_config)

    config = {
        "head_node": {},
        "worker_nodes": kuberay_worker_nodes,
    }
    if head_node_resources:
        config["head_node"]["resources"] = head_node_resources
    return config
