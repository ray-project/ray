import copy
from typing import Any
from typing import Dict

from ray.autoscaler._private.cli_logger import cli_logger

unsupported_field_message = ("The field {} is not supported "
                             "for on-premise clusters.")


def prepare_local(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    for field in "head_node", "worker_nodes", "available_node_types":
        if config.get("head_node"):
            err_msg = unsupported_field_message.format(field)
            cli_logger.error(err_msg)
    if "coordinator_address" in config["provider"]:
        config = prepare_coordinator(config)
    else:
        config = prepare_manual(config)
    # Presence of head_node and worker_nodes fields triggers legacy config
    # processing later in the bootstrap process.
    config["head_node"] = {"placeholder":{}}
    config["worker_nodes"] = {"placeholder":{}}
    return config


def prepare_coordinator(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    if "max_workers" not in config:
        cli_logger.error("The field `max_workers` is required when using an "
                         "automatically managed on-premise cluster.")
    config.setdefault("min_workers", 0)
    return config


def prepare_manual(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    if "worker_ips" not in config["provider"]:
        cli_logger.error("Please supply a list of `worker_ips` "
                         "or a `coordinator_address`.")
    num_ips = len(config["provider"]["worker_ips"])
    max_specified = "max_workers" in config
    min_specified = "min_workers" in config
    if not max_specified and not min_specified:
        # The most common use-case.
        config["min_workers"] = num_ips
        config["max_workers"] = num_ips
    elif max_specified and not min_specified:
        # Indicates intent to cap autoscaler usage.
        # Infer conservative default.
        config["min_workers"] = 0
        cli_logger.warning("Inferring `min_workers:0`. Ray will start only on "
                           "the head node. Ray may start on other nodes, "
                           "according to workload.")
    elif not max_specified and min_specified:
        config["max_workers"] = num_ips

    return config


def is_local_manual(provider_config: Dict[str, Any]) -> bool:
    return (provider_config["type"] == "local"
            and "coordinator_address" not in provider_config)


def sync_state(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Used to synchronize head and local cluster state files.
    """
    config = copy.deepcopy(config)
    cluster_name = config["cluster_name"]
    lock_path = get_lock_path(cluster_name)
    state_path = get_state_path(cluster_name)
    config["file_mounts"][lock_path] = lock_path
    config["file_mounts"][state_path] = state_path
    return config


def get_lock_path(cluster_name: str) -> str:
    return "/tmp/cluster-{}.lock".format(cluster_name)


def get_state_path(cluster_name: str) -> str:
    return "/tmp/cluster-{}.state".format(cluster_name)


def bootstrap_local(config: Dict[str, Any]) -> Dict[str, Any]:
    return config
