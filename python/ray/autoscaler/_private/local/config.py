import copy
from typing import Any
from typing import Dict

from ray.autoscaler._private.cli_logger import cli_logger

unsupported_field_message = ("The field {} is not supported "
                             "for on-premise clusters.")

NODE_TYPE = "ray.node"

def prepare_local(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare local cluster config for ingestion by cluster launcher and
    autoscaler.
    """
    config = copy.deepcopy(config)
    for field in "head_node", "worker_nodes", "available_node_types":
        if config.get(field):
            err_msg = unsupported_field_message.format(field)
            cli_logger.abort(err_msg)
    config["available_node_types"] = {NODE_TYPE:
                                        {
                                            "node_config":{},
                                            "resources": {}
                                        }
                                      }
    config["head_node_type"] = NODE_TYPE
    if "coordinator_address" in config["provider"]:
        config = prepare_coordinator(config)
    else:
        config = prepare_manual(config)
    # Presence of head_node and worker_nodes fields triggers legacy config
    # processing later in the bootstrap process.
    config["head_node"] = {"placeholder": {}}
    config["worker_nodes"] = {"placeholder": {}}
    return config


def prepare_coordinator(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    # User should explicitly set the max number of workers for the coordinator
    # to allocate.
    if "max_workers" not in config:
        cli_logger.abort("The field `max_workers` is required when using an "
                         "automatically managed on-premise cluster.")
    node_type = config["available_node_types"][NODE_TYPE]
    node_type["min_workers"] = config.pop("min_workers", None) or 0
    node_type["max_workers"] = config["max_workers"]
    return config


def prepare_manual(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    if ("worker_ips" not in config["provider"]) or (
            "head_ip" not in config["provider"]):
        cli_logger.abort("Please supply a `head_ip` and list of `worker_ips`. "
                         "Alternatively, supply a `coordinator_address`.")
    num_ips = len(config["provider"]["worker_ips"])
    node_type = config["available_node_types"][NODE_TYPE]
    # Default to keeping all provided ips in the cluster.
    config.setdefault("max_workers", num_ips)
    node_type["min_workers"] = config.pop("min_workers", None) or num_ips
    node_type["max_workers"] = config["max_workers"]
    return config


def is_local_manual(provider_config: Dict[str, Any]) -> bool:
    """Is this a LocalNodeProvider that has manually specified IPs?"""
    return (provider_config["type"] == "local"
            and "coordinator_address" not in provider_config)


def sync_state(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds cluster state file to file mounts.

    Used to synchronize head and local cluster state files, mostly to let the
    head node know that the head node itself is non-terminated.
    """
    config = copy.deepcopy(config)
    cluster_name = config["cluster_name"]
    state_path = get_state_path(cluster_name)
    config["file_mounts"][state_path] = state_path
    return config


def get_lock_path(cluster_name: str) -> str:
    return "/tmp/cluster-{}.lock".format(cluster_name)


def get_state_path(cluster_name: str) -> str:
    return "/tmp/cluster-{}.state".format(cluster_name)


def bootstrap_local(config: Dict[str, Any]) -> Dict[str, Any]:
    return config
