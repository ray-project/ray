import copy
from typing import Any
from typing import Dict

from ray.autoscaler._private.cli_logger import cli_logger

unsupported_field_message = ("The field {} is not supported "
                             "for on-premise clusters.")

LOCAL_CLUSTER_NODE_TYPE = "local.cluster.node"


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
    # We use a config with a single node type for on-prem clusters.
    # Resources internally detected by Ray are not overridden by the autoscaler
    # (see NodeProvider.do_update)
    config["available_node_types"] = {
        LOCAL_CLUSTER_NODE_TYPE: {
            "node_config": {},
            "resources": {}
        }
    }
    config["head_node_type"] = LOCAL_CLUSTER_NODE_TYPE
    if "coordinator_address" in config["provider"]:
        config = prepare_coordinator(config)
    else:
        config = prepare_manual(config)
    return config


def prepare_coordinator(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    # User should explicitly set the max number of workers for the coordinator
    # to allocate.
    if "max_workers" not in config:
        cli_logger.abort("The field `max_workers` is required when using an "
                         "automatically managed on-premise cluster.")
    node_type = config["available_node_types"][LOCAL_CLUSTER_NODE_TYPE]
    # The autoscaler no longer uses global `min_workers`.
    # Move `min_workers` to the node_type config.
    node_type["min_workers"] = config.pop("min_workers", 0)
    node_type["max_workers"] = config["max_workers"]
    return config


def prepare_manual(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    if ("worker_ips" not in config["provider"]) or (
            "head_ip" not in config["provider"]):
        cli_logger.abort("Please supply a `head_ip` and list of `worker_ips`. "
                         "Alternatively, supply a `coordinator_address`.")
    num_ips = len(config["provider"]["worker_ips"])
    node_type = config["available_node_types"][LOCAL_CLUSTER_NODE_TYPE]
    # Default to keeping all provided ips in the cluster.
    config.setdefault("max_workers", num_ips)
    # The autoscaler no longer uses global `min_workers`.
    # Move `min_workers` to the node_type config.
    node_type["min_workers"] = config.pop("min_workers", num_ips)
    node_type["max_workers"] = config["max_workers"]
    return config


def get_lock_path(cluster_name: str) -> str:
    return "/tmp/cluster-{}.lock".format(cluster_name)


def get_state_path(cluster_name: str) -> str:
    return "/tmp/cluster-{}.state".format(cluster_name)


def bootstrap_local(config: Dict[str, Any]) -> Dict[str, Any]:
    return config
