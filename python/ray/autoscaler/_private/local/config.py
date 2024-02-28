import copy
import os
from typing import Any, Dict

from ray._private.utils import get_ray_temp_dir
from ray.autoscaler._private.cli_logger import cli_logger

unsupported_field_message = "The field {} is not supported for on-premise clusters."

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
        LOCAL_CLUSTER_NODE_TYPE: {"node_config": {}, "resources": {}}
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
    if "max_worker_nodes" not in config:
        cli_logger.abort(
            "The field `max_worker_nodes` is required when using an "
            "automatically managed on-premise cluster."
        )
    node_type = config["available_node_types"][LOCAL_CLUSTER_NODE_TYPE]
    # The autoscaler no longer uses global `min_worker_nodes`.
    # Move `min_worker_nodes` to the node_type config.
    node_type["min_worker_nodes"] = config.pop("min_worker_nodes", 0)
    node_type["max_worker_nodes"] = config["max_worker_nodes"]
    return config


def prepare_manual(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validates and sets defaults for configs of manually managed on-prem
    clusters.

    - Checks for presence of required `worker_node_ips` and `head_ips` fields.
    - Defaults min and max workers to the number of `worker_node_ips`.
    - Caps min and max workers at the number of `worker_node_ips`.
    - Writes min and max worker info into the single worker node type.
    """
    config = copy.deepcopy(config)
    if ("worker_node_ips" not in config["provider"]) or (
        "head_ip" not in config["provider"]
    ):
        cli_logger.abort(
            "Please supply a `head_ip` and list of `worker_node_ips`. "
            "Alternatively, supply a `coordinator_address`."
        )
    num_ips = len(config["provider"]["worker_node_ips"])
    node_type = config["available_node_types"][LOCAL_CLUSTER_NODE_TYPE]
    # Default to keeping all provided ips in the cluster.
    config.setdefault("max_worker_nodes", num_ips)

    # The autoscaler no longer uses global `min_worker_nodes`.
    # We will move `min_worker_nodes` to the node_type config.
    min_worker_nodes = config.pop("min_worker_nodes", num_ips)
    max_worker_nodes = config["max_worker_nodes"]

    if min_worker_nodes > num_ips:
        cli_logger.warning(
            f"The value of `min_worker_nodes` supplied ({min_worker_nodes}) is greater"
            f" than the number of available worker ips ({num_ips})."
            f" Setting `min_worker_nodes={num_ips}`."
        )
        node_type["min_worker_nodes"] = num_ips
    else:
        node_type["min_worker_nodes"] = min_worker_nodes

    if max_worker_nodes > num_ips:
        cli_logger.warning(
            f"The value of `max_worker_nodes` supplied ({max_worker_nodes}) is greater"
            f" than the number of available worker ips ({num_ips})."
            f" Setting `max_worker_nodes={num_ips}`."
        )
        node_type["max_worker_nodes"] = num_ips
        config["max_worker_nodes"] = num_ips
    else:
        node_type["max_worker_nodes"] = max_worker_nodes

    if max_worker_nodes < num_ips:
        cli_logger.warning(
            f"The value of `max_worker_nodes` supplied ({max_worker_nodes}) is less"
            f" than the number of available worker ips ({num_ips})."
            f" At most {max_worker_nodes} Ray worker nodes will connect to the cluster."
        )

    return config


def get_lock_path(cluster_name: str) -> str:
    return os.path.join(get_ray_temp_dir(), "cluster-{}.lock".format(cluster_name))


def get_state_path(cluster_name: str) -> str:
    return os.path.join(get_ray_temp_dir(), "cluster-{}.state".format(cluster_name))


def bootstrap_local(config: Dict[str, Any]) -> Dict[str, Any]:
    return config
