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
    if "max_workers" not in config:
        cli_logger.abort(
            "The field `max_workers` is required when using an "
            "automatically managed on-premise cluster."
        )
    node_type = config["available_node_types"][LOCAL_CLUSTER_NODE_TYPE]
    # The autoscaler no longer uses global `min_workers`.
    # Move `min_workers` to the node_type config.
    node_type["min_workers"] = config.pop("min_workers", 0)
    node_type["max_workers"] = config["max_workers"]
    return config


def prepare_manual(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validates and sets defaults for configs of manually managed on-prem
    clusters.

    - Checks for presence of required `worker_ips` and `head_ips` fields.
    - Defaults min and max workers to the number of `worker_ips`.
    - Caps min and max workers at the number of `worker_ips`.
    - Writes min and max worker info into the single worker node type.
    """
    config = copy.deepcopy(config)
    if ("worker_ips" not in config["provider"]) or (
        "head_ip" not in config["provider"]
    ):
        cli_logger.abort(
            "Please supply a `head_ip` and list of `worker_ips`. "
            "Alternatively, supply a `coordinator_address`."
        )
    num_ips = len(config["provider"]["worker_ips"])
    node_type = config["available_node_types"][LOCAL_CLUSTER_NODE_TYPE]
    # Default to keeping all provided ips in the cluster.
    config.setdefault("max_workers", num_ips)

    # The autoscaler no longer uses global `min_workers`.
    # We will move `min_workers` to the node_type config.
    min_workers = config.pop("min_workers", num_ips)
    max_workers = config["max_workers"]

    if min_workers > num_ips:
        cli_logger.warning(
            f"The value of `min_workers` supplied ({min_workers}) is greater"
            f" than the number of available worker ips ({num_ips})."
            f" Setting `min_workers={num_ips}`."
        )
        node_type["min_workers"] = num_ips
    else:
        node_type["min_workers"] = min_workers

    if max_workers > num_ips:
        cli_logger.warning(
            f"The value of `max_workers` supplied ({max_workers}) is greater"
            f" than the number of available worker ips ({num_ips})."
            f" Setting `max_workers={num_ips}`."
        )
        node_type["max_workers"] = num_ips
        config["max_workers"] = num_ips
    else:
        node_type["max_workers"] = max_workers

    if max_workers < num_ips:
        cli_logger.warning(
            f"The value of `max_workers` supplied ({max_workers}) is less"
            f" than the number of available worker ips ({num_ips})."
            f" At most {max_workers} Ray worker nodes will connect to the cluster."
        )

    return config


def get_lock_path(cluster_name: str) -> str:
    return os.path.join(get_ray_temp_dir(), "cluster-{}.lock".format(cluster_name))


def get_state_path(cluster_name: str) -> str:
    return os.path.join(get_ray_temp_dir(), "cluster-{}.state".format(cluster_name))


def bootstrap_local(config: Dict[str, Any]) -> Dict[str, Any]:
    return config
