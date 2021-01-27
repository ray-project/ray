import copy
import logging
import os
from typing import Any, Dict, Iterator

from kubernetes.watch import Watch

from ray.autoscaler._private.kubernetes import custom_objects_api

RAY_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE")

RAY_CONFIG_DIR = os.path.expanduser("~/ray_cluster_configs")
CONFIG_SUFFIX = "_config.yaml"

CONFIG_FIELDS = {
    "maxWorkers": "max_workers",
    "upscalingSpeed": "upscaling_speed",
    "idleTimeoutMinutes": "idle_timeout_minutes",
    "headPodType": "head_node_type",
    "workerStartRayCommands": "worker_start_ray_commands",
    "headStartRayCommands": "head_start_ray_commands",
    "podTypes": "available_node_types"
}

NODE_TYPE_FIELDS = {
    "minWorkers": "min_workers",
    "maxWorkers": "max_workers",
    "podConfig": "node_config",
    "rayResources": "resources",
    "setupCommands": "worker_setup_commands"
}

PROVIDER_CONFIG = {
    "type": "kubernetes",
    "use_internal_ips": True,
    "namespace": RAY_NAMESPACE
}

root_logger = logging.getLogger("ray")
root_logger.setLevel(logging.getLevelName("DEBUG"))


def config_path(cluster_name: str) -> str:
    file_name = cluster_name + CONFIG_SUFFIX
    return os.path.join(RAY_CONFIG_DIR, file_name)


def cluster_cr_stream() -> Iterator:
    w = Watch()
    return w.stream(
        custom_objects_api().list_namespaced_custom_object,
        namespace=RAY_NAMESPACE,
        group="cluster.ray.io",
        version="v1",
        plural="rayclusters")


def cr_to_config(cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    """Convert RayCluster custom resource to a ray cluster config for use by the
    autoscaler."""
    config = translate(cluster_resource["spec"], dictionary=CONFIG_FIELDS)
    config["available_node_types"] = get_node_types(cluster_resource)
    config["cluster_name"] = cluster_resource["metadata"]["name"]
    config["provider"] = PROVIDER_CONFIG
    return config


def get_node_types(cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    cluster_owner_reference = get_cluster_owner_reference(cluster_resource)
    node_types = {}
    for pod_type in cluster_resource["spec"]["podTypes"]:
        name = pod_type["name"]
        pod_type_copy = copy.deepcopy(pod_type)
        pod_type_copy.pop("name")
        node_types[name] = translate(
            pod_type_copy, dictionary=NODE_TYPE_FIELDS)
        # Deleting a RayCluster CR will also delete the associated pods.
        node_types[name]["node_config"]["metadata"].update({
            "ownerReferences": [cluster_owner_reference]
        })
    return node_types


def get_cluster_owner_reference(
        cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "apiVersion": cluster_resource["apiVersion"],
        "kind": cluster_resource["kind"],
        "blockOwnerDeletion": True,
        "controller": True,
        "name": cluster_resource["metadata"]["name"],
        "uid": cluster_resource["metadata"]["uid"]
    }


def translate(configuration: Dict[str, Any],
              dictionary: Dict[str, str]) -> Dict[str, Any]:
    return {
        dictionary[field]: configuration[field]
        for field in dictionary if field in configuration
    }
