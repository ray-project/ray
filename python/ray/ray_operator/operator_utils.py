import copy
import logging
import os
from typing import Any, Dict, Iterator

from kubernetes.watch import Watch

from ray.autoscaler._private.kubernetes import custom_objects_api
from ray.autoscaler._private.providers import _get_default_config

RAY_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE")

RAY_CONFIG_DIR = os.environ.get("RAY_CONFIG_DIR") or \
    os.path.expanduser("~/ray_cluster_configs")

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
    cluster_name = cluster_resource["metadata"]["name"]
    config["available_node_types"] = get_node_types(cluster_resource,
                                                    cluster_name)
    config["cluster_name"] = cluster_name
    config["provider"] = get_provider_config(cluster_name)
    return config


def get_node_types(cluster_resource: Dict[str, Any], cluster_name) ->\
        Dict[str, Any]:
    cluster_owner_reference = get_cluster_owner_reference(
        cluster_resource, cluster_name)
    node_types = {}
    for pod_type in cluster_resource["spec"]["podTypes"]:
        name = pod_type["name"]
        pod_type_copy = copy.deepcopy(pod_type)
        pod_type_copy.pop("name")
        node_type = translate(pod_type_copy, dictionary=NODE_TYPE_FIELDS)
        metadata = node_type["node_config"]["metadata"]
        metadata.update({"ownerReferences": [cluster_owner_reference]})
        if name == cluster_resource["spec"]["headPodType"]:
            if "labels" not in metadata:
                metadata["labels"] = {}
            metadata["labels"].update(head_service_selector(cluster_name))
        node_types[name] = node_type
    return node_types


def get_provider_config(cluster_name):
    default_kubernetes_config = _get_default_config({"type": "kubernetes"})
    default_provider_conf = default_kubernetes_config["provider"]

    # Configure head service for dashboard and client
    head_service = copy.deepcopy(default_provider_conf["services"][0])
    service_name = f"{cluster_name}-ray-head"
    head_service["metadata"]["name"] = service_name
    head_service["spec"]["selector"] = head_service_selector(cluster_name)

    provider_conf = {}
    provider_conf["type"] = "kubernetes"
    provider_conf["use_internal_ips"] = True
    provider_conf["namespace"] = RAY_NAMESPACE
    provider_conf["services"] = [head_service]
    return provider_conf


def head_service_selector(cluster_name):
    return {"component": f"{cluster_name}-ray-head"}


def get_cluster_owner_reference(cluster_resource: Dict[str, Any],
                                cluster_name: str) -> Dict[str, Any]:
    return {
        "apiVersion": cluster_resource["apiVersion"],
        "kind": cluster_resource["kind"],
        "blockOwnerDeletion": True,
        "controller": True,
        "name": cluster_name,
        "uid": cluster_resource["metadata"]["uid"]
    }


def translate(configuration: Dict[str, Any],
              dictionary: Dict[str, str]) -> Dict[str, Any]:
    return {
        dictionary[field]: configuration[field]
        for field in dictionary if field in configuration
    }


def set_status(cluster_cr: Dict[str, Any], cluster_name: str,
               status: str) -> None:
    # TODO: Add retry logic in case of 409 due to old resource version.
    cluster_cr["status"] = {"phase": status}
    custom_objects_api()\
        .patch_namespaced_custom_object_status(namespace=RAY_NAMESPACE,
                                               group="cluster.ray.io",
                                               version="v1",
                                               plural="rayclusters",
                                               name=cluster_name,
                                               body=cluster_cr)
