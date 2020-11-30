import copy
import logging
import math
import os
from typing import Any, Dict, List, Union

from kubernetes import watch
import yaml

from ray.autoscaler._private.kubernetes import core_api, custom_objects_api

RAY_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE")

RAY_CONFIG_DIR = os.path.expanduser("~/ray_cluster_configs")
CONFIG_SUFFIX = "_config.yaml"

CONFIG_FIELDS = {
    "targetUtilizationFraction": "target_utilization_fraction",
    "idleTimeoutMinutes": "idle_timeout_minutes",
    "headPodType": "head_node_type",
    "workerDefaultPodType": "worker_default_node_type",
    "workerStartRayCommands": "worker_start_ray_commands",
    "headStartRayCommands": "head_start_ray_commands",
    "podTypes": "available_node_types"
}

NODE_TYPE_FIELDS = {
    "minWorkers": "min_workers",
    "maxWorkers": "max_workers",
    "podConfig": "node_config",
}

PROVIDER_CONFIG = {
    "type": "kubernetes",
    "use_internal_ips": True,
    "namespace": RAY_NAMESPACE
}

root_logger = logging.getLogger("ray")
root_logger.setLevel(logging.getLevelName("DEBUG"))


def config_path(cluster):
    file_name = cluster + CONFIG_SUFFIX
    return os.path.join(RAY_CONFIG_DIR, file_name)


def write_config(config, config_path):
    with open(config_path, "w") as file:
        yaml.dump(config, file)


def cluster_cr_stream():
    w = watch.Watch()
    return w.stream(
        custom_objects_api().list_namespaced_custom_object,
        namespace=RAY_NAMESPACE,
        group="cluster.ray.io",
        version="v1",
        plural="rayclusters")


def cr_to_config(cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    cr_spec = cluster_resource["spec"]
    config = translate(cr_spec, dictionary=CONFIG_FIELDS)
    pod_types = cr_spec["podTypes"]
    config["available_node_types"] = get_node_types(pod_types)
    config["cluster_name"] = cluster_resource["metadata"]["name"]
    config["provider"] = PROVIDER_CONFIG
    return config


def get_node_types(pod_types: List[Dict[str, Any]]) -> Dict[str, Any]:
    node_types = {}
    for pod_type in pod_types:
        name = pod_type["name"]
        pod_type_copy = copy.deepcopy(pod_type)
        pod_type_copy.pop("name")
        node_types[name] = translate(
            pod_type_copy, dictionary=NODE_TYPE_FIELDS)
        pod_config = pod_type_copy["podConfig"]
        node_types[name]["resources"] = get_node_type_resources(pod_config)
    return node_types


def translate(configuration: Dict[str, Any],
              dictionary: Dict[str, str]) -> Dict[str, Any]:
    return {dictionary[field]: configuration[field] for field in configuration}


def get_node_type_resources(pod_config: Dict[str, Any]) -> Dict[str, int]:
    pod_resources = pod_config["spec"]["containers"][0].get("resources", None)
    if pod_resources is None:
        return {"CPU": 0, "GPU": 0}

    node_type_resources = {
        resource_name.upper(): get_resource(pod_resources, resource_name)
        for resource_name in ["cpu", "gpu"]
    }

    return node_type_resources


def get_resource(pod_resources, resource_name) -> int:
    request = _get_resource(
        pod_resources, resource_name, field_name="requests")
    limit = _get_resource(pod_resources, resource_name, field_name="limits")
    resource = min(request, limit)
    return 0 if resource == float("inf") else int(resource)


def _get_resource(pod_resources, resource_name,
                  field_name) -> Union[int, float]:
    if (field_name in pod_resources
            and resource_name in pod_resources[field_name]):
        return _parse_resource(pod_resources[field_name][resource_name])
    else:
        return float("inf")


def _parse_resource(resource):
    resource_str = str(resource)
    if resource_str[-1] == "m":
        return math.ceil(int(resource_str[:-1]) / 1000)
    else:
        return int(resource_str)


def get_ray_head_pod_ip(config: Dict[str, Any]) -> str:
    cluster_name = config["cluster_name"]
    label_selector = f"ray-node-type=head,ray-cluster-name={cluster_name}"
    pods = core_api().list_namespaced_pod(
        namespace=RAY_NAMESPACE, label_selector=label_selector).items
    assert (len(pods)) == 1
    head_pod = pods.pop()
    return head_pod.status.pod_ip
