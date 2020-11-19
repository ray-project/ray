import copy
import os
from typing import Any, Dict, IO, List, Tuple

import yaml

from ray.autoscaler._private.kubernetes import core_api, custom_objects_api
from ray.utils import open_log

RAY_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE")

RAY_CONFIG_DIR = "/root"
RAY_CONFIG_FILE = "/ray_cluster_config.yaml"
CLUSTER_CONFIG_PATH = os.path.join(RAY_CONFIG_DIR, RAY_CONFIG_FILE)

LOG_DIR = "/root/ray-operator-logs"
ERR_NAME, OUT_NAME = "ray-operator.err", "ray-operator.out"

CONFIG_FIELDS = {
    "targetUtilizationFraction": "target_utilization_fraction",
    "idleTimeoutMinutes": "idle_time_out_minutes",
    "headPodType": "head_node_type",
    "podTypes": "available_node_types",
    "workerDefaultPodType": "worker_default_node_type"
}

NODE_TYPE_FIELDS = {
    "minWorkers": "min_workers",
    "maxWorkers": "max_workers",
    "podConfig": "node_config"
}


def prepare_ray_cluster_config():
    cluster_resource = get_cluster_CR()
    config = cr_to_config(cluster_resource)
    with open(CLUSTER_CONFIG_PATH, "w") as file:
        yaml.dump(config, file)


def get_cluster_CR():
    ray_cluster_CR = custom_objects_api().list_namespaced_custom_object(
        namespace=RAY_NAMESPACE,
        group="cluster.ray.io",
        version="v1",
        plural="rayclusters",
        field_selector="metadata.name=test-cluster")["items"][0]
    # TODO: At this point we'd like to return ray_cluster_CR. Unfortunately,
    # its podConfig fields are empty, possibly because the CRD does not
    # currently include a specification of what a podConfig is?
    # So instead we dig what we need out of metadata.
    # This probably only works if the CR has been applied with kubectl.
    cr_string = ray_cluster_CR["metadata"]["annotations"][
        "kubectl.kubernetes.io/last-applied-configuration"]
    return yaml.safe_load(cr_string)


def cr_to_config(cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    cr_spec = cluster_resource["spec"]
    config = translate(cr_spec, dictionary=CONFIG_FIELDS)
    pod_types = cr_spec["podTypes"]
    config["available_node_types"] = get_node_types(pod_types)
    config["provider"] = {"type": "kubernetes", "namespace": RAY_NAMESPACE}
    config["cluster_name"] = cluster_resource["metadata"]["name"]
    return config


def get_node_types(pod_types: List[Dict[str, Any]]) -> Dict[str, Any]:
    node_types = {}
    for pod_type in pod_types:
        name = pod_type["name"]
        pod_type_copy = copy.deepcopy(pod_type)
        pod_type_copy.pop("name")
        node_types[name] = translate(
            pod_type_copy, dictionary=NODE_TYPE_FIELDS)
    return node_types


def translate(configuration: Dict[str, Any], dictionary: Dict[str, str]):
    return {dictionary[field]: configuration[field] for field in configuration}


def get_ray_head_pod_ip(config: Dict[str, Any]) -> str:
    cluster_name = config["cluster_name"]
    label_selector = f"component=ray-head,ray-cluster-name={cluster_name}"
    pods = core_api().list_namespaced_pod(
        namespace=RAY_NAMESPACE, label_selector=label_selector).items
    assert (len(pods)) == 1
    head_pod = pods.pop()
    return head_pod.status.pod_ip


def get_logs() -> Tuple[IO, IO]:
    try:
        os.makedirs(LOG_DIR)
    except OSError:
        pass
    err_path = os.path.join(LOG_DIR, ERR_NAME)
    out_path = os.path.join(LOG_DIR, OUT_NAME)
    return open_log(err_path), open_log(out_path)
