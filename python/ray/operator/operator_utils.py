import copy
import logging
import os
from typing import Any, Dict, Iterator, List

from kubernetes.watch import Watch
from kubernetes.client.models.v1_owner_reference import V1OwnerReference

from ray.autoscaler._private.kubernetes import (auth_api, core_api,
                                                custom_objects_api)

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
"""
ownerReferences:
  - apiVersion: apps/v1
    controller: true
    blockOwnerDeletion: true
    kind: ReplicaSet
    name: my-repset
    uid: d9607e19-f88f-11e6-a518-42010a800195
"""


def fill_operator_ownerrefs() -> None:
    pod = core_api().read_namespaced_pod(
        namespace=RAY_NAMESPACE, name="ray-operator-pod")
    ref = get_owner_reference("Pod", pod.metadata.name, pod.metadata.uid)

    service_account = core_api().read_namespaced_service_account(
        namespace=RAY_NAMESPACE, name="ray-operator-serviceaccount")
    role = auth_api().read_namespaced_role(
        namespace=RAY_NAMESPACE, name="ray-operator-role")
    role_binding = auth_api().read_namespaced_role_binding(
        namespace=RAY_NAMESPACE, name="ray-operator-rolebinding")

    service_account.metadata.owner_references = [ref]
    role.metadata.owner_references = [ref]
    role_binding.metadata.owner_references = [ref]

    core_api().patch_namespaced_service_account(
        namespace=RAY_NAMESPACE,
        name="ray-operator-serviceaccount",
        body=service_account)
    auth_api().patch_namespaced_role(
        namespace=RAY_NAMESPACE, name="ray-operator-role", body=role)
    auth_api().patch_namespaced_role_binding(
        namespace=RAY_NAMESPACE,
        name="ray-operator-rolebinding",
        body=role_binding)


def get_owner_reference(kind, name, uid):
    return V1OwnerReference(
        api_version="apps/v1",
        controller=True,
        block_owner_deletion=True,
        kind=kind,
        name=name,
        uid=uid)


def get_owner_reference_dict(kind, name, uid):
    return {
        "apiVersion": "apps/v1",
        "controller": "true",
        "blockOwnerDeletion": "true",
        "kind": kind,
        "name": name,
        "uid": uid
    }


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
    cr_spec = cluster_resource["spec"]
    cluster_name = cluster_resource["metadata"]["name"]
    cluster_uid = cluster_resource["metadata"]["uid"]
    config = translate(cr_spec, dictionary=CONFIG_FIELDS)
    pod_types = cr_spec["podTypes"]
    config["available_node_types"] = get_node_types(pod_types, cluster_name,
                                                    cluster_uid)
    config["cluster_name"] = cluster_name
    config["provider"] = PROVIDER_CONFIG
    return config


def get_node_types(pod_types: List[Dict[str, Any]], cluster_name: str,
                   cluster_uid: str) -> Dict[str, Any]:
    cluster_owner_reference = get_owner_reference_dict(
        kind="RayCluster", name=cluster_name, uid=cluster_uid)
    node_types = {}
    for pod_type in pod_types:
        name = pod_type["name"]
        pod_type_copy = copy.deepcopy(pod_type)
        pod_type_copy.pop("name")
        node_types[name] = translate(
            pod_type_copy, dictionary=NODE_TYPE_FIELDS)
        node_types[name]["node_config"]["metadata"].update({
            "ownerReferences": [cluster_owner_reference]
        })
    return node_types


def translate(configuration: Dict[str, Any],
              dictionary: Dict[str, str]) -> Dict[str, Any]:
    return {dictionary[field]: configuration[field] for field in configuration}


def get_ray_head_pod_ip(config: Dict[str, Any]) -> str:
    cluster_name = config["cluster_name"]
    label_selector = f"ray-node-type=head,ray-cluster-name={cluster_name}"
    pods = core_api().list_namespaced_pod(
        namespace=RAY_NAMESPACE, label_selector=label_selector).items
    assert (len(pods)) == 1
    head_pod = pods.pop()
    return head_pod.status.pod_ip
