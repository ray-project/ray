import copy
import logging
import os
import time
from typing import Any, Dict, Iterator

from kubernetes.watch import Watch
from kubernetes.client.rest import ApiException

from ray.autoscaler._private._kubernetes import custom_objects_api
from ray.autoscaler._private._kubernetes.node_provider import\
    head_service_selector
from ray.autoscaler._private.providers import _get_default_config

RAY_API_GROUP = "cluster.ray.io"
RAY_API_VERSION = "v1"
RAYCLUSTER_PLURAL = "rayclusters"

MAX_STATUS_RETRIES = 3
DELAY_BEFORE_STATUS_RETRY = .5

OPERATOR_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE")
# Operator is namespaced if the above environment variable is set,
# cluster-scoped otherwise:
NAMESPACED_OPERATOR = OPERATOR_NAMESPACE is not None

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
    "podTypes": "available_node_types",
    "headSetupCommands": "head_setup_commands",
    "workerSetupCommands": "worker_setup_commands",
    "SetupCommands": "setup_commands"
}

NODE_TYPE_FIELDS = {
    "minWorkers": "min_workers",
    "maxWorkers": "max_workers",
    "podConfig": "node_config",
    "rayResources": "resources",
    "setupCommands": "worker_setup_commands",
    "workerSetupCommands": "worker_setup_commands"
}

root_logger = logging.getLogger("ray")
root_logger.setLevel(logging.getLevelName("DEBUG"))

logger = logging.getLogger(__name__)


def namespace_dir(namespace: str) -> str:
    """Directory in which to store configs for Ray clusters in a given
    namespace."""
    return os.path.join(RAY_CONFIG_DIR, namespace)


def config_path(cluster_namespace: str, cluster_name: str) -> str:
    """Where to store a cluster's config, given the cluster's name and
    namespace."""
    file_name = cluster_name + CONFIG_SUFFIX
    return os.path.join(namespace_dir(cluster_namespace), file_name)


def cluster_scoped_cr_stream() -> Iterator:
    w = Watch()
    return w.stream(
        custom_objects_api().list_cluster_custom_object,
        group=RAY_API_GROUP,
        version=RAY_API_VERSION,
        plural=RAYCLUSTER_PLURAL)


def namespaced_cr_stream(namespace) -> Iterator:
    w = Watch()
    return w.stream(
        custom_objects_api().list_namespaced_custom_object,
        namespace=namespace,
        group=RAY_API_GROUP,
        version=RAY_API_VERSION,
        plural=RAYCLUSTER_PLURAL)


def cr_to_config(cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    """Convert RayCluster custom resource to a ray cluster config for use by the
    autoscaler."""
    config = translate(cluster_resource["spec"], dictionary=CONFIG_FIELDS)
    cluster_name = cluster_resource["metadata"]["name"]
    namespace = cluster_resource["metadata"]["namespace"]
    cluster_owner_reference = get_cluster_owner_reference(
        cluster_resource, cluster_name)
    config["available_node_types"] = get_node_types(
        cluster_resource, cluster_name, cluster_owner_reference)
    config["cluster_name"] = cluster_name
    config["provider"] = get_provider_config(cluster_name, namespace,
                                             cluster_owner_reference)
    return config


def get_node_types(cluster_resource: Dict[str, Any], cluster_name: str,
                   cluster_owner_reference: Dict[str, Any]) -> Dict[str, Any]:
    node_types = {}
    for pod_type in cluster_resource["spec"]["podTypes"]:
        name = pod_type["name"]
        pod_type_copy = copy.deepcopy(pod_type)
        pod_type_copy.pop("name")
        node_type = translate(pod_type_copy, dictionary=NODE_TYPE_FIELDS)
        metadata = node_type["node_config"]["metadata"]
        metadata.update({"ownerReferences": [cluster_owner_reference]})
        # Prepend cluster name:
        metadata["generateName"] = f"{cluster_name}-{metadata['generateName']}"
        if name == cluster_resource["spec"]["headPodType"]:
            if "labels" not in metadata:
                metadata["labels"] = {}
        node_types[name] = node_type
    return node_types


def get_provider_config(cluster_name, namespace, cluster_owner_reference):
    default_kubernetes_config = _get_default_config({"type": "kubernetes"})
    default_provider_conf = default_kubernetes_config["provider"]

    # Configure head service for dashboard and client
    head_service = copy.deepcopy(default_provider_conf["services"][0])
    service_name = f"{cluster_name}-ray-head"
    head_service["metadata"]["name"] = service_name
    # Garbage-collect service upon cluster deletion.
    head_service["metadata"]["ownerReferences"] = [cluster_owner_reference]
    # Allows service to access the head pod.
    # The corresponding label is set on the head pod in
    # KubernetesNodeProvider.create_node().
    head_service["spec"]["selector"] = head_service_selector(cluster_name)

    provider_conf = {}
    provider_conf["type"] = "kubernetes"
    provider_conf["use_internal_ips"] = True
    provider_conf["namespace"] = namespace
    provider_conf["services"] = [head_service]

    # Signal to autoscaler that the Operator is in use:
    provider_conf["_operator"] = True
    return provider_conf


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


def set_status(cluster_name: str, cluster_namespace: str, status: str) -> None:
    """Sets status.phase field for a RayCluster with the given name and
    namespace.

    Just in case, handles the 409 error that would arise if the RayCluster
    API object is modified between retrieval and patch.

    Args:
        cluster_name: Name of the Ray cluster.
        cluster_namespace: Namespace in which the Ray cluster is running.
        status: String to set for the RayCluster object's status.phase field.

    """
    for _ in range(MAX_STATUS_RETRIES - 1):
        try:
            _set_status(cluster_name, cluster_namespace, status)
            return
        except ApiException as e:
            if e.status == 409:
                logger.info("Caught a 409 error while setting"
                            " RayCluster status. Retrying...")
                time.sleep(DELAY_BEFORE_STATUS_RETRY)
                continue
            else:
                raise
    # One more try
    _set_status(cluster_name, cluster_namespace, status)


def _set_status(cluster_name: str,
                cluster_namespace: str,
                status: str) -> None:
    cluster_cr = custom_objects_api()\
        .get_namespaced_custom_object(namespace=cluster_namespace,
                                      group="cluster.ray.io",
                                      version="v1",
                                      plural="rayclusters",
                                      name=cluster_name)
    cluster_cr["status"] = {"phase": status}
    custom_objects_api()\
        .patch_namespaced_custom_object_status(namespace=cluster_namespace,
                                               group="cluster.ray.io",
                                               version="v1",
                                               plural="rayclusters",
                                               name=cluster_name,
                                               body=cluster_cr)
