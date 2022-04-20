import copy
import logging
import os
import re
import time
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List

from kubernetes.watch import Watch
from kubernetes.client.rest import ApiException

from ray import ray_constants
from ray.autoscaler._private._kubernetes import custom_objects_api
from ray.autoscaler._private._kubernetes.node_provider import head_service_selector
from ray.autoscaler._private.providers import _get_default_config

RAY_API_GROUP = "cluster.ray.io"
RAY_API_VERSION = "v1"
RAYCLUSTER_PLURAL = "rayclusters"

STATUS_AUTOSCALING_EXCEPTION = "AutoscalingExceptionRecovery"
STATUS_ERROR = "Error"
STATUS_RUNNING = "Running"
STATUS_UPDATING = "Running"
AUTOSCALER_RETRIES_FIELD = "autoscalerRetries"

MAX_STATUS_RETRIES = 3
DELAY_BEFORE_STATUS_RETRY = 0.5

OPERATOR_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE", "")
# Operator is namespaced if the above environment variable is set,
# cluster-scoped otherwise:
NAMESPACED_OPERATOR = OPERATOR_NAMESPACE != ""

RAY_CONFIG_DIR = os.environ.get("RAY_CONFIG_DIR") or os.path.expanduser(
    "~/ray_cluster_configs"
)

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
    "SetupCommands": "setup_commands",
}

NODE_TYPE_FIELDS = {
    "minWorkers": "min_workers",
    "maxWorkers": "max_workers",
    "podConfig": "node_config",
    "rayResources": "resources",
    "setupCommands": "worker_setup_commands",
    "workerSetupCommands": "worker_setup_commands",
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
        plural=RAYCLUSTER_PLURAL,
    )


def namespaced_cr_stream(namespace) -> Iterator:
    w = Watch()
    return w.stream(
        custom_objects_api().list_namespaced_custom_object,
        namespace=namespace,
        group=RAY_API_GROUP,
        version=RAY_API_VERSION,
        plural=RAYCLUSTER_PLURAL,
    )


def cr_to_config(cluster_resource: Dict[str, Any]) -> Dict[str, Any]:
    """Convert RayCluster custom resource to a ray cluster config for use by the
    autoscaler."""
    config = translate(cluster_resource["spec"], dictionary=CONFIG_FIELDS)
    cluster_name = cluster_resource["metadata"]["name"]
    namespace = cluster_resource["metadata"]["namespace"]
    cluster_owner_reference = get_cluster_owner_reference(
        cluster_resource, cluster_name
    )
    config["available_node_types"] = get_node_types(
        cluster_resource, cluster_name, cluster_owner_reference
    )
    config["cluster_name"] = cluster_name
    head_service_ports = cluster_resource["spec"].get("headServicePorts", None)
    config["provider"] = get_provider_config(
        cluster_name, namespace, cluster_owner_reference, head_service_ports
    )
    return config


def get_node_types(
    cluster_resource: Dict[str, Any],
    cluster_name: str,
    cluster_owner_reference: Dict[str, Any],
) -> Dict[str, Any]:
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


def get_provider_config(
    cluster_name, namespace, cluster_owner_reference, head_service_ports
):

    provider_conf = {}
    provider_conf["type"] = "kubernetes"
    provider_conf["use_internal_ips"] = True
    provider_conf["namespace"] = namespace
    provider_conf["services"] = [
        get_head_service(cluster_name, cluster_owner_reference, head_service_ports)
    ]
    # Signal to autoscaler that the Operator is in use:
    provider_conf["_operator"] = True

    return provider_conf


def get_head_service(cluster_name, cluster_owner_reference, head_service_ports):
    # Configure head service for dashboard, client, and Ray Serve.

    # Pull the default head service from
    # autoscaler/_private/_kubernetes/defaults.yaml
    default_kubernetes_config = _get_default_config({"type": "kubernetes"})
    default_provider_conf = default_kubernetes_config["provider"]
    head_service = copy.deepcopy(default_provider_conf["services"][0])

    # Configure the service's name
    service_name = f"{cluster_name}-ray-head"
    head_service["metadata"]["name"] = service_name

    # Garbage-collect service upon cluster deletion.
    head_service["metadata"]["ownerReferences"] = [cluster_owner_reference]

    # Allows service to access the head pod.
    # The corresponding label is set on the head pod in
    # KubernetesNodeProvider.create_node().
    head_service["spec"]["selector"] = head_service_selector(cluster_name)

    # Configure custom ports if provided by the user.
    if head_service_ports:
        user_port_dict = port_list_to_dict(head_service_ports)
        default_port_dict = port_list_to_dict(head_service["spec"]["ports"])
        # Update default ports with user specified ones.
        default_port_dict.update(user_port_dict)
        updated_port_list = port_dict_to_list(default_port_dict)
        head_service["spec"]["ports"] = updated_port_list

    return head_service


def port_list_to_dict(port_list: List[Dict]) -> Dict:
    """Converts a list of ports with 'name' entries to a dict with name keys.

    Convenience method used when updating default head service ports with user
    specified ports.
    """
    out_dict = {}
    for item in port_list:
        value = copy.deepcopy(item)
        key = value.pop("name")
        out_dict[key] = value
    return out_dict


def port_dict_to_list(port_dict: Dict) -> List[Dict]:
    """Inverse of port_list_to_dict."""
    out_list = []
    for key, value in port_dict.items():
        out_list.append({"name": key, **value})
    return out_list


def get_cluster_owner_reference(
    cluster_resource: Dict[str, Any], cluster_name: str
) -> Dict[str, Any]:
    return {
        "apiVersion": cluster_resource["apiVersion"],
        "kind": cluster_resource["kind"],
        "blockOwnerDeletion": True,
        "controller": True,
        "name": cluster_name,
        "uid": cluster_resource["metadata"]["uid"],
    }


def translate(
    configuration: Dict[str, Any], dictionary: Dict[str, str]
) -> Dict[str, Any]:
    return {
        dictionary[field]: configuration[field]
        for field in dictionary
        if field in configuration
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
                logger.info(
                    "Caught a 409 error while setting RayCluster status. Retrying..."
                )
                time.sleep(DELAY_BEFORE_STATUS_RETRY)
                continue
            else:
                raise
    # One more try
    _set_status(cluster_name, cluster_namespace, status)


def _set_status(cluster_name: str, cluster_namespace: str, phase: str) -> None:
    cluster_cr = custom_objects_api().get_namespaced_custom_object(
        namespace=cluster_namespace,
        group=RAY_API_GROUP,
        version=RAY_API_VERSION,
        plural=RAYCLUSTER_PLURAL,
        name=cluster_name,
    )
    status = cluster_cr.get("status", {})
    autoscaler_retries = status.get(AUTOSCALER_RETRIES_FIELD, 0)
    if phase == STATUS_AUTOSCALING_EXCEPTION:
        autoscaler_retries += 1
    cluster_cr["status"] = {
        "phase": phase,
        AUTOSCALER_RETRIES_FIELD: autoscaler_retries,
    }
    custom_objects_api().patch_namespaced_custom_object_status(
        namespace=cluster_namespace,
        group=RAY_API_GROUP,
        version=RAY_API_VERSION,
        plural=RAYCLUSTER_PLURAL,
        name=cluster_name,
        body=cluster_cr,
    )


def infer_head_port(cluster_config: Dict[str, Any]) -> str:
    """Infer Ray head port from the head Ray start command. If no port argument
    is provided, return the default port.

    The port is used by the Operator to initialize the monitor.

    Args:
        cluster_config: Ray autoscaler cluster config dict

    Returns:
        Ray head port.

    """
    head_start_commands = cluster_config.get("head_start_ray_commands", [])
    for cmd in head_start_commands:
        # Split on space and equals sign.
        components = re.split("=| ", cmd)
        for i, component in enumerate(components):
            if component == "--port":
                # Port value is the next component.
                port = components[i + 1]
                return port
    return str(ray_constants.DEFAULT_PORT)


def check_redis_password_not_specified(cluster_config, name, namespace):
    """Detect if Redis password is specified in the head Ray start commands.
    The operator does not currently support setting a custom Redis password.
    """
    head_start_commands = cluster_config.get("head_start_ray_commands", [])
    if any("redis-password" in cmd for cmd in head_start_commands):
        prefix = ",".join([name, namespace]) + ":"
        raise ValueError(
            f"{prefix}The Ray Kubernetes Operator does not"
            " support setting a custom Redis password in Ray"
            " start commands."
        )
