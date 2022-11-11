import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import requests

from ray.autoscaler._private.constants import (
    WORKER_LIVENESS_CHECK_KEY,
    WORKER_RPC_DRAIN_KEY,
)
from ray.autoscaler.batching_node_provider import (
    BatchingNodeProvider,
    NodeData,
    ScaleRequest,
)
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UP_TO_DATE,
    STATUS_UPDATE_FAILED,
    TAG_RAY_USER_NODE_TYPE,
)

# Key for KubeRay label that identifies a Ray pod as head or worker.
KUBERAY_LABEL_KEY_KIND = "ray.io/node-type"
# Key for KubeRay label that identifies the worker group (autoscaler node type) of a
# Ray pod.
KUBERAY_LABEL_KEY_TYPE = "ray.io/group"
# Kind label value indicating the pod is the head.
KUBERAY_KIND_HEAD = "head"
# Group name (node type) to use for the  head.
KUBERAY_TYPE_HEAD = "head-group"

# Design:

# Each modification the autoscaler wants to make is posted to the API server goal state
# (e.g. if the autoscaler wants to scale up, it increases the number of
# replicas of the worker group it wants to scale, if it wants to scale down
# it decreases the number of replicas and adds the exact pods that should be
# terminated to the scaleStrategy).

# KuberayNodeProvider inherits from BatchingNodeProvider.
# Thus, the autoscaler's create and terminate requests are batched into a single
# Scale Request object which is submitted at the end of autoscaler update.
# KubeRay node provider converts the ScaleRequest into a RayCluster CR patch
# and applies the patch in the submit_scale_request method.
# To reduce potential for race conditions, KuberayNodeProvider
# skips submitting the patch if the operator has not yet processed workersToDelete.


# Note: Log handlers set up in autoscaling monitor entrypoint.
logger = logging.getLogger(__name__)

provider_exists = False


def node_data_from_pod(pod: Dict[str, Any]) -> NodeData:
    """Converts a pod into node data useable the autoscaler."""
    labels = pod["metadata"]["labels"]

    kind, type = "", ""
    if labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_HEAD:
        kind = NODE_KIND_HEAD
        type = KUBERAY_TYPE_HEAD
    else:
        kind = NODE_KIND_WORKER
        type = labels[KUBERAY_LABEL_KEY_TYPE]

    status = status_tag(pod)
    ip = pod["status"].get("podIP", "IP not yet assigned")
    return NodeData(kind=kind, type=type, status=status, ip=ip)


def status_tag(pod: Dict[str, Any]) -> str:
    """Convert pod state to Ray autoscaler status tag."""
    if (
        "containerStatuses" not in pod["status"]
        or not pod["status"]["containerStatuses"]
    ):
        return "pending"

    state = pod["status"]["containerStatuses"][0]["state"]

    if "pending" in state:
        return "pending"
    if "running" in state:
        return STATUS_UP_TO_DATE
    if "waiting" in state:
        return "waiting"
    if "terminated" in state:
        return STATUS_UPDATE_FAILED
    raise ValueError("Unexpected container state.")


def load_k8s_secrets() -> Tuple[Dict[str, str], str]:
    """
    Loads secrets needed to access K8s resources.

    Returns:
        headers: Headers with K8s access token
        verify: Path to certificate
    """
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as secret:
        token = secret.read()

    headers = {
        "Authorization": "Bearer " + token,
    }
    verify = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    return headers, verify


def url_from_resource(namespace: str, path: str) -> str:
    """Convert resource path to REST URL for Kubernetes API server.

    Args:
        namespace: The K8s namespace of the resource
        path: The part of the resource path that starts with the resource type.
            Supported resource types are "pods" and "rayclusters".
    """
    if path.startswith("pods"):
        api_group = "/api/v1"
    elif path.startswith("rayclusters"):
        api_group = "/apis/ray.io/v1alpha1"
    else:
        raise NotImplementedError("Tried to access unknown entity at {}".format(path))
    return (
        "https://kubernetes.default:443"
        + api_group
        + "/namespaces/"
        + namespace
        + "/"
        + path
    )


def _worker_group_index(raycluster: Dict[str, Any], group_name: str) -> int:
    """Extract worker group index from RayCluster."""
    group_names = [spec["groupName"] for spec in raycluster["spec"]["workerGroupSpecs"]]
    return group_names.index(group_name)


def _worker_group_max_replicas(
    raycluster: Dict[str, Any], group_index: int
) -> Optional[int]:
    """Extract the maxReplicas of a worker group.

    If maxReplicas is unset, return None, to be interpreted as "no constraint".
    At time of writing, it should be impossible for maxReplicas to be unset, but it's
    better to handle this anyway.
    """
    return raycluster["spec"]["workerGroupSpecs"][group_index].get("maxReplicas")


def _worker_group_replicas(raycluster: Dict[str, Any], group_index: int):
    # 1 is the default replicas value used by the KubeRay operator
    return raycluster["spec"]["workerGroupSpecs"][group_index].get("replicas", 1)


class KuberayNodeProvider(BatchingNodeProvider):  # type: ignore
    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ):
        logger.info("Creating KuberayNodeProvider.")
        self.namespace = provider_config["namespace"]
        self.cluster_name = cluster_name

        self.headers, self.verify = load_k8s_secrets()

        assert (
            provider_config.get(WORKER_LIVENESS_CHECK_KEY, True) is False
        ), f"To use KuberayNodeProvider, must set `{WORKER_LIVENESS_CHECK_KEY}:False`."
        assert (
            provider_config.get(WORKER_RPC_DRAIN_KEY, False) is True
        ), f"To use KuberayNodeProvider, must set `{WORKER_RPC_DRAIN_KEY}:True`."
        BatchingNodeProvider.__init__(
            self, provider_config, cluster_name, _allow_multiple
        )

    def get_node_data(self):
        # Store the raycluster CR
        self._raycluster = self._get(f"rayclusters/{self.cluster_name}")

        pod_list = self._get("pods")
        node_data_dict = {}
        for pod in pod_list["items"]:
            # Kubernetes sets metadata.deletionTimestamp immediately after admitting a
            # request to delete an object. Full removal of the object may take some time
            # after the deletion timestamp is set. See link for details:
            # https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-deletion
            if "deletionTimestamp" in pod["metadata"]:
                # Ignore pods marked for termination.
                continue
            pod_name = pod["metadata"]["name"]
            node_data_dict[pod_name] = node_data_from_pod(pod)
        return node_data_dict

    def submit_scale_request(self, scale_request: ScaleRequest):
        payload = self._scale_request_to_patch_payload(scale_request, self._raycluster)
        path = "rayclusters/{}".format(self.cluster_name)
        logger.info(
            "Autoscaler is submitting the following patch to RayCluster"
            f"{self.cluster_name} in namespace {self.namespace}."
        )
        logger.info(payload)
        self._patch(path, payload)

    def safe_to_scale(self) -> bool:
        """To reduce race conditions, wait for the operator to clear the workersToDelete
        queue before submitting another scale request.
        """
        for group_spec in self._raycluster["spec"].get("workerGroupSpecs", []):
            if group_spec.get("workersToDelete", []):
                logger.warning(
                    "workersToDelete has not been processed yet."
                    " Autoscaler backing off submitting scale request."
                )
                return False
        return True

    def _scale_request_to_patch_payload(
        self, scale_request: ScaleRequest, raycluster: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Converts autoscaler scale request into a RayCluster CR patch."""
        patch_payload = []
        # Collect patches for replica counts.
        for node_type, target_count in scale_request.desired_num_workers.items():
            group_index = _worker_group_index(raycluster, node_type)
            group_max_replicas = _worker_group_max_replicas(raycluster, group_index)
            # Cap the replica count to maxReplicas.
            if group_max_replicas is not None and group_max_replicas < target_count:
                logger.warning(
                    "Autoscaler attempted to create "
                    + "more than maxReplicas pods of type {}.".format(node_type)
                )
                target_count = group_max_replicas
            if target_count == _worker_group_replicas(raycluster, group_index):
                # No patch required.
                continue
            path = f"/spec/workerGroupSpecs/{group_index}/replicas"
            patch = {"op": "replace", "path": path, "value": target_count}
            patch_payload.append(patch)

        # Maps node_type to nodes to delete for that group.
        deletion_groups = defaultdict(list)
        for worker in scale_request.workers_to_delete:
            node_type = self.node_tags(worker)[TAG_RAY_USER_NODE_TYPE]
            deletion_groups[node_type].append(worker)

        for node_type, nodes_to_delete in deletion_groups.items():
            group_index = _worker_group_index(raycluster, node_type)
            path = f"/spec/workerGroupSpecs/{group_index}/scaleStrategy"
            patch = {
                "op": "replace",
                "path": path,
                "value": {"workersToDelete": nodes_to_delete},
            }
            patch_payload.append(patch)

        return patch_payload

    def _url(self, path: str) -> str:
        """Convert resource path to REST URL for Kubernetes API server."""
        if path.startswith("pods"):
            api_group = "/api/v1"
        elif path.startswith("rayclusters"):
            api_group = "/apis/ray.io/v1alpha1"
        else:
            raise NotImplementedError(
                "Tried to access unknown entity at {}".format(path)
            )
        return (
            "https://kubernetes.default:443"
            + api_group
            + "/namespaces/"
            + self.namespace
            + "/"
            + path
        )

    def _get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers."""
        url = url_from_resource(namespace=self.namespace, path=path)
        result = requests.get(url, headers=self.headers, verify=self.verify)
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()

    def _patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        url = url_from_resource(namespace=self.namespace, path=path)
        result = requests.patch(
            url,
            json.dumps(payload),
            headers={**self.headers, "Content-type": "application/json-patch+json"},
            verify=self.verify,
        )
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()
