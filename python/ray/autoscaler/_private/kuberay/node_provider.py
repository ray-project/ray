import json
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import requests

from ray.autoscaler._private.constants import (
    WORKER_LIVENESS_CHECK_KEY,
    WORKER_RPC_DRAIN_KEY,
)
from ray.autoscaler._private.util import NodeID, NodeIP, NodeKind, NodeStatus, NodeType
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

# These should be synced with:
# https://github.com/ray-project/kuberay/blob/f2d94ffe213dd8f69481b09c474047cb899fa73b/ray-operator/apis/ray/v1/raycluster_types.go#L165-L171 # noqa
# Kind label value indicating the pod is the head.
KUBERAY_KIND_HEAD = "head"
# Kind label value indicating the pod is the worker.
KUBERAY_KIND_WORKER = "worker"

# KubeRay CRD version
KUBERAY_CRD_VER = os.getenv("KUBERAY_CRD_VER", "v1alpha1")

KUBERAY_REQUEST_TIMEOUT_S = int(os.getenv("KUBERAY_REQUEST_TIMEOUT_S", 60))

RAY_HEAD_POD_NAME = os.getenv("RAY_HEAD_POD_NAME")

# https://kubernetes.io/docs/tasks/run-application/access-api-from-pod
# While running in a Pod, your container can create an HTTPS URL for the
# Kubernetes API server by fetching the KUBERNETES_SERVICE_HOST and
# KUBERNETES_SERVICE_PORT_HTTPS environment variables.
KUBERNETES_SERVICE_HOST = os.getenv(
    "KUBERNETES_SERVICE_HOST", "https://kubernetes.default"
)
KUBERNETES_SERVICE_PORT = os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
KUBERNETES_HOST = f"{KUBERNETES_SERVICE_HOST}:{KUBERNETES_SERVICE_PORT}"
# Key for GKE label that identifies which multi-host replica a pod belongs to
REPLICA_INDEX_KEY = "replicaIndex"

# Design:

# Each modification the autoscaler wants to make is posted to the API server goal state
# (e.g. if the autoscaler wants to scale up, it increases the number of
# replicas of the worker group it wants to scale, if it wants to scale down
# it decreases the number of replicas and adds the exact pods that should be
# terminated to the scaleStrategy).

# KubeRayNodeProvider inherits from BatchingNodeProvider.
# Thus, the autoscaler's create and terminate requests are batched into a single
# Scale Request object which is submitted at the end of autoscaler update.
# KubeRay node provider converts the ScaleRequest into a RayCluster CR patch
# and applies the patch in the submit_scale_request method.

# To reduce potential for race conditions, KubeRayNodeProvider
# aborts the autoscaler update if the operator has not yet processed workersToDelete -
# see KubeRayNodeProvider.safe_to_scale().
# Once it is confirmed that workersToDelete have been cleaned up, KubeRayNodeProvider
# clears the workersToDelete list.


# Note: Log handlers set up in autoscaling monitor entrypoint.
logger = logging.getLogger(__name__)

provider_exists = False


def node_data_from_pod(pod: Dict[str, Any]) -> NodeData:
    """Converts a Ray pod extracted from K8s into Ray NodeData.
    NodeData is processed by BatchingNodeProvider.
    """
    kind, type = kind_and_type(pod)
    status = status_tag(pod)
    ip = pod_ip(pod)
    replica_index = _replica_index_label(pod)
    return NodeData(
        kind=kind, type=type, replica_index=replica_index, status=status, ip=ip
    )


def kind_and_type(pod: Dict[str, Any]) -> Tuple[NodeKind, NodeType]:
    """Determine Ray node kind (head or workers) and node type (worker group name)
    from a Ray pod's labels.
    """
    labels = pod["metadata"]["labels"]
    kind = (
        NODE_KIND_HEAD
        if labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_HEAD
        else NODE_KIND_WORKER
    )
    type = labels[KUBERAY_LABEL_KEY_TYPE]
    return kind, type


def _replica_index_label(pod: Dict[str, Any]) -> Optional[str]:
    """Returns the replicaIndex label for a Pod in a multi-host TPU worker group.
    The replicaIndex label is set by the GKE TPU Ray webhook and is of
    the form {$WORKER_GROUP_NAME-$REPLICA_INDEX} where $REPLICA_INDEX
    is an integer from 0 to Replicas-1.
    """
    labels = pod["metadata"]["labels"]
    return labels.get(REPLICA_INDEX_KEY, None)


def pod_ip(pod: Dict[str, Any]) -> NodeIP:
    return pod["status"].get("podIP", "IP not yet assigned")


def status_tag(pod: Dict[str, Any]) -> NodeStatus:
    """Convert pod state to Ray autoscaler node status.

    See the doc string of the class
    batching_node_provider.NodeData for the semantics of node status.
    """
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


def worker_delete_patch(group_index: str, workers_to_delete: List[NodeID]):
    path = f"/spec/workerGroupSpecs/{group_index}/scaleStrategy"
    value = {"workersToDelete": workers_to_delete}
    return replace_patch(path, value)


def worker_replica_patch(group_index: str, target_replicas: int):
    path = f"/spec/workerGroupSpecs/{group_index}/replicas"
    value = target_replicas
    return replace_patch(path, value)


def replace_patch(path: str, value: Any) -> Dict[str, Any]:
    return {"op": "replace", "path": path, "value": value}


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


def url_from_resource(
    namespace: str,
    path: str,
    kuberay_crd_version: str = KUBERAY_CRD_VER,
    kubernetes_host: str = KUBERNETES_HOST,
) -> str:
    """Convert resource path to REST URL for Kubernetes API server.

    Args:
        namespace: The K8s namespace of the resource
        path: The part of the resource path that starts with the resource type.
            Supported resource types are "pods" and "rayclusters".
        kuberay_crd_version: The API version of the KubeRay CRD.
            Looks like "v1alpha1", "v1".
        kubernetes_host: The host of the Kubernetes API server.
            Uses $KUBERNETES_SERVICE_HOST and
            $KUBERNETES_SERVICE_PORT to construct the kubernetes_host if not provided.

            When set by Kubernetes,
            $KUBERNETES_SERVICE_HOST could be an IP address. That's why the https
            scheme is added here.

            Defaults to "https://kubernetes.default:443".
    """
    if kubernetes_host.startswith("http://"):
        raise ValueError("Kubernetes host must be accessed over HTTPS.")
    if not kubernetes_host.startswith("https://"):
        kubernetes_host = "https://" + kubernetes_host
    if path.startswith("pods"):
        api_group = "/api/v1"
    elif path.startswith("rayclusters"):
        api_group = "/apis/ray.io/" + kuberay_crd_version
    else:
        raise NotImplementedError("Tried to access unknown entity at {}".format(path))
    return kubernetes_host + api_group + "/namespaces/" + namespace + "/" + path


def _worker_group_index(raycluster: Dict[str, Any], group_name: str) -> int:
    """Extract worker group index from RayCluster."""
    group_names = [
        spec["groupName"] for spec in raycluster["spec"].get("workerGroupSpecs", [])
    ]
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


class IKubernetesHttpApiClient(ABC):
    """
    An interface for a Kubernetes HTTP API client.

    This interface could be used to mock the Kubernetes API client in tests.
    """

    @abstractmethod
    def get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers."""
        pass

    @abstractmethod
    def patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        pass


class KubernetesHttpApiClient(IKubernetesHttpApiClient):
    def __init__(self, namespace: str, kuberay_crd_version: str = KUBERAY_CRD_VER):
        self._kuberay_crd_version = kuberay_crd_version
        self._namespace = namespace
        self._headers, self._verify = load_k8s_secrets()

    def get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers.

        Args:
            path: The part of the resource path that starts with the resource type.

        Returns:
            The JSON response of the GET request.

        Raises:
            HTTPError: If the GET request fails.
        """
        url = url_from_resource(
            namespace=self._namespace,
            path=path,
            kuberay_crd_version=self._kuberay_crd_version,
        )
        result = requests.get(
            url,
            headers=self._headers,
            timeout=KUBERAY_REQUEST_TIMEOUT_S,
            verify=self._verify,
        )
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()

    def patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers

        Args:
            path: The part of the resource path that starts with the resource type.
            payload: The JSON patch payload.

        Returns:
            The JSON response of the PATCH request.

        Raises:
            HTTPError: If the PATCH request fails.
        """
        url = url_from_resource(
            namespace=self._namespace,
            path=path,
            kuberay_crd_version=self._kuberay_crd_version,
        )
        result = requests.patch(
            url,
            json.dumps(payload),
            headers={**self._headers, "Content-type": "application/json-patch+json"},
            verify=self._verify,
        )
        if not result.status_code == 200:
            result.raise_for_status()
        return result.json()


class KubeRayNodeProvider(BatchingNodeProvider):  # type: ignore
    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ):
        logger.info("Creating KubeRayNodeProvider.")
        self.namespace = provider_config["namespace"]
        self.cluster_name = cluster_name

        self.k8s_api_client = KubernetesHttpApiClient(self.namespace)

        assert (
            provider_config.get(WORKER_LIVENESS_CHECK_KEY, True) is False
        ), f"To use KubeRayNodeProvider, must set `{WORKER_LIVENESS_CHECK_KEY}:False`."
        assert (
            provider_config.get(WORKER_RPC_DRAIN_KEY, False) is True
        ), f"To use KubeRayNodeProvider, must set `{WORKER_RPC_DRAIN_KEY}:True`."
        BatchingNodeProvider.__init__(
            self, provider_config, cluster_name, _allow_multiple
        )

    def get_node_data(self) -> Dict[NodeID, NodeData]:
        """Queries K8s for pods in the RayCluster. Converts that pod data into a
        map of pod name to Ray NodeData, as required by BatchingNodeProvider.
        """
        # Store the raycluster CR
        self._raycluster = self._get(f"rayclusters/{self.cluster_name}")

        # Get the pods resource version.
        # Specifying a resource version in list requests is important for scalability:
        # https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list
        resource_version = self._get_pods_resource_version()
        if resource_version:
            logger.info(
                f"Listing pods for RayCluster {self.cluster_name}"
                f" in namespace {self.namespace}"
                f" at pods resource version >= {resource_version}."
            )

        # Filter pods by cluster_name.
        label_selector = requests.utils.quote(f"ray.io/cluster={self.cluster_name}")

        resource_path = f"pods?labelSelector={label_selector}"
        if resource_version:
            resource_path += (
                f"&resourceVersion={resource_version}"
                + "&resourceVersionMatch=NotOlderThan"
            )

        pod_list = self._get(resource_path)
        fetched_resource_version = pod_list["metadata"]["resourceVersion"]
        logger.info(
            f"Fetched pod data at resource version" f" {fetched_resource_version}."
        )

        # Extract node data from the pod list.
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
        """Converts the scale request generated by BatchingNodeProvider into
        a patch that modifies the RayCluster CR's replicas and/or workersToDelete
        fields. Then submits the patch to the K8s API server.
        """
        # Transform the scale request into a patch payload.
        patch_payload = self._scale_request_to_patch_payload(
            scale_request, self._raycluster
        )

        # Submit the patch to K8s.
        logger.info(
            "Autoscaler is submitting the following patch to RayCluster "
            f"{self.cluster_name} in namespace {self.namespace}."
        )
        logger.info(patch_payload)
        self._submit_raycluster_patch(patch_payload)

    def safe_to_scale(self) -> bool:
        """Returns False iff non_terminated_nodes contains any pods in the RayCluster's
        workersToDelete lists.

        Explanation:
        If there are any workersToDelete which are non-terminated,
        we should wait for the operator to do its job and delete those
        pods. Therefore, we back off the autoscaler update.

        If, on the other hand, all of the workersToDelete have already been cleaned up,
        then we patch away the workersToDelete lists and return True.
        In the future, we may consider having the operator clean up workersToDelete
        on it own:
        https://github.com/ray-project/kuberay/issues/733

        Note (Dmitri):
        It is stylistically bad that this function has a side effect.
        """
        # Get the list of nodes.
        node_set = set(self.node_data_dict.keys())
        worker_groups = self._raycluster["spec"].get("workerGroupSpecs", [])

        # Accumulates the indices of worker groups with non-empty workersToDelete
        non_empty_worker_group_indices = []

        for group_index, worker_group in enumerate(worker_groups):
            workersToDelete = worker_group.get("scaleStrategy", {}).get(
                "workersToDelete", []
            )
            if workersToDelete:
                non_empty_worker_group_indices.append(group_index)
            for worker in workersToDelete:
                if worker in node_set:
                    # The operator hasn't removed this worker yet. Abort
                    # the autoscaler update.
                    logger.warning(f"Waiting for operator to remove worker {worker}.")
                    return False

        # All required workersToDelete have been removed.
        # Clean up the workersToDelete field.
        patch_payload = []
        for group_index in non_empty_worker_group_indices:
            patch = worker_delete_patch(group_index, workers_to_delete=[])
            patch_payload.append(patch)
        if patch_payload:
            logger.info("Cleaning up workers to delete.")
            logger.info(f"Submitting patch {patch_payload}.")
            self._submit_raycluster_patch(patch_payload)

        # It's safe to proceed with the autoscaler update.
        return True

    def _get_pods_resource_version(self) -> str:
        """
        Extract a recent pods resource version by reading the head pod's
        metadata.resourceVersion of the response.
        """
        if not RAY_HEAD_POD_NAME:
            return None
        pod_resp = self._get(f"pods/{RAY_HEAD_POD_NAME}")
        return pod_resp["metadata"]["resourceVersion"]

    def _scale_request_to_patch_payload(
        self, scale_request: ScaleRequest, raycluster: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Converts autoscaler scale request into a RayCluster CR patch payload."""
        patch_payload = []
        # Collect patches for replica counts.
        for node_type, target_replicas in scale_request.desired_num_workers.items():
            group_index = _worker_group_index(raycluster, node_type)
            group_max_replicas = _worker_group_max_replicas(raycluster, group_index)
            # Cap the replica count to maxReplicas.
            if group_max_replicas is not None and group_max_replicas < target_replicas:
                logger.warning(
                    "Autoscaler attempted to create "
                    + "more than maxReplicas pods of type {}.".format(node_type)
                )
                target_replicas = group_max_replicas
            # Check if we need to change the target count.
            if target_replicas == _worker_group_replicas(raycluster, group_index):
                # No patch required.
                continue
            # Need to patch replica count. Format the patch and add it to the payload.
            patch = worker_replica_patch(group_index, target_replicas)
            patch_payload.append(patch)

        # Maps node_type to nodes to delete for that group.
        deletion_groups = defaultdict(list)
        for worker in scale_request.workers_to_delete:
            node_type = self.node_tags(worker)[TAG_RAY_USER_NODE_TYPE]
            deletion_groups[node_type].append(worker)

        for node_type, workers_to_delete in deletion_groups.items():
            group_index = _worker_group_index(raycluster, node_type)
            patch = worker_delete_patch(group_index, workers_to_delete)
            patch_payload.append(patch)

        return patch_payload

    def _submit_raycluster_patch(self, patch_payload: List[Dict[str, Any]]):
        """Submits a patch to modify a RayCluster CR."""
        path = "rayclusters/{}".format(self.cluster_name)
        self._patch(path, patch_payload)

    def _get(self, path: str) -> Dict[str, Any]:
        """Wrapper for REST GET of resource with proper headers."""
        return self.k8s_api_client.get(path)

    def _patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        return self.k8s_api_client.patch(path, payload)
