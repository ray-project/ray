import json
import logging
import requests
from typing import Any, Dict, List, Tuple

from ray.autoscaler._private.constants import (
    DISABLE_NODE_UPDATERS_KEY,
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UP_TO_DATE,
    STATUS_UPDATE_FAILED,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)

# Terminology:

# Labels and Tags
# We call the Kuberay labels "labels" and the Ray autoscaler tags "tags".
# The labels are prefixed by "ray.io". Tags are prefixed by "ray-".
# We convert between the two but do not mix them.

# Worker Groups and Available Node Types
# In Kuberay the different node types are called "worker groups", in the
# the Ray autoscaler they are called "available node types".

# Design:

# Each modification the autoscaler wants to make is posted to the API server goal state
# (e.g. if the autoscaler wants to scale up, it increases the number of
# replicas of the worker group it wants to scale, if it wants to scale down
# it decreases the number of replicas and adds the exact pods that should be
# terminated to the scaleStrategy). In order to guarantee consistency, the NodeProvider
# then waits until Kuberay's reconciliation loop creates the pod specifications in the
# API server and then returns control back to the autoscaler. The waiting period
# is typically small, on the order of a few seconds. We make sure that only one
# such modification is in process by serializing all modification operations with
# a lock in the NodeProvider.

# Note: Log handlers set up in autoscaling monitor entrypoint.
logger = logging.getLogger(__name__)

provider_exists = False


def to_label_selector(tags: Dict[str, str]) -> str:
    """Convert tags to label selector to embed in query to K8s API server."""
    label_selector = ""
    for k, v in tags.items():
        if label_selector != "":
            label_selector += ","
        label_selector += "{}={}".format(k, v)
    return label_selector


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


def make_node_tags(labels: Dict[str, str], status_tag: str) -> Dict[str, str]:
    """Convert Kuberay labels to Ray autoscaler tags."""
    tags = {"ray-node-status": status_tag}

    if labels["ray.io/node-type"] == "head":
        tags[TAG_RAY_NODE_KIND] = NODE_KIND_HEAD
        tags[TAG_RAY_USER_NODE_TYPE] = "head-group"
    else:
        tags[TAG_RAY_NODE_KIND] = NODE_KIND_WORKER
        tags[TAG_RAY_USER_NODE_TYPE] = labels["ray.io/group"]

    return tags


def load_k8s_secrets() -> Tuple[Dict[str, str], str]:
    """
    Loads secrets needed to access K8s resources.

    Returns:
        headers (dict): Headers with K8s access token
        verify (str): Path to certificate
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


class KuberayNodeProvider(NodeProvider):  # type: ignore
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

        # Disallow multiple node providers, unless explicitly allowed for testing.
        global provider_exists
        if not _allow_multiple:
            assert (
                not provider_exists
            ), "Only one KuberayNodeProvider allowed per process."
        assert (
            provider_config.get(DISABLE_NODE_UPDATERS_KEY, False) is True
        ), f"To use KuberayNodeProvider, must set `{DISABLE_NODE_UPDATERS_KEY}:True`."
        assert provider_config.get(DISABLE_LAUNCH_CONFIG_CHECK_KEY, False) is True, (
            "To use KuberayNodeProvider, must set "
            f"`{DISABLE_LAUNCH_CONFIG_CHECK_KEY}:True`."
        )
        assert (
            provider_config.get(FOREGROUND_NODE_LAUNCH_KEY, False) is True
        ), f"To use KuberayNodeProvider, must set `{FOREGROUND_NODE_LAUNCH_KEY}:True`."
        provider_exists = True

        super().__init__(provider_config, cluster_name)

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
        assert result.status_code == 200
        return result.json()

    def _get_non_terminating_pods(
        self, tag_filters: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Get the list of pods in the Ray cluster, excluding pods
        marked for deletion.

        Filter by the specified tag_filters.

        Return a list of pod objects, represented as dictionaries.

        Details on K8s resource deletion:
        https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-deletion
        """
        label_filters = to_label_selector(
            {
                "ray.io/cluster": self.cluster_name,
            }
        )
        data = self._get("pods?labelSelector=" + requests.utils.quote(label_filters))
        result = []
        for pod in data["items"]:
            # Kubernetes sets metadata.deletionTimestamp immediately after admitting a
            # request to delete an object. Full removal of the object may take some time
            # after the deletion timestamp is set. See link in docstring for details.
            if "deletionTimestamp" in pod["metadata"]:
                # Ignore pods marked for termination.
                continue
            labels = pod["metadata"]["labels"]
            tags = make_node_tags(labels, status_tag(pod))
            if tag_filters.items() <= tags.items():
                result.append(pod)
        return result

    def _patch(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Wrapper for REST PATCH of resource with proper headers."""
        url = url_from_resource(namespace=self.namespace, path=path)
        result = requests.patch(
            url,
            json.dumps(payload),
            headers={**self.headers, "Content-type": "application/json-patch+json"},
            verify=self.verify,
        )
        assert result.status_code == 200
        return result.json()

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Dict[str, Dict[str, str]]:
        """Creates a number of nodes within the namespace."""
        url = "rayclusters/{}".format(self.cluster_name)
        raycluster = self._get(url)
        group_name = tags["ray-user-node-type"]
        group_index = _worker_group_index(raycluster, group_name)
        tag_filters = {TAG_RAY_USER_NODE_TYPE: group_name}
        current_replica_count = len(self.non_terminated_nodes(tag_filters))
        path = f"/spec/workerGroupSpecs/{group_index}/replicas"
        payload = [
            {
                "op": "replace",
                "path": path,
                "value": current_replica_count + count,
            },
        ]
        self._patch(url, payload)
        return {}

    def internal_ip(self, node_id: str) -> str:
        """Get internal IP of a node (= Kubernetes pod)."""
        data = self._get("pods/{}".format(node_id))
        return data["status"].get("podIP", "IP not yet assigned")

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Get tags of a node (= Kubernetes pod)."""
        data = self._get("pods/{}".format(node_id))
        return make_node_tags(data["metadata"]["labels"], status_tag(data))

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict."""
        return [
            pod["metadata"]["name"]
            for pod in self._get_non_terminating_pods(tag_filters)
        ]

    def terminate_node(self, node_id: str) -> None:
        """Terminates the specified node (= Kubernetes pod)."""
        self.terminate_nodes([node_id])

    def terminate_nodes(self, node_ids: List[str]) -> Dict[str, Dict[str, str]]:
        """Batch terminates the specified nodes (= Kubernetes pods)."""
        # Split node_ids into groups according to node type and terminate
        # them individually. Note that in most cases, node_ids contains
        # a single element and therefore it is most likely not worth
        # optimizing this code to batch the requests to the API server.
        groups = {}
        current_replica_counts = {}
        label_filters = to_label_selector({"ray.io/cluster": self.cluster_name})
        pods = self._get("pods?labelSelector=" + requests.utils.quote(label_filters))
        for pod in pods["items"]:
            group_name = pod["metadata"]["labels"]["ray.io/group"]
            current_replica_counts[group_name] = (
                current_replica_counts.get(group_name, 0) + 1
            )
            if pod["metadata"]["name"] in node_ids:
                groups.setdefault(group_name, []).append(pod["metadata"]["name"])

        url = "rayclusters/{}".format(self.cluster_name)
        raycluster = self._get(url)

        for group_name, nodes in groups.items():
            group_index = _worker_group_index(raycluster, group_name)
            prefix = f"/spec/workerGroupSpecs/{group_index}"
            payload = [
                {
                    "op": "replace",
                    "path": prefix + "/replicas",
                    "value": current_replica_counts[group_name] - len(nodes),
                },
                {
                    "op": "replace",
                    "path": prefix + "/scaleStrategy",
                    "value": {"workersToDelete": nodes},
                },
            ]
            self._patch(url, payload)
        return {}
