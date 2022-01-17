import json
import logging
import requests
import threading
import time
from typing import Any, Dict, List, Tuple, Union

import kubernetes
from kubernetes.config.config_exception import ConfigException

from ray.autoscaler.node_provider import NodeProvider

## Terminology:

# Labels and Tags
# We call the Kuberay labels "labels" and the Ray autoscaler tags "tags".
# The labes are prefixed by "ray.io". Tags are prefixed by "ray-".
# We convert between the two but do not mix them.

# Worker Groups and Available Node Types
# In Kuberay the different node types are called "worker groups", in the
# the Ray autoscaler they are called "available node types".

## Design:

# Each modification the autoscaler wants to make is posted to the API server goal state
# (e.g. if the autoscaler wants to scale up, it increases the number of
# replicas of the worker group it wants to scale, if it wants to scale down
# it decreases the number of replicas and adds the exact pods that should be
# terminated to the scaleStrategy). In order to guarantee consistency, the NodeProvider
# then waits until Kuberay's reconciliation loop creates the pods and then returns control
# back to the autoscaler. The waiting period is typically small, on the order
# of a few seconds. We make sure that only one such modification is in process
# by serializing all modification operations with a lock in the NodeProvider.


# Note: Log handlers set up in autoscaling monitor entrypoint.
logger = logging.getLogger(__name__)

provider_exists = False


def to_label_selector(tags):
    label_selector = ""
    for k, v in tags.items():
        if label_selector != "":
            label_selector += ","
        label_selector += "{}={}".format(k, v)
    return label_selector

def status_tag(pod: Dict[str, Any]):
    if "containerStatuses" not in pod["status"]:
        return "pending"
    
    state = pod["status"]["containerStatuses"][0]["state"]

    logger.info("status_tag: state = {}".format(state))

    if "pending" in state:
        return "pending"
    if "running" in state:
        return "up-to-date"
    if "waiting" in state:
        return "waiting"
    if "terminated" in state:
        return "update-failed"
    raise ValueError("Unexpected container state.")

def make_node_tags(labels: Dict[str, str], status_tag: str):
    "Convert Kuberay labels to Ray autoscaler tags."
    tags = {"ray-node-status": status_tag}

    if labels["ray.io/node-type"] == "head":
        tags["ray-node-type"] = "head"
        tags["ray-user-node-type"] = "head-group"
    else:
        tags["ray-node-type"] = "worker"
        tags["ray-user-node-type"] = labels["ray.io/group"]

    return tags


class KuberayNodeProvider(NodeProvider):  # type: ignore
    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ):
        logger.info("Creating ClientNodeProvider.")
        self.namespace = provider_config["namespace"]
        self.cluster_name = cluster_name
        self._lock = threading.RLock()

        with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as secret:
            token = secret.read()

        self.headers = {
            "Authorization": "Bearer " + token,
        }
        self.verify = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

        # Disallow multiple node providers, unless explicitly allowed for testing.
        global provider_exists
        if not _allow_multiple:
            assert (
                not provider_exists
            ), "Only one ClientNodeProvider allowed per process."
        assert (
            provider_config.get("disable_node_updaters", False) is True
        ), "Must disable node updaters to use ClientNodeProvider."
        provider_exists = True

        super().__init__(provider_config, cluster_name)

    def _url(self, path, api_group):
        if path.startswith("pods"):
            api_group = "/api/v1"
        elif path.startswith("rayclusters"):
            api_group="/apis/ray.io/v1alpha1"
        else:
            raise NotImplementedError("Tried to access unknown entity at {}".format(path))
        return "https://kubernetes.default:443" + api_group + "/namespaces/" + self.namespace + "/" + path

    def _get(self, path):
        result = requests.get(self._url(path), headers=self.headers, verify=self.verify)
        assert result.status_code == 200
        return result.json()

    def _patch(self, path, payload):
        result = requests.patch(
            self._url(path),
            json.dumps(payload),
            headers={**self.headers, "Content-type": "application/json-patch+json"},
            verify=self.verify
        )
        assert result.status_code == 200
        return result.json()

    def _get_worker_group(self, raycluster: Dict[str, Any], group_name: str) -> Tuple[int, Dict[str, str]]:
        group_index = None
        group_spec = None
        worker_group_specs = raycluster["spec"]["workerGroupSpecs"]
        for index, spec in enumerate(worker_group_specs):
            if spec["groupName"] == group_name:
                group_index = index
                group_spec = spec
        assert group_index is not None and group_spec is not None
        return group_index, group_spec

    def _wait_for_pods(self, group_name: str, replicas: int):
        label_filters = to_label_selector({"ray.io/cluster": self.cluster_name, "ray.io/group": group_name})
        while True:
            pods = self._get("pods?labelSelector=" + requests.utils.quote(label_filters))
            logger.info("Currently have {} replicas of group {}, requested {}.".format(len(pods["items"]), group_name, replicas))
            if len(pods["items"]) == replicas:
                break
            else:
                logger.info("Still waiting for reconciler, number of replicas is {}, expected {}.".format(len(pods["items"]), replicas))
                time.sleep(10.0)

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Dict[str, Dict[str, str]]:
        """Creates a number of nodes within the namespace."""
        with self._lock:
            url = "rayclusters/{}".format(self.cluster_name)
            raycluster = self._get(url)
            group_name = tags["ray-user-node-type"]
            group_index, group_spec = self._get_worker_group(raycluster, group_name)
            path = f"/spec/workerGroupSpecs/{group_index}/replicas"
            payload = [
                {"op": "test", "path": path, "value": group_spec["replicas"]},
                {"op": "replace", "path": path, "value": group_spec["replicas"] + count}
            ]
            self._patch(url, payload)
            self._wait_for_pods(group_name, group_spec["replicas"] + count)
        return {}

    def internal_ip(self, node_id: str) -> str:
        data = self._get("pods/{}".format(node_id))
        return data["status"].get("podIP", "IP not yet assigned")

    def node_tags(self, node_id: str) -> Dict[str, str]:
        data = self._get("pods/{}".format(node_id))
        return make_node_tags(data["metadata"]["labels"], status_tag(data))

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.
        Also updates caches of ips and tags.
        """
        data = self._get("pods/")

        logger.info("Called non_terminated_nodes with tag_filters {}".format(tag_filters))

        result = []
        for pod in data["items"]:
            labels = pod["metadata"]["labels"]
            if labels.get("ray.io/cluster") == "raycluster-complete":
                def matches(tags, tag_filters):
                    for k, v in tag_filters.items():
                        if k not in tags or tags[k] != v:
                            return False
                    return True
                tags = make_node_tags(labels, status_tag(pod))
                if matches(tags, tag_filters):
                    result.append(pod["metadata"]["name"])

        logger.info("Result is {}".format(result))

        return result

    def terminate_node(self, node_id: str) -> None:
        """Terminates the specified node."""
        self.terminate_nodes([node_id])

    def terminate_nodes(self, node_ids: List[str]) -> Dict[str, Dict[str, str]]:
        with self._lock:
            # Split node_ids into groups according to node type and terminate them individually.
            # Note that in most cases, node_ids contains a single element and therefore it is
            # most likely not worth optimizing this code to batch the requests to the API server.
            groups = {}
            label_filters = to_label_selector({"ray.io/cluster": self.cluster_name})
            pods = self._get("pods?labelSelector=" + requests.utils.quote(label_filters))
            for pod in pods["items"]:
                if pod["metadata"]["name"] in node_ids:
                    groups.setdefault(pod["metadata"]["labels"]["ray.io/group"], []).append(pod["metadata"]["name"])

            url = "rayclusters/{}".format(self.cluster_name)
            raycluster = self._get(url)
            for group_name, nodes in groups.items():
                group_index, group_spec = self._get_worker_group(raycluster, group_name)
                prefix = f"/spec/workerGroupSpecs/{group_index}"
                payload = [
                    {"op": "test", "path": prefix + "/replicas", "value": group_spec["replicas"]},
                    {"op": "replace", "path": prefix + "/replicas", "value": group_spec["replicas"] - len(nodes)},
                    {"op": "replace", "path": prefix + "/scaleStrategy", "value": {"workersToDelete": nodes}}
                ]
                self._patch(url, payload)
                self._wait_for_pods(group_name, group_spec["replicas"] - len(nodes))
                # Clean up workersToDelete
                self._patch(url, [{"op": "replace", "path": prefix + "/scaleStrategy", "value": {"workersToDelete": []}}])
        return {}