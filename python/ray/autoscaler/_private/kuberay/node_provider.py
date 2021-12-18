import json
import logging
import requests
import threading
import time
from typing import Any, Dict, List, Union

import kubernetes
from kubernetes.config.config_exception import ConfigException

from ray.autoscaler.node_provider import NodeProvider

# Terminology:
# We call the Kuberay labels "labels" and the Ray autoscaler tags "tags".
# The labes are prefixed by "ray.io". Tags are prefixed by "ray-".
# We convert between the two but do not mix them.


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
        tags["ray-user-node-type"] = "ray.head.default"
    else:
        tags["ray-node-type"] = "worker"
        tags["ray-user-node-type"] = "ray.worker.default"

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

    def _get(self, path):
        result = requests.get("https://kubernetes.default:443" + path, headers=self.headers, verify=self.verify)
        assert result.status_code == 200
        return result.json()

    def _patch(self, path, payload):
        result = requests.patch(
            "https://kubernetes.default:443" + path,
            json.dumps(payload),
            headers={**self.headers, "Content-type": "application/json-patch+json"},
            verify=self.verify
        )
        assert result.status_code == 200
        return result.json()


    def _modify_nodes(self, node_config: Dict[str, Any], tags: Dict[str, str], count: int, node_ids: List[str]):
        "Create or terminate nodes, if count > 0 nodes are created and if count < 0 they are deleted."
        # Check arguments for consistency
        if count >= 0:
            assert len(node_ids) == 0
        else:
            assert len(node_ids) == -count
        # Get the current number of replicas
        url = "/apis/ray.io/v1alpha1/namespaces/default/rayclusters/{}".format(self.cluster_name)
        data = self._get(url)
        old_replicas = data["spec"]["workerGroupSpecs"][0]["replicas"]
        # Compute the new number of replicas
        new_replicas = old_replicas + count
        # Update the number of replicas
        path = "/spec/workerGroupSpecs/0/replicas"
        payload = [
            {"op": "test", "path": path, "value": old_replicas},
            {"op": "replace", "path": path, "value": new_replicas}
        ]
        if node_ids != []:
            payload.append(
                {"op": "replace", "path": "/spec/workerGroupSpecs/0/scaleStrategy", "value": {"workersToDelete": node_ids}}
            )
        self._patch(url, payload)

        # Wait for new pods to be added to API server by the Kuberay reconciliator
        group_name = data["spec"]["workerGroupSpecs"][0]["groupName"]
        label_filters = to_label_selector({"ray.io/cluster": self.cluster_name, "ray.io/group": group_name})
        while True:
            pods = self._get("/api/v1/namespaces/default/pods?labelSelector=" + requests.utils.quote(label_filters))
            logger.info("Currently have {} replicas of group {}, requested {}.".format(len(pods["items"]), group_name, new_replicas))
            if len(pods["items"]) == new_replicas:
                if node_ids != []:
                    # Clean up workersToDelete (Kuberay doesn't seem to be doing it)
                    self._path(url, [{"op": "replace", "path": "/spec/workerGroupSpecs/0/scaleStrategy", "value": {"workersToDelete": []}}])
                break
            else:
                logger.info("Still waiting for reconciler, number of replicas is {}, expected {}.".format(len(pods["items"]), new_replicas))
                time.sleep(10.0)


    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Dict[str, Dict[str, str]]:
        """Creates a number of nodes within the namespace."""
        self._modify_nodes(node_config, tags, count, [])
        return {}

    def internal_ip(self, node_id: str) -> str:
        data = self._get("/api/v1/namespaces/default/pods/{}".format(node_id))
        return data["status"].get("podIP", "IP not yet assigned")

    def node_tags(self, node_id: str) -> Dict[str, str]:
        url = "https://kubernetes.default:443/api/v1/namespaces/default/pods/{}".format(node_id)
        data = requests.get(url, headers=self.headers, verify=self.verify).json()
        labels = data["metadata"]["labels"]
        return make_node_tags(labels, status_tag(data))

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.
        Also updates caches of ips and tags.
        """
        url = "https://kubernetes.default:443/api/v1/namespaces/default/pods/"
        data = requests.get(url, headers=self.headers, verify=self.verify).json()

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
        self._modify_nodes({}, {}, -len(node_ids), node_ids)
        return {}