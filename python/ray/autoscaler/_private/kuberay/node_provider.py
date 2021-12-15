import json
import logging
import requests
import threading
from typing import Any, Dict, List, Union

import kubernetes
from kubernetes.config.config_exception import ConfigException

from ray.autoscaler.node_provider import NodeProvider


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


    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Dict[str, Dict[str, str]]:
        """Creates a number of nodes within the namespace.
        Returns a mapping from created node ids to node metadata.
        """
        # Get the current number of replicas
        url = "https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/{}".format(self.cluster_name)
        data = requests.get(url, headers=self.headers, verify=self.verify).json()
        old_replicas = data["spec"]["workerGroupSpecs"][0]["replicas"]
        # Compute the new number of replicas
        new_replicas = old_replicas + count
        # Update the number of replicas
        path = "/spec/workerGroupSpecs/0/replicas"
        # TODO: Add retry loop if test and set fails
        command = [{"op": "test", "path": path, "value": old_replicas}, {"op": "replace", "path": path, "value": new_replicas}]
        data = requests.patch(url, json.dumps(command), headers={**self.headers, "Content-type": "application/json-patch+json"}, verify=self.verify).json()

        return data

    def internal_ip(self, node_id: str) -> str:
        url = "https://kubernetes.default:443/api/v1/namespaces/default/pods/{}".format(node_id)
        data = requests.get(url, headers=self.headers, verify=self.verify).json()
        return data["status"].get("podIP", "IP not yet assigned")

    def node_tags(self, node_id: str) -> Dict[str, str]:
        url = "https://kubernetes.default:443/api/v1/namespaces/default/pods/{}".format(node_id)
        data = requests.get(url, headers=self.headers, verify=self.verify).json()
        tags = data["metadata"]["labels"]
        result = tags.copy()
        result["ray-node-status"] = status_tag(data)
        if result["ray.io/node-type"] == "head":
            result["ray-node-type"] = "head"
            result["ray-user-node-type"] = "ray.head.default"
        else:
            result["ray-node-type"] = "worker"
            result["ray-user-node-type"] = "ray.worker.default"
        return result

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.
        Also updates caches of ips and tags.
        """
        url = "https://kubernetes.default:443/api/v1/namespaces/default/pods/"
        data = requests.get(url, headers=self.headers, verify=self.verify).json()

        return [pod["metadata"]["name"] for pod in data["items"] if "ray.io/cluster" in pod["metadata"]["labels"] and pod["metadata"]["labels"]["ray.io/cluster"] == "raycluster-complete"]

    def terminate_node(self, node_id: str) -> None:
        """Terminates the specified node."""
        self.terminate_nodes([node_id])

    def terminate_nodes(self, node_ids: List[str]) -> Dict[str, Dict[str, str]]:
        """Terminates a set of nodes.
        Returns map of deleted node ids to node metadata.
        """
        # TODO Implement node termination
        return {}