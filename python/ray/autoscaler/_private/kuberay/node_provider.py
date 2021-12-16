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
        """Terminates a set of nodes.
        Returns map of deleted node ids to node metadata.
        """
        # Get the current number of replicas
        url = "https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/{}".format(self.cluster_name)
        data = requests.get(url, headers=self.headers, verify=self.verify).json()
        old_replicas = data["spec"]["workerGroupSpecs"][0]["replicas"]
        # Compute the new number of replicas
        new_replicas = old_replicas - len(node_ids)
        # Update the number of replicas
        path = "/spec/workerGroupSpecs/0/replicas"
        # TODO: Add retry loop if test and set fails
        command = [
            {"op": "test", "path": path, "value": old_replicas},
            {"op": "replace", "path": path, "value": new_replicas},
            # {"op": "add", "path": "/spec/workerGroupSpecs/0/scaleStrategy/workersToDelete", "value": node_ids}
            {"op": "replace", "path": "/spec/workerGroupSpecs/0/scaleStrategy", "value": {"workersToDelete": node_ids}},
        ]
        result = requests.patch(url, json.dumps(command), headers={**self.headers, "Content-type": "application/json-patch+json"}, verify=self.verify).json()
        logger.info("result from terminate_nodes = {}".format(result))
        return {}