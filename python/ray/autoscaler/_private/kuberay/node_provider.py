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

        # requests.get("https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/raycluster-complete", headers=headers, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()

        # headers = {"Authorization": "Bearer " + open("/var/run/secrets/kubernetes.io/serviceaccount/token").read()}

        # requests.get("https://kubernetes.default:443/api/v1/namespaces/default/pods/", headers=headers, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()

        # scale up
        # requests.patch("https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/raycluster-complete", '[{"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 3}]', headers={**headers, "Content-type": "application/json-patch+json"}, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()

        # requests.get("https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/raycluster-complete", headers={**headers, "Content-type": "application/json-patch+json"}, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()

        # scale down

        # requests.patch("https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/raycluster
        # ...: -complete", '[{"op": "replace", "path": "/spec/workerGroupSpecs/0/scaleStrategy", "value": {"workersToDelete"
        # ...: : ["raycluster-complete-worker-small-group-jjvz4"]}}]', headers={**headers, "Content-type": "application/json
        # ...: -patch+json"}, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()

        # get ip

        # requests.get("https://kubernetes.default:443/api/v1/namespaces/default/pods/raycluster-complete-worker-small-group-2mbv6", headers=headers, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()["podIp"]

        ## requests.patch("https://kubernetes.default:443/apis/ray.io/v1alpha1/namespaces/default/rayclusters/raycluster-complete", '[{"op": "add", "path": "/spec/workerGroupSpecs/0/scaleStrategy/workersToDelete", "value": "raycluster-complete-worker-small-group-jjvz4"}]', headers={**headers, "Content-type": "application/json-patch+json"}, verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt").json()

        # c = kubernetes.client.ApiClient()
        # api_instance = kubernetes.client.CustomObjectsApi(c)
        # api_response = api_instance.get_namespaced_custom_object("ray.io", "v1alpha1", "default", "rayclusters", "raycluster-complete")

        # api_instance.patch_namespaced_custom_object("ray.io", "v1alpha1", "default", "rayclusters", "raycluster-complete", {"op": "replace", "path": "/spec/workerGroupSpecs/0/replicas", "value": 2})


        # core_api()

        return {}

    def internal_ip(self, node_id: str) -> str:
        url = "https://kubernetes.default:443/api/v1/namespaces/default/pods/{}".format(node_id)
        data = requests.get(url, headers=self.headers, verify=self.verify).json()
        return data["podIP"]

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict) from the tag cache.
        Falls back to direct RPC if necessary.
        """
        with self.cache_lock:
            tags = self.tag_cache.get(node_id, None)
            if tags is None:
                logger.warning(f"Tags of node {node_id} not found in the cache.")
                tags = self._node_tags_rpc(node_id)
                self.tag_cache[node_id] = tags
            return tags

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
        request = kuberay_pb2.TerminateNodesRequest(node_ids=node_ids)
        response = self._node_provider_stub.TerminateNodes(request)
        node_to_meta = _node_meta_response_to_dict(response)
        return node_to_meta