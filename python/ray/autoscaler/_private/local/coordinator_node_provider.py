import json
import logging
from http.client import RemoteDisconnected

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME

logger = logging.getLogger(__name__)


class CoordinatorSenderNodeProvider(NodeProvider):
    """NodeProvider for automatically managed private/local clusters.

    The cluster management is handled by a remote coordinating server.
    The server listens on <coordinator_address>, therefore, the address
    should be provided in the provider section in the cluster config.
    The server receieves HTTP requests from this class and uses
    LocalNodeProvider to get their responses.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.coordinator_address = provider_config["coordinator_address"]

    def _get_http_response(self, request):
        headers = {
            "Content-Type": "application/json",
        }
        request_message = json.dumps(request).encode()
        http_coordinator_address = "http://" + self.coordinator_address

        try:
            import requests  # `requests` is not part of stdlib.
            from requests.exceptions import ConnectionError

            r = requests.get(
                http_coordinator_address,
                data=request_message,
                headers=headers,
                timeout=None,
            )
        except (RemoteDisconnected, ConnectionError):
            logger.exception(
                "Could not connect to: "
                + http_coordinator_address
                + ". Did you run python coordinator_server.py"
                + " --ips <list_of_node_ips> --host <HOST> --port <PORT>?"
            )
            raise
        except ImportError:
            logger.exception(
                "Not all Ray Autoscaler dependencies were found. "
                "In Ray 1.4+, the Ray CLI, autoscaler, and dashboard will "
                'only be usable via `pip install "ray[default]"`. Please '
                "update your install command."
            )
            raise

        response = r.json()
        return response

    def non_terminated_nodes(self, tag_filters):
        # Only get the non terminated nodes associated with this cluster name.
        tag_filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        request = {"type": "non_terminated_nodes", "args": (tag_filters,)}
        return self._get_http_response(request)

    def is_running(self, node_id):
        request = {"type": "is_running", "args": (node_id,)}
        return self._get_http_response(request)

    def is_terminated(self, node_id):
        request = {"type": "is_terminated", "args": (node_id,)}
        return self._get_http_response(request)

    def node_tags(self, node_id):
        request = {"type": "node_tags", "args": (node_id,)}
        return self._get_http_response(request)

    def external_ip(self, node_id):
        request = {"type": "external_ip", "args": (node_id,)}
        response = self._get_http_response(request)
        return response

    def internal_ip(self, node_id):
        request = {"type": "internal_ip", "args": (node_id,)}
        response = self._get_http_response(request)
        return response

    def create_node(self, node_config, tags, count):
        # Tag the newly created node with this cluster name. Helps to get
        # the right nodes when calling non_terminated_nodes.
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        request = {
            "type": "create_node",
            "args": (node_config, tags, count),
        }
        self._get_http_response(request)

    def set_node_tags(self, node_id, tags):
        request = {"type": "set_node_tags", "args": (node_id, tags)}
        self._get_http_response(request)

    def terminate_node(self, node_id):
        request = {"type": "terminate_node", "args": (node_id,)}
        self._get_http_response(request)

    def terminate_nodes(self, node_ids):
        request = {"type": "terminate_nodes", "args": (node_ids,)}
        self._get_http_response(request)
