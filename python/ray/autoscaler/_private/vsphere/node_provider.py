from ray.autoscaler.node_provider import NodeProvider
from typing import Any, Dict


class VsphereWcpNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        pass

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config

    def non_terminated_nodes(self, tag_filters):
        return []

    def is_running(self, node_id):
        return False

    def is_terminated(self, node_id):
        return True

    def node_tags(self, node_id):
        return None

    def external_ip(self, node_id):
        return None

    def internal_ip(self, node_id):
        # Currently vSphere VMs do not show an internal IP,
        # so we need to return the external IP.
        return None

    def set_node_tags(self, node_id, tags):
        pass

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to VM object for the created
        instances.
        """
        return {}

    def terminate_node(self, node_id):
        pass

    def terminate_nodes(self, node_ids):
        pass

    def safe_to_scale(self) -> bool:
        return False
