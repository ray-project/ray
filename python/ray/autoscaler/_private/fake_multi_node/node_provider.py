from typing import Tuple, List

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_NODE_KIND, NODE_KIND_HEAD, NODE_KIND_WORKER,
                                 TAG_RAY_USER_NODE_TYPE, TAG_RAY_NODE_NAME,
                                 TAG_RAY_NODE_STATUS, STATUS_UP_TO_DATE)
from ray.autoscaler._private.util import format_readonly_node_type


class FakeMultiNodeProvider(NodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of autoscaling functionality."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.nodes = {
            "head": {
                "node_type": "ray.head.default",
            },
        }

    def non_terminated_nodes(self, tag_filters):
        return list(self.nodes.keys())

    def is_running(self, node_id):
        return node_id in self.nodes

    def is_terminated(self, node_id):
        return node_id not in self.nodes

    def node_tags(self, node_id):
        tags = {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD if node_id == "head" else NODE_KIND_WORKER,
            TAG_RAY_USER_NODE_TYPE: self.nodes[node_id]["node_type"],
            TAG_RAY_NODE_NAME: node_id,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
        }
        return tags

    def external_ip(self, node_id):
        return node_id

    def internal_ip(self, node_id):
        return node_id

    def set_node_tags(self, node_id, tags):
        raise AssertionError("Readonly node provider cannot be updated")

    def create_node(self, node_config, tags, count):
        raise AssertionError("Readonly node provider cannot be updated")

    def terminate_node(self, node_id):
        raise AssertionError("Readonly node provider cannot be updated")

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
