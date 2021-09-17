from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_NODE_KIND, NODE_KIND_HEAD,
                                 TAG_RAY_USER_NODE_TYPE, TAG_RAY_NODE_NAME,
                                 TAG_RAY_NODE_STATUS, STATUS_UP_TO_DATE)


class ReadOnlyNodeProvider(NodeProvider):
    """A node provider that merely reports the current cluster state.

    This is used for laptop mode / manual cluster setup modes, in order to
    provide status reporting in the same way for users."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.nodes = {}

    def is_readonly(self):
        return True

    def _set_last_batch(self, batch):
        nodes = {}
        for msg in batch:
            node_id = msg.node_id.hex()
            # We make up a fake node type for each node (since each node
            # could have its own unique configuration).
            nodes[node_id] = {
                "node_type": "local_{}".format(node_id),
                "ip": msg.node_manager_address,
            }
        self.nodes = nodes

    def non_terminated_nodes(self, tag_filters):
        return list(self.nodes.keys())

    def is_running(self, node_id):
        return node_id in self.nodes

    def is_terminated(self, node_id):
        return node_id not in self.nodes

    def node_tags(self, node_id):
        tags = {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
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
        assert False

    def create_node(self, node_config, tags, count):
        assert False

    def terminate_node(self, node_id):
        assert False

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
