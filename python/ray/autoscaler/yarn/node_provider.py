from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.yarn.config import WORKER_SERVICE
from ray.tags import TAG_RAY_NODE_TYPE, NODE_TYPE_HEAD

logger = logging.getLogger(__name__)


def match_filter(tags, tag_filters):
    for k, v in tag_filters:
        if k not in tags or tags[k] != v:
            return False

    return True


class WorkerNode(object):
    def __init__(self, container, tags=None):
        self.container = container
        self.tags = tags if tags else {}


class YARNNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        # Mapping from node_id to WorkerNode.
        self.nodes = {}

    @property
    def app_client(self):
        if not hasattr(self, "_app_client"):
            from skein import ApplicationClient
            self._app_client = ApplicationClient.from_current()
        return self._app_client

    def non_terminated_nodes(self, tag_filters):
        node_ids = []
        for node_id, node in self.nodes:
            if match_filter(node.tags, tag_filters):
                node_ids.append(node_id)

        # todo: actually check status
        return node_ids

    def is_running(self, node_id):
        # todo: actually check status
        return node_id in self.nodes

    def is_terminated(self, node_id):
        # todo: actually check status
        return node_id not in self.nodes

    def node_tags(self, node_id):
        return self.nodes[node_id].tags

    def external_ip(self, node_id):
        raise NotImplementedError("Must use internal IPs with YARN.")

    def internal_ip(self, node_id):
        return self.nodes.yarn_node_http_address.split(":")[0]

    def set_node_tags(self, node_id, tags):
        self.nodes[node_id].tags.update(tags)

    def _create_head_node(self, node_config, tags):
        raise NotImplementedError("Starting the head node with 'ray up' not "
                                  "implemented in YARN.")

    def _create_worker_node(self, node_config, tags, count):
        for _ in range(count):
            container = self.app_client.add_container(WORKER_SERVICE)
            self.nodes[container.id] = WorkerNode(container, tags)

    def create_node(self, node_config, tags, count):
        # todo: does this tag get set here?
        if (TAG_RAY_NODE_TYPE in tags
                and tags[TAG_RAY_NODE_TYPE] == NODE_TYPE_HEAD):
            assert count == 1, "Can only create one head node."
            self._create_head_node(node_config, tags)
        else:
            self._create_worker_node(node_config, tags, count)

    def terminate_node(self, node_id):
        # todo what to do about head node?
        self.app_client.kill_container(node_id)
        del self.nodes[node_id]

    def terminate_nodes(self, node_ids):
        for node_id in node_ids:
            self.terminate_node(node_id)
