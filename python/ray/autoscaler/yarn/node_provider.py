from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.autoscaler.node_provider import NodeProvider

logger = logging.getLogger(__name__)


class YARNNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

    def non_terminated_nodes(self, tag_filters):
        pass

    def is_running(self, node_id):
        pass

    def is_terminated(self, node_id):
        pass

    def node_tags(self, node_id):
        pass

    def external_ip(self, node_id):
        pass

    def internal_ip(self, node_id):
        pass

    def set_node_tags(self, node_id, tags):
        pass

    def create_node(self, node_config, tags, count):
        pass

    def terminate_node(self, node_id):
        pass

    def terminate_nodes(self, node_ids):
        pass
