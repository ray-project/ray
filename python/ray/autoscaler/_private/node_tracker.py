import os
import sys

from ray.autoscaler._private import constants


class NodeTracker:
    """Map nodes to their corresponding logs.

    We need to be a little careful here. At an given point in time, node_id <->
    ip can be interchangeably used, but the node_id -> ip relation is not
    bijective _across time_ since IP addresses can be reused. Therefore, we
    should treat node_id as the only unique identifier.

    """

    def __init__(self):
        # Mapping from node_id -> (ip, node type, stdout_path, process runner)
        # TODO(Alex): In the spirit of statelessness, we should try to load
        # this mapping from the filesystem. _Technically_ this tracker is only
        # used for "recent" failures though, so remembering old nodes is a best
        # effort, therefore it's already correct
        self.node_mapping = {}

        # A quick, inefficient FIFO cache implementation.
        self.lru_order = []

    def _add_node_mapping(self, node_id, value):
        if node_id in self.node_mapping:
            return

        assert len(self.lru_order) == len(self.node_mapping)
        if len(self.lru_order) >= constants.AUTOSCALER_MAX_NODES_TRACKED:
            # The LRU eviction case
            node_id = self.lru_order.pop(0)
            del self.node_mapping[node_id]

        self.node_mapping[node_id] = value
        self.lru_order.append(node_id)

    def track(self, node_id, ip=None, node_type=None):
        if node_id not in self.node_mapping:
            self._add_node_mapping(
                node_id, (ip, node_type))

    def get_all_failed_node_info(self, non_failed_ids):
        failed_nodes = self.node_mapping.keys() - non_failed_ids
        failed_info = []
        # Returning the list in order is important for display purposes.
        for node_id in filter(lambda node_id: node_id in failed_nodes,
                              self.lru_order):
            failed_info.append(self.node_mapping[node_id])
        return failed_info
