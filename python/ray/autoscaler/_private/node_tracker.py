from ray.autoscaler._private import constants
from typing import List, Set, Tuple


class NodeTracker:
    """Map nodes to their corresponding logs.

    We need to be a little careful here. At an given point in time, node_id <->
    ip can be interchangeably used, but the node_id -> ip relation is not
    bijective _across time_ since IP addresses can be reused. Therefore, we
    should treat node_id as the only unique identifier.
    """

    def __init__(self):
        # Mapping from node_id -> (ip, node type, stdout_path, process runner)
        self.node_mapping = {}

        # A quick, inefficient FIFO cache implementation.
        self.lru_order = []

    def _add_node_mapping(self, node_id: str, value: str):
        if node_id in self.node_mapping:
            return

        assert len(self.lru_order) == len(self.node_mapping)
        if len(self.lru_order) >= constants.AUTOSCALER_MAX_NODES_TRACKED:
            # The LRU eviction case
            node_id = self.lru_order.pop(0)
            del self.node_mapping[node_id]

        self.node_mapping[node_id] = value
        self.lru_order.append(node_id)

    def track(self, node_id: str, ip: str, node_type: str):
        """
        Begin to track a new node.

        Args:
            node_id (str): The node id.
            ip (str): The node ip address.
            node_type (str): The node type.
        """
        if node_id not in self.node_mapping:
            self._add_node_mapping(node_id, (ip, node_type))

    def untrack(self, node_id: str):
        """Gracefully stop tracking a node. If a node is intentionally removed from
        the cluster, we should stop tracking it so we don't mistakenly mark it
        as failed.

        Args:
            node_id (str): The node id which failed.
        """
        if node_id in self.node_mapping:
            self.lru_order.remove(node_id)
            del self.node_mapping[node_id]

    def get_all_failed_node_info(
            self, non_failed_ids: Set[str]) -> List[Tuple[str, str]]:
        """Get the information about all failed nodes. A failed node is any node which
        we began to track that is not pending or alive (i.e. not failed).

        Args:
            non_failed_ids (set): Nodes are failed unless they are in this set.

        Returns:
            List[Tuple[str, str]]: A list of tuples. Each tuple is the ip
            address and type of a failed node.
        """
        failed_nodes = self.node_mapping.keys() - non_failed_ids
        failed_info = []
        # Returning the list in order is important for display purposes.
        for node_id in filter(lambda node_id: node_id in failed_nodes,
                              self.lru_order):
            failed_info.append(self.node_mapping[node_id])
        return failed_info
