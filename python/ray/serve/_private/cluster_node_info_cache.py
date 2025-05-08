from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple, Union

import ray
from ray._raylet import GcsClient
from ray.serve._private.constants import RAY_GCS_RPC_TIMEOUT_S


class ClusterNodeInfoCache(ABC):
    """Provide access to cached node information in the cluster."""

    def __init__(self, gcs_client: GcsClient):
        self._gcs_client = gcs_client
        self._cached_alive_nodes = None
        self._cached_node_labels = dict()
        self._cached_total_resources_per_node = dict()
        self._cached_available_resources_per_node = dict()

    def update(self):
        """Update the cache by fetching latest node information from GCS.

        This should be called once in each update cycle.
        Within an update cycle, everyone will see the same
        cached node info avoiding any potential issues
        caused by inconsistent node info seen by different components.
        """
        nodes = self._gcs_client.get_all_node_info(timeout=RAY_GCS_RPC_TIMEOUT_S)
        alive_nodes = [
            (node_id.hex(), node.node_name, node.instance_id)
            for (node_id, node) in nodes.items()
            if node.state == ray.core.generated.gcs_pb2.GcsNodeInfo.ALIVE
        ]

        # Sort on NodeID to ensure the ordering is deterministic across the cluster.
        sorted(alive_nodes)
        self._cached_alive_nodes = alive_nodes
        self._cached_node_labels = {
            node_id.hex(): dict(node.labels) for (node_id, node) in nodes.items()
        }

        # Node resources
        self._cached_total_resources_per_node = {
            node_id.hex(): dict(node.resources_total)
            for (node_id, node) in nodes.items()
        }

        self._cached_available_resources_per_node = (
            ray._private.state.available_resources_per_node()
        )

    def get_alive_nodes(self) -> List[Tuple[str, str, str]]:
        """Get IDs, IPs, and Instance IDs for all live nodes in the cluster.

        Returns a list of (node_id: str, node_ip: str, instance_id: str).
        The node_id can be passed into the Ray SchedulingPolicy API.
        """
        return self._cached_alive_nodes

    def get_total_resources_per_node(self) -> Dict[str, Dict]:
        """Get total resources for alive nodes."""
        return self._cached_total_resources_per_node

    def get_alive_node_ids(self) -> Set[str]:
        """Get IDs of all live nodes in the cluster."""
        return {node_id for node_id, _, _ in self.get_alive_nodes()}

    @abstractmethod
    def get_draining_nodes(self) -> Dict[str, int]:
        """Get draining nodes in the cluster and their deadlines."""
        raise NotImplementedError

    @abstractmethod
    def get_node_az(self, node_id: str) -> Optional[str]:
        """Get availability zone of a node."""
        raise NotImplementedError

    def get_active_node_ids(self) -> Set[str]:
        """Get IDs of all active nodes in the cluster.

        A node is active if it's schedulable for new tasks and actors.
        """
        return self.get_alive_node_ids() - set(self.get_draining_nodes())

    def get_available_resources_per_node(self) -> Dict[str, Union[float, Dict]]:
        """Get available resources per node.

        Returns a map from (node_id -> Dict of resources).
        """

        return self._cached_available_resources_per_node


class DefaultClusterNodeInfoCache(ClusterNodeInfoCache):
    def __init__(self, gcs_client: GcsClient):
        super().__init__(gcs_client)

    def get_draining_nodes(self) -> Dict[str, int]:
        return dict()

    def get_node_az(self, node_id: str) -> Optional[str]:
        """Get availability zone of a node."""
        return None
