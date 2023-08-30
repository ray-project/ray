from abc import ABC, abstractmethod
from typing import (
    Optional,
    Set,
    List,
    Tuple,
)

import ray
from ray._raylet import GcsClient
from ray.serve._private.constants import RAY_GCS_RPC_TIMEOUT_S


class ClusterNodeInfoCache(ABC):
    """Provide access to cached node information in the cluster."""

    def __init__(self, gcs_client: GcsClient):
        self._gcs_client = gcs_client
        self._cached_alive_nodes = None
        self._cached_node_labels = dict()

    def update(self):
        """Update the cache by fetching latest node information from GCS.

        This should be called once in each update cycle.
        Within an update cycle, everyone will see the same
        cached node info avoiding any potential issues
        caused by inconsistent node info seen by different components.
        """
        nodes = self._gcs_client.get_all_node_info(timeout=RAY_GCS_RPC_TIMEOUT_S)
        alive_nodes = [
            (ray.NodeID.from_binary(node_id).hex(), node["node_name"].decode("utf-8"))
            for (node_id, node) in nodes.items()
            if node["state"] == ray.core.generated.gcs_pb2.GcsNodeInfo.ALIVE
        ]

        # Sort on NodeID to ensure the ordering is deterministic across the cluster.
        sorted(alive_nodes)
        self._cached_alive_nodes = alive_nodes
        self._cached_node_labels = {
            ray.NodeID.from_binary(node_id).hex(): {
                label_name.decode("utf-8"): label_value.decode("utf-8")
                for label_name, label_value in node["labels"].items()
            }
            for (node_id, node) in nodes.items()
        }

    def get_alive_nodes(self) -> List[Tuple[str, str]]:
        """Get IDs and IPs for all live nodes in the cluster.

        Returns a list of (node_id: str, ip_address: str). The node_id can be
        passed into the Ray SchedulingPolicy API.
        """
        return self._cached_alive_nodes

    def get_alive_node_ids(self) -> Set[str]:
        """Get IDs of all live nodes in the cluster."""
        return {node_id for node_id, _ in self.get_alive_nodes()}

    @abstractmethod
    def get_draining_node_ids(self) -> Set[str]:
        """Get IDs of all draining nodes in the cluster."""
        raise NotImplementedError

    @abstractmethod
    def get_node_az(self, node_id: str) -> Optional[str]:
        """Get availability zone of a node."""
        raise NotImplementedError

    def get_active_node_ids(self) -> Set[str]:
        """Get IDs of all active nodes in the cluster.

        A node is active if it's schedulable for new tasks and actors.
        """
        return self.get_alive_node_ids() - self.get_draining_node_ids()


class DefaultClusterNodeInfoCache(ClusterNodeInfoCache):
    def __init__(self, gcs_client: GcsClient):
        super().__init__(gcs_client)

    def get_draining_node_ids(self) -> Set[str]:
        return set()

    def get_node_az(self, node_id: str) -> Optional[str]:
        """Get availability zone of a node."""
        return None
