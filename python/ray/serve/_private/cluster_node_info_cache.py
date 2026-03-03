import logging
from abc import ABC, abstractmethod
from typing import Dict, FrozenSet, List, Optional, Set, Tuple, Union

import ray
from ray._common.utils import binary_to_hex
from ray._raylet import GcsClient
from ray.serve._private.constants import RAY_GCS_RPC_TIMEOUT_S, SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ClusterNodeInfoCache(ABC):
    """Provide access to cached node information in the cluster."""

    def __init__(self, gcs_client: GcsClient):
        self._gcs_client = gcs_client
        self._cached_alive_nodes = None
        self._cached_node_labels = dict()
        self._cached_total_resources_per_node = dict()
        self._cached_available_resources_per_node = dict()
        # Track alive node IDs to detect cluster membership changes and skip
        # rebuilding labels / total resources when nothing changed.
        self._alive_node_id_set: FrozenSet[str] = frozenset()

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
        alive_nodes.sort()
        self._cached_alive_nodes = alive_nodes

        # Detect whether the set of alive nodes has changed. Rebuild labels
        # and total resources only when it has, since they are static per-node
        # properties that don't change while a node stays alive.
        current_alive_ids = frozenset(node_id for node_id, _, _ in alive_nodes)
        if current_alive_ids != self._alive_node_id_set:
            self._alive_node_id_set = current_alive_ids
            self._cached_node_labels = {
                node_id.hex(): dict(node.labels)
                for (node_id, node) in nodes.items()
                if node_id.hex() in current_alive_ids
            }
            self._cached_total_resources_per_node = {
                node_id.hex(): dict(node.resources_total)
                for (node_id, node) in nodes.items()
                if node_id.hex() in current_alive_ids
            }

        # Fetch available resources using the existing GCS client rather than
        # the legacy GlobalStateAccessor path (which opens a second connection
        # and performs redundant protobuf deserialization).
        self._cached_available_resources_per_node = (
            self._fetch_available_resources_per_node()
        )

    def _fetch_available_resources_per_node(self) -> Dict[str, Dict[str, float]]:
        """Fetch available resources per alive node via get_all_resource_usage()."""
        try:
            reply = self._gcs_client.get_all_resource_usage(
                timeout=RAY_GCS_RPC_TIMEOUT_S
            )
        except Exception:
            logger.warning(
                "Failed to fetch resource usage from GCS. "
                "Available resources cache will be stale.",
                exc_info=True,
            )
            return self._cached_available_resources_per_node

        return {
            node_id: dict(resource_data.resources_available)
            for resource_data in reply.resource_usage_data.batch
            if (node_id := binary_to_hex(resource_data.node_id))
            in self._alive_node_id_set
        }

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

    def get_node_labels(self, node_id: str) -> Dict[str, str]:
        """Get the labels for a specific node from the cache."""
        return self._cached_node_labels.get(node_id, {})


class DefaultClusterNodeInfoCache(ClusterNodeInfoCache):
    def __init__(self, gcs_client: GcsClient):
        super().__init__(gcs_client)

    def get_draining_nodes(self) -> Dict[str, int]:
        return dict()

    def get_node_az(self, node_id: str) -> Optional[str]:
        """Get availability zone of a node."""
        return None
