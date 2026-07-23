import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, FrozenSet, List, NamedTuple, Optional, Set, Tuple

import ray
from ray._common.utils import binary_to_hex
from ray._raylet import GcsClient  # type: ignore[attr-defined]
from ray.serve._private.constants import RAY_GCS_RPC_TIMEOUT_S, SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


class _NodeInfoSnapshot(NamedTuple):
    """The node-info fields refresh computes and apply_pending swaps into the cache."""

    alive_nodes: List[Tuple[str, str, str]]
    alive_node_id_set: FrozenSet[str]
    node_labels: Dict[str, Dict[str, str]]
    total_resources_per_node: Dict[str, Dict]
    available_resources_per_node: Dict[str, Dict[str, float]]


class ClusterNodeInfoCache(ABC):
    """Provide access to cached node information in the cluster."""

    def __init__(self, gcs_client: GcsClient):
        self._gcs_client = gcs_client
        self._cached_alive_nodes: Optional[List[Tuple[str, str, str]]] = None
        self._cached_node_labels: Dict[str, Dict[str, str]] = dict()
        self._cached_total_resources_per_node: Dict[str, Dict] = dict()
        self._cached_available_resources_per_node: Dict[str, Dict[str, float]] = dict()
        # Track alive node IDs to detect cluster membership changes and skip
        # rebuilding labels / total resources when nothing changed.
        self._alive_node_id_set: FrozenSet[str] = frozenset()
        self._refresh_in_flight: bool = False
        # Staged by refresh_async(); promoted by apply_pending() (see below).
        self._pending_snapshot: Optional[_NodeInfoSnapshot] = None

    def update(self):
        """Update the cache by fetching latest node information from GCS (blocking).

        Applies synchronously. Used at controller startup (to warm the cache before the
        control loop starts) and on the shutdown path, where the background refresh loop
        has already exited.
        """
        self._apply_snapshot(self._compute_snapshot(self._prior_state()))

    async def refresh_async(self):
        """Fetch node info off the event loop and STAGE it (apply_pending promotes
        it). The GCS calls release the GIL, so the executor runs while a slow reply is
        in flight. Single-flight: at most one refresh at a time."""
        if self._refresh_in_flight:
            return
        self._refresh_in_flight = True
        # Capture the carry-forward state on the event-loop thread so the executor
        # never reads self while apply_pending() may be swapping in a newer snapshot.
        prior = self._prior_state()
        try:
            loop = asyncio.get_running_loop()
            self._pending_snapshot = await loop.run_in_executor(
                None, self._compute_snapshot, prior
            )
        except Exception:
            logger.warning(
                "Async node-info refresh failed; node info cache will be stale.",
                exc_info=True,
            )
        finally:
            self._refresh_in_flight = False

    def apply_pending(self) -> None:
        """Promote the latest staged snapshot into the live cache. Called at the top
        of each tick before any reader, so the cache stays immutable for the tick and
        every component sees one consistent node-info view (the invariant the old
        per-cycle update() held)."""
        snapshot = self._pending_snapshot
        if snapshot is not None:
            self._pending_snapshot = None
            self._apply_snapshot(snapshot)

    def is_refresh_in_flight(self) -> bool:
        """True while a background refresh_async() is fetching from the GCS."""
        return self._refresh_in_flight

    def _apply_snapshot(self, snapshot: _NodeInfoSnapshot):
        # Positional unpack, no await -> atomic on the event-loop thread.
        (
            self._cached_alive_nodes,
            self._alive_node_id_set,
            self._cached_node_labels,
            self._cached_total_resources_per_node,
            self._cached_available_resources_per_node,
        ) = snapshot

    def _prior_state(self):
        """Capture (on the event-loop thread) the fields _compute_snapshot carries
        forward, so the executor never reads them while apply_pending() may be swapping
        in a newer snapshot."""
        return (
            self._alive_node_id_set,
            self._cached_node_labels,
            self._cached_total_resources_per_node,
            self._cached_available_resources_per_node,
        )

    def _compute_snapshot(self, prior):
        """Fetch + compute the node-info snapshot from GCS without reading or mutating
        self (other than the immutable GCS client), so it is safe to run in an executor
        thread. `prior` is the carry-forward state captured by the caller via
        _prior_state(). Returns the _NodeInfoSnapshot applied by _apply_snapshot."""
        (
            prior_alive_ids,
            prior_node_labels,
            prior_total_resources,
            prior_available,
        ) = prior
        nodes = self._gcs_client.get_all_node_info(timeout=RAY_GCS_RPC_TIMEOUT_S)
        alive_nodes = [
            (node_id.hex(), node.node_name, node.instance_id)
            for (node_id, node) in nodes.items()
            # `ray.core.generated` is a compiled-proto package that only exists
            # in built environments.
            if node.state
            == ray.core.generated.gcs_pb2.GcsNodeInfo.ALIVE  # pyrefly: ignore[missing-attribute]
        ]
        # Sort on NodeID to ensure the ordering is deterministic across the cluster.
        alive_nodes.sort()

        # Detect whether the set of alive nodes has changed. Rebuild labels and total
        # resources only when it has (static per-node properties).
        current_alive_ids = frozenset(node_id for node_id, _, _ in alive_nodes)
        if current_alive_ids != prior_alive_ids:
            node_labels = {
                node_id.hex(): dict(node.labels)
                for (node_id, node) in nodes.items()
                if node_id.hex() in current_alive_ids
            }
            total_resources = {
                node_id.hex(): dict(node.resources_total)
                for (node_id, node) in nodes.items()
                if node_id.hex() in current_alive_ids
            }
        else:
            node_labels = prior_node_labels
            total_resources = prior_total_resources

        available = self._fetch_available_resources_per_node(
            current_alive_ids, prior_available
        )
        return _NodeInfoSnapshot(
            alive_nodes,
            current_alive_ids,
            node_labels,
            total_resources,
            available,
        )

    def _fetch_available_resources_per_node(
        self, alive_id_set: FrozenSet[str], prior_available: Dict[str, Dict[str, float]]
    ) -> Dict[str, Dict[str, float]]:
        """Fetch available resources per alive node via get_all_resource_usage().

        `prior_available` is the carry-forward value returned if the GCS call fails; it
        is passed in (not read from self) so this stays safe in an executor thread.
        """
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
            return prior_available

        return {
            node_id: dict(resource_data.resources_available)
            for resource_data in reply.resource_usage_data.batch
            if (node_id := binary_to_hex(resource_data.node_id)) in alive_id_set
        }

    def get_alive_nodes(self) -> List[Tuple[str, str, str]]:
        """Get IDs, IPs, and Instance IDs for all live nodes in the cluster.

        Returns a list of (node_id: str, node_ip: str, instance_id: str).
        The node_id can be passed into the Ray SchedulingPolicy API.
        """
        return self._cached_alive_nodes  # type: ignore[return-value]

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

    def get_available_resources_per_node(self) -> Dict[str, Dict[str, float]]:
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
