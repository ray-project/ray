import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set

from ray.serve._private.common import ReplicaID, RequestMetadata
from ray.serve._private.constants import (
    RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.serve._private.request_router.replica_wrapper import RunningReplica
    from ray.serve.handle import DeploymentHandle

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass()
class RequestRoutingContext:
    multiplexed_start_matching_time: Optional[float] = None
    tried_fewest_multiplexed_models: bool = False
    tried_first_multiplexed_models: bool = False
    tried_same_node: bool = False
    tried_same_az: bool = False
    should_backoff: bool = False


@PublicAPI(stability="alpha")
@dataclass
class PendingRequest:
    """A request that is pending execution by a replica."""

    args: List[Any]
    """Positional arguments for the request."""

    kwargs: Dict[Any, Any]
    """Keyword arguments for the request."""

    metadata: RequestMetadata
    """Metadata for the request, including request ID and whether it's streaming."""

    created_at: float = field(default_factory=lambda: time.time())
    """Timestamp when the request was created."""

    future: asyncio.Future = field(default_factory=lambda: asyncio.Future())
    """An asyncio Future that will be set when the request is routed."""

    routing_context: RequestRoutingContext = field(
        default_factory=RequestRoutingContext
    )
    """Context for request routing, used to track routing attempts and backoff."""

    resolved: bool = False
    """Whether the arguments have been resolved."""

    def reset_future(self):
        """Reset the `asyncio.Future`, must be called if this request is re-used."""
        self.future = asyncio.Future()


@dataclass(frozen=True)
class ReplicaQueueLengthCacheEntry:
    queue_len: int
    timestamp: float


@dataclass
class ReplicaSelection:
    """Represents a selected replica, holding information for dispatch or coordination.

    This class is returned by the choose_replica() context manager.
    The slot reservation lifecycle is managed by the context manager.
    """

    # Public, user-accessible fields
    replica_id: str
    """Unique identifier for the selected replica."""

    node_ip: str
    """IP address of the node running this replica."""

    port: Optional[int]
    """Port number for direct communication (if configured)."""

    node_id: str
    """Ray node ID where the replica is running."""

    availability_zone: Optional[str]
    """Cloud availability zone of the replica's node."""

    # Internal fields (not part of public API)
    _replica: "RunningReplica"
    _deployment_handle: "DeploymentHandle"
    _method_name: str
    _slot_token: str  # Token for reserved slot
    _dispatched: bool = field(
        default=False, init=False
    )  # Tracks if dispatch was called

    @property
    def address(self) -> str:
        """Returns the replica address in host:port format."""
        if self.port:
            return f"{self.node_ip}:{self.port}"
        return self.node_ip

    def to_dict(self) -> Dict[str, Any]:
        """Serialize public fields to a dictionary."""
        return {
            "replica_id": self.replica_id,
            "node_ip": self.node_ip,
            "port": self.port,
            "node_id": self.node_id,
            "availability_zone": self.availability_zone,
        }

    def _mark_dispatched(self) -> None:
        """Internal: Mark this selection as dispatched (slot consumed).

        Raises:
            RuntimeError: If the selection has already been dispatched.
        """
        if self._dispatched:
            raise RuntimeError(
                f"ReplicaSelection for {self.replica_id} has already been dispatched. "
                "Each selection can only be dispatched once."
            )
        self._dispatched = True

    def _release_slot(self) -> None:
        """Internal: Release the reserved slot."""
        self._replica.release_slot(self._slot_token)


class ReplicaQueueLengthCache:
    def __init__(
        self,
        *,
        staleness_timeout_s: float = RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S,
        get_curr_time_s: Optional[Callable[[], float]] = None,
    ):
        self._cache: Dict[ReplicaID, ReplicaQueueLengthCacheEntry] = {}
        self._staleness_timeout_s = staleness_timeout_s
        self._get_curr_time_s = (
            get_curr_time_s if get_curr_time_s is not None else lambda: time.time()
        )

    def _is_timed_out(self, timestamp_s: int) -> bool:
        return self._get_curr_time_s() - timestamp_s > self._staleness_timeout_s

    def get(self, replica_id: ReplicaID) -> Optional[int]:
        """Get the queue length for a replica.

        Returns `None` if the replica ID is not present or the entry is timed out.
        """
        entry = self._cache.get(replica_id)
        if entry is None or self._is_timed_out(entry.timestamp):
            return None

        return entry.queue_len

    def update(self, replica_id: ReplicaID, queue_len: int):
        """Set (or update) the queue length for a replica ID."""
        self._cache[replica_id] = ReplicaQueueLengthCacheEntry(
            queue_len, self._get_curr_time_s()
        )

    def invalidate_key(self, replica_id: ReplicaID):
        self._cache.pop(replica_id, None)

    def remove_inactive_replicas(self, *, active_replica_ids: Set[ReplicaID]):
        """Removes entries for all replica IDs not in the provided active set."""
        # NOTE: the size of the cache dictionary changes during this loop.
        for replica_id in list(self._cache.keys()):
            if replica_id not in active_replica_ids:
                self._cache.pop(replica_id)
