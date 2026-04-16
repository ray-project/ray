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


@dataclass
class ReplicaSelection:
    """Represents a selected replica from choose_replica() context manager.

    Tracks whether dispatch() was called and provides timing information for
    metrics collection.
    """

    replica: "RunningReplica"  # Forward reference to avoid circular import
    """The selected replica to dispatch the request to."""

    selection_start_time: float = field(default_factory=time.monotonic)
    """Monotonic timestamp (seconds) when this selection's slot was reserved.
    Used internally to compute serve_selection_dispatch_gap_ms."""

    _dispatched: bool = field(default=False, repr=False)
    """Internal flag tracking whether dispatch() was called."""

    _release_callback: Optional[Callable[[], None]] = field(default=None, repr=False)
    """Optional callback to release the reserved slot."""

    def _mark_dispatched(self):
        """Mark this selection as dispatched."""
        self._dispatched = True

    def _release_slot(self):
        """Release the reserved slot if a callback was provided."""
        if self._release_callback is not None:
            self._release_callback()


@dataclass(frozen=True)
class ReplicaQueueLengthCacheEntry:
    queue_len: int
    timestamp: float


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
