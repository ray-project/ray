import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from ray.serve._private.common import ReplicaID, RequestMetadata
from ray.serve._private.constants import (
    RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class PendingRequest:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata
    created_at: float = field(default_factory=time.time)
    future: asyncio.Future = field(default_factory=lambda: asyncio.Future())

    def reset_future(self):
        """Reset the `asyncio.Future`, must be called if this request is re-used."""
        self.future = asyncio.Future()


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
            get_curr_time_s if get_curr_time_s is not None else time.time
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
