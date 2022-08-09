import datetime
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from cachetools import TTLCache

from ray.autoscaler._private.constants import (
    AUTOSCALER_NODE_AVAILABILITY_MAX_STALENESS_S,
)
from ray.autoscaler.node_launch_exception import NodeLaunchException


@dataclass
class UnavailableNodeInformation:
    category: str
    description: str


@dataclass
class NodeAvailabilityRecord:
    node_type: str
    is_available: bool
    last_checked_timestamp: float
    unavailable_node_information: Optional[UnavailableNodeInformation]


@dataclass
class NodeAvailabilitySummary:
    node_availabilities: Dict[
        str, NodeAvailabilityRecord
    ]  # Mapping from node type to node availability record.

    def __str__(self) -> str:
        return ""


class NodeProviderAvailabilityTracker:
    """A thread safe, TTL cache of node provider availability. We don't use
    cachetools.TTLCache because we want fine grain control over when entries
    expire (e.g. insert an entry at a previous point in time)."""

    def __init__(
        self,
        timer: Callable[[], float] = time.time,
        ttl: float = AUTOSCALER_NODE_AVAILABILITY_MAX_STALENESS_S,
    ):
        """
        A cache that tracks the availability of nodes and throw away entries which have grown too stale.

        Args:
          timer: A function that returns the current time in seconds.
          ttl: The ttl from the insertion timestamp of an entry.
        """
        self.timer = timer
        self.ttl = ttl
        # Mapping from node type to (eviction_time, record)
        self.store: Dict[str, Tuple[float, NodeAvailabilityRecord]] = {}
        # A global lock to simplify thread safety handling.
        self.lock = threading.RLock()

    def _unsafe_update_node_availability(
        self,
        node_type: str,
        timestamp: int,
        node_launch_exception: Optional[NodeLaunchException],
    ) -> None:
        if node_launch_exception is None:
            record = NodeAvailabilityRecord(
                node_type=node_type,
                is_available=True,
                last_checked_timestamp=timestamp,
                unavailable_node_information=None,
            )
        else:
            info = UnavailableNodeInformation(
                category=node_launch_exception.category,
                description=node_launch_exception.description,
            )
            record = NodeAvailabilityRecord(
                node_type=node_type,
                is_available=False,
                last_checked_timestamp=timestamp,
                unavailable_node_information=None,
            )

        if node_type in self.store:
            del self.store[node_type]

        expiration_time = timestamp + self.ttl
        self.store[node_type] = (expiration_time, record)

        self._remove_old_entries()

    def update_node_availability(
        self,
        node_type: str,
        timestamp: int,
        node_launch_exception: Optional[NodeLaunchException],
    ) -> None:
        """
        Update the availability and details of a single ndoe type.

        Args:
          node_type: The node type.
          timestamp: The timestamp that this information is accurate as of.
          node_launch_exception: Details about why the node launch failed. If empty, the node type will be considered available.
        """
        with self.lock:
            self._unsafe_update_node_availability(
                node_type, timestamp, node_launch_exception
            )

    def summary(self) -> NodeAvailabilitySummary:
        """
        Returns a summary of node availabilities and their staleness.

        Returns
            A summary of node availabilities and their staleness.
        """
        self._remove_old_entries()
        return NodeAvailabilitySummary(
            {node_type: record for node_type, (_, record) in self.store.items()}
        )

    def _remove_old_entries(self):
        """Remove any expired entries from the cache. Note that python
        dictionaries are ordered by insertion time and we always reinsert
        entries upon updating them, therefore we iterate through the dictionary
        in expiration order.
        """
        cur_time = self.timer()
        with self.lock:
            for key, (expiration_time, _) in list(self.store.items()):
                if expiration_time < cur_time:
                    del self.store[key]
                else:
                    break
