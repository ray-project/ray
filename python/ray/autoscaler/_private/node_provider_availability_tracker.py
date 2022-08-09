import datetime
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

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

    @classmethod
    def from_fields(cls, **fields) -> Optional["NodeAvailabilitySummary"]:
        """Implement marshalling from nested fields. pydantic isn't a core dependency
        so we're implementing this by hand instead."""
        parsed = {}

        node_availabilites_dict = fields.get("node_availabilities", {})

        for node_type, node_availability_record_dict in node_availabilites_dict.items():
            unavailable_information_dict = node_availability_record_dict.pop(
                "unavailable_node_information", None
            )
            unavaiable_information = None
            if unavailable_information_dict is not None:
                unavaiable_information = UnavailableNodeInformation(
                    **unavailable_information_dict
                )

            parsed[node_type] = NodeAvailabilityRecord(
                unavailable_node_information=unavaiable_information,
                **node_availability_record_dict,
            )

        return NodeAvailabilitySummary(node_availabilities=parsed)

    def summary_string(self, separator_len: int) -> str:
        if self:
            formatted_lines = []
            sorted_keys = sorted(
                self.node_availabilities.keys(),
                key=lambda node_type: self.node_availabilities[
                    node_type
                ].last_checked_timestamp,
            )
            for node_type in sorted_keys:
                record = self.node_availabilities[node_type]
                category = "Available"
                if not record.is_available:
                    assert record.unavailable_node_information is not None
                    category = record.unavailable_node_information.category
                attempted_time = datetime.datetime.fromtimestamp(
                    record.last_checked_timestamp
                )
                formatted_time = (
                    f"{attempted_time.hour}:"
                    f"{attempted_time.minute}:"
                    f"{attempted_time.second}"
                )
                formatted_line = (
                    f" {node_type} (attempted={formatted_time}): {category}"
                )
                formatted_lines.append(formatted_line)

            return (
                "Launches\n"
                + ("-" * separator_len)
                + "\n"
                + "Node types:\n"
                + ("\n".join(formatted_lines))
            )

        else:
            return ""
        pass

    def __eq__(self, other: "NodeAvailabilitySummary"):
        return self.node_availabilities == other.node_availabilities

    def __bool__(self) -> bool:
        return bool(self.node_availabilities)


class NodeProviderAvailabilityTracker:
    """A thread safe, TTL cache of node provider availability. We don't use
    cachetools.TTLCache because we want fine grain control over when entries
    expire (e.g. insert an entry at a previous point in time)."""

    def __init__(
        self,
        timer: Callable[[], float] = time.time,
        ttl: float = AUTOSCALER_NODE_AVAILABILITY_MAX_STALENESS_S,
    ):
        """A cache that tracks the availability of nodes and throw away
        entries which have grown too stale.

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
                unavailable_node_information=info,
            )

        expiration_time = timestamp + self.ttl

        # TODO (Alex): In theory it would be nice to make this dictionary
        # ordered by expiration time, unfortunately that's a bit difficult
        # since `update_node_availability` can be called with out of order
        # timestamps.
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
          node_launch_exception: Details about why the node launch failed. If
            empty, the node type will be considered available."""
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
        """Remove any expired entries from the cache."""
        cur_time = self.timer()
        with self.lock:
            for key, (expiration_time, _) in list(self.store.items()):
                if expiration_time < cur_time:
                    del self.store[key]
