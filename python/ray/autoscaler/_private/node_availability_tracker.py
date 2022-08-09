from dataclasses import dataclass
from typing import Dict, Optional

from ray.autoscaler.node_launch_exception import NodeLaunchException


@dataclass
class UnavailableNodeInformation:
    unavailable_category: str
    unavailable_description: str
    transition_timestamp: int
    last_checked_timestamp: int
    quantity_timestamp: str


@dataclass
class NodeAvailabilityRecord:
    node_type: str
    is_available: bool
    unavailable_node_information: Optional[UnavailableNodeInformation]


@dataclass
class NodeAvailabilitySummary:
    node_availabilities: Dict[
        str, NodeAvailabilityRecord
    ]  # Mapping from node type to node availability record.


class NodeProviderAvailabilityTracker:
    def __init__(self):
        pass

    def update_node_availability(
        self, timestamp: int, node_launch_exception: NodeLaunchException
    ) -> None:
        pass

    def summary(self) -> NodeAvailabilitySummary:
        pass
