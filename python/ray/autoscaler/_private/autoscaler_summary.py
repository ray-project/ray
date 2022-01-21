from dataclasses import dataclass
from typing import Dict, List, Tuple
from ray.autoscaler._private.resource_demand_scheduler import NodeType, NodeIP

# Status of a node e.g. "up-to-date", see ray/autoscaler/tags.py
NodeStatus = str


@dataclass
class AutoscalerSummary:
    active_nodes: Dict[NodeType, int]
    pending_nodes: List[Tuple[NodeIP, NodeType, NodeStatus]]
    pending_launches: Dict[NodeType, int]
    failed_nodes: List[Tuple[NodeIP, NodeType]]
