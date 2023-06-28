
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from ray.core.generated.experimental.autoscaler_pb2  import NodeState 


@dataclass 
class NodeInfo:
    # The instance type name, e.g. p3.2xlarge
    instance_type_name : str
    # The detailed state of the node/request.
    # E.g. waiting-for-ssh, launching, idle, running, etc.
    node_status: str
    # Ip address of the node if launched.
    ip_address: Optional[str]


@dataclass
class FailedNodeInfo(NodeInfo):
    failure_detail: str


@dataclass
class ResourceDemand:
    # Bundles in the demand.
    bundles: List[Dict[str, float]]

@dataclass
class PlacementGroupResourceDemand(ResourceDemand):
    # Placement group demand information.
    placement_group_id: str
    # Placement group strategy.
    strategy: str


@dataclass
class RayTaskActorDemand(ResourceDemand):
    # Number of tasks/actors that require this bundle shapes.
    count: int

@dataclass
class ClusterConstraintDemand(ResourceDemand):
    pass


@dataclass
class NodeUsage:
    # The node resource usage.
    usage: Dict[str, float]
    # How long the node has been idle.
    idle_time_ms: int
    # Usage details: what's making the node non-idle.
    usage_details: Any


@dataclass
class Stats:
    # How long it took to get the GCS request.
    gcs_request_time_s: float


@dataclass
class ClusterStatus:
    # Healthy nodes information (alive)
    healthy_nodes: List[NodeInfo]
    # Pending nodes.
    pending_nodes: List[NodeInfo]
    # Failures
    failed_nodes: List[FailedNodeInfo]
    # Resource usage summary for entire cluster.
    cluster_resource_usage: Dict[str, float]
    # Demand summary.
    resource_demands: List[ResourceDemand]
    # Node resource usage breakdown
    node_usages: List[NodeUsage]
    # Query metics
    stats: Stats


    def format_str(self, verbose_lvl=0):
        # This could be what the `ray status` is getting.
        return "not implemented"



