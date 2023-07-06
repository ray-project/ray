from dataclasses import dataclass
from typing import Dict, List, Optional

NODE_DEATH_CAUSE_RAYLET_DIED = "RayletUnexpectedlyDied"


@dataclass
class ResourceUsage:
    # Resource name.
    resource_name: str = ""
    # Total resource.
    total: float = 0.0
    # Resource used.
    used: float = 0.0


@dataclass
class NodeUsage:
    # The node resource usage.
    usage: List[ResourceUsage]
    # How long the node has been idle.
    idle_time_ms: int


@dataclass
class NodeInfo:

    # The instance type name, e.g. p3.2xlarge
    instance_type_name: str
    # The detailed state of the node/request.
    # E.g. idle, running, etc.
    node_status: str
    # ray node id.
    node_id: str
    # ray node type name.
    ray_node_type_name: str
    # Cloud instance id.
    instance_id: str
    # Ip address of the node when alive.
    ip_address: str
    # Resource usage breakdown.
    resource_usage: Optional[NodeUsage]
    # Failure detail if the node failed.
    failure_detail: Optional[str]


@dataclass
class PendingNode:
    # The instance type name, e.g. p3.2xlarge
    instance_type_name: str
    # ray node type name.
    ray_node_type_name: str
    # The current status of the request.
    # e.g. setting-up-ssh, launching.
    details: str
    # IP address if available.
    ip_address: Optional[str]


@dataclass
class ResourceRequestByCount:
    # Bundles in the demand.
    bundle: Dict[str, float]
    # Number of bundles with the same shape.
    count: int


@dataclass
class ResourceDemand:
    # The bundles in the demand with shape and count info.
    bundles: List[ResourceRequestByCount]


@dataclass
class PlacementGroupResourceDemand(ResourceDemand):
    # Placement group strategy.
    strategy: str


@dataclass
class RayTaskActorDemand(ResourceDemand):
    pass


@dataclass
class ClusterConstraintDemand(ResourceDemand):
    pass


@dataclass
class Stats:
    # How long it took to get the GCS request.
    gcs_request_time_s: float


@dataclass
class ClusterStatus:
    # Healthy nodes information (alive)
    healthy_nodes: List[NodeInfo]
    # Pending nodes requests.
    pending_nodes: List[PendingNode]
    # Failures
    failed_nodes: List[NodeInfo]
    # Resource usage summary for entire cluster.
    cluster_resource_usage: List[ResourceUsage]
    # Demand summary.
    resource_demands: List[ResourceDemand]
    # Query metics
    stats: Stats

    def format_str(self, verbose_lvl=0):
        # This could be what the `ray status` is getting.
        return "not implemented"
