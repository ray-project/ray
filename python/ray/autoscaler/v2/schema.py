from dataclasses import dataclass
from typing import Dict, List, Optional

from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilitySummary,
)

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
    # E.g. idle, running, setting-up, etc.
    node_status: str
    # ray node id.
    node_id: str
    # ray node type name.
    ray_node_type_name: str
    # Cloud instance id.
    instance_id: str
    # Ip address of the node when alive.
    ip_address: str
    # Resource usage breakdown if node alive.
    resource_usage: Optional[NodeUsage] = None
    # Failure detail if the node failed.
    failure_detail: Optional[str] = None


@dataclass
class PendingLaunchRequest:
    # The instance type name, e.g. p3.2xlarge
    instance_type_name: str
    # ray node type name.
    ray_node_type_name: str
    # count.
    count: int


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
    gcs_request_time_s: Optional[float] = None
    # How long it took to get all live instances from node provider.
    none_terminated_node_request_time_s: Optional[float] = None
    # How long for autoscaler to process the scaling decision.
    autoscaler_iteration_time_s: Optional[float] = None


@dataclass
class ClusterStatus:
    # Healthy nodes information (alive)
    healthy_nodes: List[NodeInfo]
    # Pending launches.
    pending_launches: List[PendingLaunchRequest]
    # Pending nodes.
    pending_nodes: List[NodeInfo]
    # Failures
    failed_nodes: List[NodeInfo]
    # Resource usage summary for entire cluster.
    cluster_resource_usage: List[ResourceUsage]
    # Demand summary.
    resource_demands: List[ResourceDemand]
    # Query metics
    stats: Stats
    # TODO(rickyx): Not sure if this is actually used.
    # We don't have any tests that cover this is actually
    # being produced. And I have not seen this either.
    # Node availability info.
    node_availability: Optional[NodeAvailabilitySummary]

    def to_str(self, verbose_lvl=0):
        # This could be what the `ray status` is getting.
        return "not implemented"
