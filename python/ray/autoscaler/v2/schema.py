import re
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
    # ray node type name.
    ray_node_type_name: str
    # Cloud instance id.
    instance_id: str
    # Ip address of the node when alive.
    ip_address: str
    # The status of the node. Optional for pending nodes.
    node_status: Optional[str] = None
    # ray node id. None if still pending.
    node_id: Optional[str] = None
    # Resource usage breakdown if node is running.
    resource_usage: Optional[NodeUsage] = None
    # Failure detail if the node failed.
    failure_detail: Optional[str] = None
    # Descriptive details.
    details: Optional[str] = None


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

    def __str__(self) -> str:
        return f"[{self.count} {self.bundle}]"


@dataclass
class ResourceDemand:
    # The bundles in the demand with shape and count info.
    bundles_by_count: List[ResourceRequestByCount]


@dataclass
class PlacementGroupResourceDemand(ResourceDemand):
    # Details string (parsed into below information)
    details: str
    # Placement group's id.
    pg_id: Optional[str] = None
    # Strategy, e.g. STRICT_SPREAD
    strategy: Optional[str] = None
    # Placement group's state, e.g. PENDING
    state: Optional[str] = None

    def __post_init__(self):
        if not self.details:
            return

        # Details in the format of <pg_id>:<strategy>|<state>, parse
        # it into the above fields.
        pattern = r"^.*:.*\|.*$"
        match = re.match(pattern, self.details)
        if not match:
            return

        pg_id, details = self.details.split(":")
        strategy, state = details.split("|")
        self.pg_id = pg_id
        self.strategy = strategy
        self.state = state


@dataclass
class RayTaskActorDemand(ResourceDemand):
    pass


@dataclass
class ClusterConstraintDemand(ResourceDemand):
    pass


@dataclass
class ResourceDemandSummary:
    # Placement group demand.
    placement_group_demand: List[PlacementGroupResourceDemand]
    # Ray task actor demand.
    ray_task_actor_demand: List[RayTaskActorDemand]
    # Cluster constraint demand.
    cluster_constraint_demand: List[ClusterConstraintDemand]


@dataclass
class Stats:
    # How long it took to get the GCS request.
    gcs_request_time_s: Optional[float] = None
    # How long it took to get all live instances from node provider.
    none_terminated_node_request_time_s: Optional[float] = None
    # How long for autoscaler to process the scaling decision.
    autoscaler_iteration_time_s: Optional[float] = None
    # The last seen autoscaler state version from Ray.
    autoscaler_version: Optional[str] = None
    # The last seen cluster state resource version.
    cluster_resource_state_version: Optional[str] = None


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
    resource_demands: ResourceDemandSummary
    # Query metics
    stats: Stats
    # TODO(rickyx): Not sure if this is actually used.
    # We don't have any tests that cover this is actually
    # being produced. And I have not seen this either.
    # Node availability info.
    node_availability: Optional[NodeAvailabilitySummary]

    # TODO(rickyx): we don't show infeasible requests as of now.
    # (They will just be pending forever as part of the demands)
    # We should show them properly in the future.
