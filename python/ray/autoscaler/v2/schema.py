import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

# TODO(rickyx): once we have graceful shutdown, we could populate
# the failure detail with the actual termination message. As of now,
# we will use a more generic message to include cases such as:
# (idle termination, node death, crash, preemption, etc)
NODE_DEATH_CAUSE_RAYLET_DIED = "NodeTerminated"


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
    # ray node id in hex. None if still pending.
    node_id: Optional[str] = None
    # Resource usage breakdown if node is running.
    resource_usage: Optional[NodeUsage] = None
    # Failure detail if the node failed.
    failure_detail: Optional[str] = None
    # Descriptive details.
    details: Optional[str] = None

    def total_resources(self) -> Dict[str, float]:
        if self.resource_usage is None:
            return {}
        return {r.resource_name: r.total for r in self.resource_usage.usage}

    def available_resources(self) -> Dict[str, float]:
        if self.resource_usage is None:
            return {}
        return {r.resource_name: r.total - r.used for r in self.resource_usage.usage}

    def used_resources(self) -> Dict[str, float]:
        if self.resource_usage is None:
            return {}
        return {r.resource_name: r.used for r in self.resource_usage.usage}


@dataclass
class LaunchRequest:
    class Status(Enum):
        FAILED = "FAILED"
        PENDING = "PENDING"

    # The instance type name, e.g. p3.2xlarge
    instance_type_name: str
    # ray node type name.
    ray_node_type_name: str
    # count.
    count: int
    # State: (e.g. PENDING, FAILED)
    state: Status
    # When the launch request was made in unix timestamp in secs.
    request_ts_s: int
    # When the launch request failed unix timestamp in secs if failed.
    failed_ts_s: Optional[int] = None
    # Request details, e.g. error reason if the launch request failed.
    details: Optional[str] = None


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
    placement_group_demand: List[PlacementGroupResourceDemand] = field(
        default_factory=list
    )
    # Ray task actor demand.
    ray_task_actor_demand: List[RayTaskActorDemand] = field(default_factory=list)
    # Cluster constraint demand.
    cluster_constraint_demand: List[ClusterConstraintDemand] = field(
        default_factory=list
    )


@dataclass
class Stats:
    # How long it took to get the GCS request.
    # This is required when initializing the Stats since it should be calculated before
    # the request was made.
    gcs_request_time_s: float
    # How long it took to get all live instances from node provider.
    none_terminated_node_request_time_s: Optional[float] = None
    # How long for autoscaler to process the scaling decision.
    autoscaler_iteration_time_s: Optional[float] = None
    # The last seen autoscaler state version from Ray.
    autoscaler_version: Optional[str] = None
    # The last seen cluster state resource version.
    cluster_resource_state_version: Optional[str] = None
    # Request made time unix timestamp: when the data was pulled from GCS.
    request_ts_s: Optional[int] = None


@dataclass
class ClusterStatus:
    # Healthy nodes information (alive)
    healthy_nodes: List[NodeInfo] = field(default_factory=list)
    # Pending launches.
    pending_launches: List[LaunchRequest] = field(default_factory=list)
    # Failed launches.
    failed_launches: List[LaunchRequest] = field(default_factory=list)
    # Pending nodes.
    pending_nodes: List[NodeInfo] = field(default_factory=list)
    # Failures
    failed_nodes: List[NodeInfo] = field(default_factory=list)
    # Resource usage summary for entire cluster.
    cluster_resource_usage: List[ResourceUsage] = field(default_factory=list)
    # Demand summary.
    resource_demands: ResourceDemandSummary = field(
        default_factory=ResourceDemandSummary
    )
    # Query metics
    stats: Stats = field(default_factory=Stats)

    def total_resources(self) -> Dict[str, float]:
        return {r.resource_name: r.total for r in self.cluster_resource_usage}

    def available_resources(self) -> Dict[str, float]:
        return {r.resource_name: r.total - r.used for r in self.cluster_resource_usage}

    # TODO(rickyx): we don't show infeasible requests as of now.
    # (They will just be pending forever as part of the demands)
    # We should show them properly in the future.
