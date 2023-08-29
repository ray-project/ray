from collections import defaultdict
import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ray.autoscaler._private.autoscaler import AutoscalerSummary
from ray.autoscaler.v2.instance_manager.node_provider import NodeProvider
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    GetClusterStatusReply,
    NodeState,
    NodeStatus,
    ResourceRequest,
)

# TODO(rickyx): once we have graceful shutdown, we could populate
# the failure detail with the actual termination message. As of now,
# we will use a more generic message to include cases such as:
# (idle termination, node death, crash, preemption, etc)
NODE_DEATH_CAUSE_RAYLET_DIED = "NodeTerminated"

DEFAULT_UPSCALING_SPEED = 1.0


@dataclass
class ResourceUsage:
    # Resource name.
    resource_name: str = ""
    # Total resource.
    total: float = 0.0
    # Resource used.
    used: float = 0.0


RUNTIME_RESOURCES = ["CPU", "GPU", "memory", "object_store_memory"]


@dataclass
class NodeUsage:
    # The node resource usage.
    usage: List[ResourceUsage]
    # How long the node has been idle.
    idle_time_ms: int

    @staticmethod
    def from_total_resources(
        total_resources: Dict[str, float],
        idle_time_ms: int = 0,
    ) -> "NodeUsage":
        """Converts a list of resource tuples to NodeUsage.

        Args:
            resource_tuples (List[Tuple[str, float, float]]): A list of resource tuples.
            Each tuple is in the format of (resource_name, total, used).

        """

        return NodeUsage(
            usage=[
                ResourceUsage(resource_name=k, total=v, used=0)
                for k, v in total_resources.items()
            ],
            idle_time_ms=idle_time_ms,
        )

    def total_resources(self) -> Dict[str, float]:
        return {u.resource_name: u.total for u in self.usage}

    def available_resources(self) -> Dict[str, float]:
        return {u.resource_name: u.total - u.used for u in self.usage}

    def total_runtime_resources(self) -> Dict[str, float]:
        return {
            u.resource_name: u.total
            for u in self.usage
            if u.resource_name in RUNTIME_RESOURCES
        }


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
    # TODO: handle draining nodes.
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

    def __post_init__(self):
        def _check_healthy_nodes():
            assert self.node_id is not None
            assert self.resource_usage is not None

        if self.node_status and self.node_status in [
            NodeStatus.Name(NodeStatus.RUNNING),
            NodeStatus.Name(NodeStatus.IDLE),
        ]:
            _check_healthy_nodes()


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

    @staticmethod
    def from_resource_requests(
        resource_requests: List[Dict[str, float]]
    ) -> List["ResourceRequestByCount"]:
        resource_requests_by_count = defaultdict(int)
        for request in resource_requests:
            bundle = frozenset(request.items())
            resource_requests_by_count[bundle] += 1

        return [
            ResourceRequestByCount(dict(bundle), count)
            for bundle, count in resource_requests_by_count.items()
        ]


# TODO(rickyx): we should integrate affinity constraints into the resource demand
# information as well.
# TODO(rickyx): We should also probably just flatten this w/o the count info since
# we are most likely using the lists rather than the by count info.
@dataclass
class ResourceDemand:
    # The bundles in the demand with shape and count info.
    bundles_by_count: List[ResourceRequestByCount]

    def as_resource_dicts(self) -> List[Dict[str, float]]:
        resource_dicts = []
        for bundle_by_count in self.bundles_by_count:
            resource_dicts.extend([bundle_by_count.bundle] * bundle_by_count.count)
        return resource_dicts


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

    def with_task_actor_demand(self, bundles: List[Dict[str, float]]) -> None:
        self.ray_task_actor_demand.append(
            RayTaskActorDemand(
                bundles_by_count=ResourceRequestByCount.from_resource_requests(bundles)
            )
        )

    def get_task_actor_demand(self) -> List[Dict[str, float]]:
        demands = []
        for demand in self.ray_task_actor_demand:
            demands.extend(demand.as_resource_dicts())
        return demands


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

    # TODO(rickyx): we don't show infeasible requests as of now.
    # (They will just be pending forever as part of the demands)
    # We should show them properly in the future.


class IAutoscaler(metaclass=ABCMeta):
    @abstractmethod
    def update(self) -> None:
        pass

    @abstractmethod
    def get_provider(self) -> NodeProvider:
        pass

    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _summary(self) -> AutoscalerSummary:
        """
        TODO: this is just for v1 compatibility
        """
        pass

    @abstractmethod
    def _kill_workers(self) -> None:
        pass
