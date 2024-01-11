import re
from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Dict, List, Optional, Set, Tuple
from ray.autoscaler._private.autoscaler import NodeStatus
from ray.core.generated.autoscaler_pb2 import NodeState
from ray.core.generated.instance_manager_pb2 import Instance

# TODO(rickyx): once we have graceful shutdown, we could populate
# the failure detail with the actual termination message. As of now,
# we will use a more generic message to include cases such as:
# (idle termination, node death, crash, preemption, etc)
NODE_DEATH_CAUSE_RAYLET_DIED = "NodeTerminated"


# e.g., cpu_4_ondemand.
NodeType = str


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
    # Activity on the node.
    node_activity: Optional[List[str]] = None

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
    # Healthy nodes information (non-idle)
    active_nodes: List[NodeInfo] = field(default_factory=list)
    # Idle node information
    idle_nodes: List[NodeInfo] = field(default_factory=list)
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


@dataclass
class InvalidInstanceStatusError(ValueError):
    """Raised when an instance has an invalid status."""

    # The instance id.
    instance_id: str
    # The current status of the instance.
    cur_status: Instance.InstanceStatus
    # The new status to be set to.
    new_status: Instance.InstanceStatus

    def __str__(self):
        return (
            f"Instance {self.instance_id} with current status "
            f"{Instance.InstanceStatus.Name(self.cur_status)} "
            f"cannot be set to {Instance.InstanceStatus.Name(self.new_status)}"
        )


class InstanceUtil:
    """
    A helper class to group updates and operations on an Instance object defined
    in instance_manager.proto
    """

    @staticmethod
    def new_instance(
        instance_id: str,
        instance_type: str,
        request_id: str = "",
    ) -> Instance:
        instance = Instance()
        instance.version = 0  # it will be populated by the underlying storage.
        instance.instance_id = instance_id
        instance.instance_type = instance_type
        instance.launch_request_id = request_id
        instance.status = Instance.QUEUED
        InstanceUtil._record_status_transition(
            instance, Instance.QUEUED, "created from InstanceUtil"
        )
        return instance

    @staticmethod
    def is_cloud_instance_allocated(instance_status: Instance.InstanceStatus) -> bool:
        """
        Returns True if the instance is in a status where there could exist
        a cloud instance allocated by the cloud provider.
        """
        assert instance_status != Instance.UNKNOWN
        return instance_status in {
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.STOPPING,
            Instance.RAY_INSTALL_FAILED,
        }

    @staticmethod
    def is_ray_running_reachable(instance_status: Instance.InstanceStatus) -> bool:
        """
        Returns True if the instance is in a status where it may transition
        to RAY_RUNNING status.
        """
        assert instance_status != Instance.UNKNOWN
        return instance_status in [
            Instance.UNKNOWN,
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
        ]

    @staticmethod
    def is_ray_stopping_reachable(instance_status: Instance.InstanceStatus) -> bool:
        """
        Returns True if the instance is in a status where it may transition
        to RAY_STOPPING status.
        """
        return (
            InstanceUtil.is_ray_running_reachable(instance_status)
            or instance_status == Instance.RAY_RUNNING
        )

    @staticmethod
    def is_ray_stop_reachable(instance_status: Instance.InstanceStatus) -> bool:
        """
        Returns True if the instance is in a status where it may transition
        to RAY_STOPPED status.
        """
        return (
            InstanceUtil.is_ray_stopping_reachable(instance_status)
            or instance_status == Instance.RAY_STOPPING
        )

    @staticmethod
    def set_status(
        instance: Instance,
        new_instance_status: Instance.InstanceStatus,
        details: str = "",
    ):
        """Transitions the instance to the new state.

        Args:
            instance: The instance to update.
            new_instance_status: The new status to transition to.
            details: The details of the transition.

        Raises:
            InvalidInstanceStatusError if the transition is not allowed.
        """
        if (
            new_instance_status
            not in InstanceUtil.get_valid_transitions()[instance.status]
        ):
            raise InvalidInstanceStatusError(
                instance_id=instance.instance_id,
                cur_status=instance.status,
                new_status=new_instance_status,
            )
        instance.status = new_instance_status
        InstanceUtil._record_status_transition(instance, new_instance_status, details)

    @staticmethod
    def _record_status_transition(
        instance: Instance, status: Instance.InstanceStatus, details: str
    ):
        """Records the status transition.

        Args:
            instance: The instance to update.
            status: The new status to transition to.
        """
        now_ns = time.time_ns()
        instance.status_history.append(
            Instance.StatusHistory(
                instance_status=status,
                timestamp_ns=now_ns,
                details=details,
            )
        )

    @staticmethod
    def get_valid_transitions() -> Dict[
        "Instance.InstanceStatus", Set["Instance.InstanceStatus"]
    ]:
        return {
            # This is the initial status of a new instance.
            Instance.QUEUED: {
                # Cloud provider requested to launch a node for the instance.
                # This happens when the a launch request is made to the node provider.
                Instance.REQUESTED
            },
            # When in this status, a launch request to the node provider is made.
            Instance.REQUESTED: {
                # Cloud provider allocated a cloud node for the instance.
                # This happens when the cloud node first appears in the list of running
                # cloud nodes from the cloud node provider.
                Instance.ALLOCATED,
                # Cloud provider failed to allocate a cloud node for the instance, retrying
                # again while being added back to the launch queue.
                Instance.QUEUED,
                # Cloud provider fails to allocate one, no retry would happen anymore.
                Instance.ALLOCATION_FAILED,
            },
            # When in this status, the cloud node is allocated and running. This
            # happens when the cloud node is present in node provider's list of
            # running cloud nodes.
            Instance.ALLOCATED: {
                # Ray needs to be install and launch on the provisioned cloud node.
                # This happens when the cloud node is allocated, and the autoscaler
                # is responsible for installing and launching ray on the cloud node.
                # For node provider that manages the ray installation and launching,
                # this state is skipped.
                Instance.RAY_INSTALLING,
                # Ray is already installed and running on the provisioned cloud node.
                # This happens when a ray node joins the ray cluster, and the instance
                # is discovered in the set of running ray nodes from the Ray cluster.
                Instance.RAY_RUNNING,
                # Instance is requested to be stopped, e.g. instance leaked: no matching
                # Instance with the same type is found in the autoscaler's state.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # Ray process is being installed and started on the cloud node.
            # This status is skipped for node provider that manages the ray
            # installation and launching. (e.g. Ray-on-Spark)
            Instance.RAY_INSTALLING: {
                # Ray installed and launched successfully, reported by the ray cluster.
                # Similar to the Instance.ALLOCATED -> Instance.RAY_RUNNING transition,
                # where the ray process is managed by the node provider.
                Instance.RAY_RUNNING,
                # Ray installation failed. This happens when the ray process failed to
                # be installed and started on the cloud node.
                Instance.RAY_INSTALL_FAILED,
                # Wen the ray node is reported as stopped by the ray cluster.
                # This could happen that the ray process was stopped quickly after start
                # such that a ray running node  wasn't discovered and the RAY_RUNNING
                # transition was skipped.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed during the installation process.
                Instance.STOPPED,
            },
            # Ray process is installed and running on the cloud node. When in this
            # status, a ray node must be present in the ray cluster.
            Instance.RAY_RUNNING: {
                # Ray is requested to be stopped to the ray cluster,
                # e.g. idle termination.
                Instance.RAY_STOPPING,
                # Ray is already stopped, as reported by the ray cluster.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # When in this status, the ray process is requested to be stopped to the
            # ray cluster, but not yet present in the dead ray node list reported by
            # the ray cluster.
            Instance.RAY_STOPPING: {
                # Ray is stopped, and the ray node is present in the dead ray node list
                # reported by the ray cluster.
                Instance.RAY_STOPPED,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # When in this status, the ray process is stopped, and the ray node is
            # present in the dead ray node list reported by the ray cluster.
            Instance.RAY_STOPPED: {
                # Cloud node is requested to be stopped.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # When in this status, the cloud node is requested to be stopped to
            # the node provider.
            Instance.STOPPING: {
                # When a cloud node no longer appears in the list of running cloud nodes
                # from the node provider.
                Instance.STOPPED
            },
            # Whenever a cloud node disappears from the list of running cloud nodes
            # from the node provider, the instance is marked as stopped. Since
            # we guarantee 1:1 mapping of a Instance to a cloud node, this is a
            # terminal state.
            Instance.STOPPED: set(),  # Terminal state.
            # When in this status, the cloud node failed to be allocated by the
            # node provider.
            Instance.ALLOCATION_FAILED: set(),  # Terminal state.
            Instance.RAY_INSTALL_FAILED: {
                # Autoscaler requests to shutdown the instance when ray install failed.
                Instance.STOPPING,
                # Cloud node somehow failed.
                Instance.STOPPED,
            },
            # Initial state before the instance is created. Should never be used.
            Instance.UNKNOWN: set(),
        }

    @staticmethod
    def get_status_times_ns(
        instance: Instance,
        select_instance_status: Optional["Instance.InstanceStatus"] = None,
    ) -> List[int]:
        """
        Returns a list of timestamps of the instance status update.

        Args:
            instance: The instance.
            instance_status: The status to search for. If None, returns all
                status updates timestamps.

        Returns:
            The list of timestamps of the instance status updates.
        """
        ts_list = []
        for status_update in instance.status_history:
            if (
                select_instance_status
                and status_update.instance_status != select_instance_status
            ):
                continue
            ts_list.append(status_update.timestamp_ns)

        return ts_list


@dataclass
class AutoscalerInstance:
    """
    AutoscalerInstance represents an instance that's managed by the autoscaler.
    This includes two states:
        1. the instance manager state: information of the underlying cloud node.
        2. the ray node state, e.g. resources, ray node status.

    The two states are linked by the cloud instance id, which should be set
    when the ray node is started.
    """

    # The cloud instance id. It could be None if the instance hasn't been assigned
    # a cloud node id, e.g. the instance is still in QUEUED or REQUESTED status.
    cloud_instance_id: Optional[str] = None

    # The ray node state status. It could be None when no ray node is running
    # or has run on the cloud node: for example, ray is still being installed
    # or the instance manager hasn't had a cloud node assigned (e.g. QUEUED,
    # REQUESTED).
    ray_node: Optional[NodeState] = None

    # The instance manager state. It would be None when the ray_node is not None.
    # It could be None iff:
    #   1. There's a ray node, but the instance manager hasn't discovered the
    #   cloud node that's running this ray process yet. This could happen since
    #   the instance manager only discovers instances periodically.
    #
    #   2. There was a ray node running on the cloud node, which was already stopped
    #   and removed from the instance manager state. But the ray state is still lagging
    #   behind.
    #
    #   3. There is a ray node that's unmanaged by the instance manager.
    #
    im_instance: Optional[Instance] = None

    # | cloud_instance_id | ray_node | im_instance |
    # |-------------------|----------|-------------|
    # | None              | None     | None        | Not possible.
    # | None              | None     | not None    | OK. An instance hasn't had ray running on it yet.
    # | None              | Not None | None        | OK. Possible if the ray node is not started by autoscaler.
    # | None              | Not None | not None    | Not possible - no way to link im instance with ray node.
    # | not None          | None     | None        | Not possible since cloud instance id is either part of im state or ray node.
    # | not None          | None     | not None    | OK. e.g. An instance that's not running ray yet.
    # | not None          | Not None | None        | OK. See scenario 1, 2, 3 above.
    # | not None          | Not None | not None    | OK. An instance that's running ray.
    def is_valid(self) -> Tuple[bool, str]:
        # Valid combinations:
        valid_combinations = {
            # (cloud_instance_id is None, ray_node is None, im_instance is None): (valid, error_msg) # noqa E501
            (True, True, True): (False, "Not possible"),
            (True, True, False): (True, ""),
            (True, False, True): (
                True,
                "There's a ray node w/o cloud node id, must be started not by autoscaler",
            ),
            (True, False, False): (
                False,
                "Not possible - no way to link im instance with ray node",
            ),
            (False, True, True): (
                False,
                "Not possible since cloud instance id is either part of im state or ray node",
            ),
            (False, True, False): (True, ""),
            (False, False, True): (True, ""),
            (False, False, False): (True, ""),
        }

        valid, error_msg = valid_combinations[
            (
                self.cloud_instance_id is None,
                self.ray_node is None,
                self.im_instance is None,
            )
        ]
        if not valid:
            return valid, error_msg

        if self.cloud_instance_id is None:
            return (
                not InstanceUtil.is_cloud_instance_allocated(self.im_instance.status),
                "instance should be in a status where cloud node is not allocated.",
            )

        return True, ""

    def is_ray_running(self) -> bool:
        """Whether the ray node is running."""
        return self.ray_node is not None and self.ray_node.status in [
            NodeStatus.RUNNING,
            NodeStatus.IDLE,
        ]

    def is_ray_stop(self) -> bool:
        """Whether the ray node is stopped."""
        return self.ray_node is None or self.ray_node.status in [
            NodeStatus.DEAD,
        ]
