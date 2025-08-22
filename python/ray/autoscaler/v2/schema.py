import re
from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Dict, List, Optional, Tuple

from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.core.generated.autoscaler_pb2 import NodeState, NodeStatus
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
    # Ray node labels.
    labels: Optional[Dict[str, str]] = None

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


IPPRSpecsSchema = {
    # JSON schema for IPPR (In-Place Pod Resize) specs provided via the
    # Kubernetes annotation `ray.io/ippr` on a RayCluster CR.
    #
    # Structure:
    # {
    #   "groups": {
    #     "<groupName>": {
    #       "max-cpu":     string|number,  # K8s quantity (e.g. "2", "1500m")
    #       "max-memory":  string|integer, # K8s quantity (e.g. "8Gi", 2147483648)
    #       "resize-timeout": integer      # Seconds to wait for a pod resize to
    #                                       # complete before considering it timed out
    #     },
    #     ...
    #   }
    # }
    #
    # Notes:
    # - The set of valid <groupName> keys corresponds to the RayCluster
    #   `workerGroupSpecs[].groupName` plus the implicit "headgroup".
    # - The minimal CPU/memory values (min_*) are derived from the pod template
    #   requests/limits and are not part of this schema.
    "type": "object",
    "properties": {
        "groups": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "max-cpu": {"type": ["string", "number"]},
                    "max-memory": {"type": ["string", "integer"]},
                    "resize-timeout": {"type": "integer"},
                },
                "required": ["max-cpu", "max-memory", "resize-timeout"],
            },
        }
    },
}


@dataclass
class IPPRGroupSpec:
    """Per-group IPPR limits and baseline resources.

    This mirrors a single Ray group (worker group or head group). The minimal
    resources are derived from the pod template's container resources (either
    requests or limits if present), and the maximal resources and timeout are
    provided by the IPPR spec annotation validated by ``IPPRSpecsSchema``.

    Attributes:
        min_cpu: Baseline CPU in cores derived from the pod template (float, e.g., 1.5).
        max_cpu: Maximum CPU in cores allowed for in-place resize for this group.
        min_memory: Baseline memory in bytes derived from the pod template.
        max_memory: Maximum memory in bytes allowed for in-place resize for this group.
        resize_timeout: Timeout in seconds for a single resize operation before
            it is considered timed out.
    """

    min_cpu: float
    max_cpu: float
    min_memory: int
    max_memory: int
    resize_timeout: int


@dataclass
class IPPRSpecs:
    """Typed, validated IPPR specs across Ray groups.

    Attributes:
        groups: Mapping from Ray group name (e.g., worker group ``groupName`` or
            ``"headgroup"``) to its ``IPPRGroupSpec``.
    """

    groups: Dict[str, IPPRGroupSpec]


@dataclass
class IPPRStatus:
    """Represents the current and target resources for a pod under IPPR.

    This structure is the working state used by the autoscaler to decide if and
    when to apply in-place pod resizes and when to synchronize resource changes
    with the Raylet.

    Attributes:
        cloud_instance_id: Cloud instance identifier for the pod (K8s pod name).
        spec: The group-level limits and baselines for this pod.
        current_cpu: Current CPU allocation in cores from the pod status.
        current_memory: Current memory allocation in bytes from the pod status.
        desired_cpu: Target CPU allocation in cores.
        desired_memory: Target memory allocation in bytes.
        resized_at: Unix timestamp (seconds) when a resize request was issued to
            Kubernetes, or None if not pending/needed.
        resized_status: Lower-cased status from pod conditions for IPPR, e.g.
            "inprogress", "deferred", "infeasible", "error"; None indicates no
            active resize and is treated as finished.
        resized_message: Message from the pod condition describing the resize
            state or failure, if any.
        suggested_cpu: A suggested CPU target (in cores) adjusted based on node
            capacity when a resize was deferred/infeasible.
        suggested_memory: A suggested memory target (in bytes) adjusted based on
            node capacity when a resize was deferred/infeasible.
        adjusted_max_cpu: Temporary cap for CPU (cores) derived from suggestions
            or recent capacity observations. Overrides ``spec.max_cpu`` when set.
        adjusted_max_memory: Temporary cap for memory (bytes) derived from
            suggestions or recent capacity observations. Overrides
            ``spec.max_memory`` when set.
        raylet_id: Raylet node id (hex) running in this pod, used to sync Ray's
            internal resource view when K8s successfully changed pod resources.
    """

    cloud_instance_id: str
    spec: IPPRGroupSpec
    current_cpu: float
    current_memory: int
    desired_cpu: float
    desired_memory: int
    resized_at: Optional[int] = None
    resized_status: Optional[str] = None
    resized_message: Optional[str] = None
    suggested_cpu: Optional[float] = None
    suggested_memory: Optional[int] = None
    adjusted_max_cpu: Optional[float] = None
    adjusted_max_memory: Optional[int] = None
    raylet_id: Optional[str] = None

    def update(
        self,
        raylet_id: str,
        desired_cpu: Optional[float],
        desired_memory: Optional[int],
    ) -> None:
        """Update the desired resources and reset resize tracking state.

        Sets the new desired CPU/memory if provided, associates the Raylet id,
        and marks the resize state as "new" so the scheduler can identify the IPPR
        action before the next iteration.

        Args:
            raylet_id: Raylet node id (hex) for the pod.
            desired_cpu: Optional new desired CPU in cores.
            desired_memory: Optional new desired memory in bytes.
        """
        if desired_cpu is not None:
            self.desired_cpu = desired_cpu
        if desired_memory is not None:
            self.desired_memory = desired_memory
        self.raylet_id = raylet_id
        self.resized_at = None
        self.resized_status = "new"
        self.resized_message = None

    def is_ready_to_resize(self) -> bool:
        """Whether this pod should be sent an IPPR request now.

        Returns True if there is a Raylet id, the status is marked as "new",
        and the desired resources differ from the current resources.
        """
        return (
            self.raylet_id
            and self.resized_status == "new"
            and (
                self.desired_cpu != self.current_cpu
                or self.desired_memory != self.current_memory
            )
        )

    def is_in_progress(self) -> bool:
        """Whether a resize is on going or about to be issued.

        True if a resize was already issued to K8s (``resized_at`` set), or if
        the status is newly queued for resize (``is_ready_to_resize``).
        """
        return self.resized_at is not None or self.is_ready_to_resize()

    def is_finished(self) -> bool:
        """Whether there is no active resize in progress.

        We treat ``resized_status is None`` as finished. After K8s completes a
        resize and the Raylet has been synchronized, the provider clears
        ``resized_at`` and leaves ``resized_status`` as None.
        """
        return self.resized_status is None

    def need_sync_with_raylet(self) -> bool:
        """Whether the Raylet's internal resources need to be updated.

        Returns True when all of the following hold:
        - We know the ``raylet_id`` for this pod.
        - A resize was previously issued to Kubernetes (``resized_at`` set).
        - Kubernetes has finished applying the resize (``resized_status`` is None).
        - The pod's current resources equal the desired resources.

        In this case, the provider will call Raylet's gRPC to update its local
        resource instances and then clear ``resized_at`` on the pod annotation.
        """
        return (
            self.raylet_id is not None
            and self.resized_at is not None
            and self.resized_status is None
            and self.desired_cpu == self.current_cpu
            and self.desired_memory == self.current_memory
        )

    def max_cpu(self) -> float:
        """Effective maximum CPU cores allowed for this pod.

        Uses ``adjusted_max_cpu`` when present (suggested by capacity checks),
        otherwise falls back to ``spec.max_cpu``.
        """
        return self.adjusted_max_cpu or self.spec.max_cpu

    def max_memory(self) -> int:
        """Effective maximum memory bytes allowed for this pod.

        Uses ``adjusted_max_memory`` when present (suggested by capacity checks),
        otherwise falls back to ``spec.max_memory``.
        """
        return self.adjusted_max_memory or self.spec.max_memory

    def can_resize_up(self) -> bool:
        """Whether the pod can still be scaled up within allowed limits.

        Only returns True when no resize is in progress and the current
        CPU/memory are below the effective max limits.
        """
        return self.is_finished() and (
            self.current_cpu < self.max_cpu() or self.current_memory < self.max_memory()
        )

    def is_timeout(self) -> bool:
        """Whether an in-flight resize has exceeded the group's timeout.

        Returns True when a resize was issued and now current time exceeds
        ``resized_at + spec.resize_timeout``.
        """
        return (
            self.resized_at is not None
            and self.resized_at + self.spec.resize_timeout < time.time()
        )

    def is_failed(self) -> bool:
        """Whether the last resize attempt reported an error from Kubernetes."""
        return self.resized_status == "error"


@dataclass
class AutoscalerInstance:
    """
    AutoscalerInstance represents an instance that's managed by the autoscaler.
    This includes two states:
        1. the instance manager state: information of the underlying cloud instance.
        2. the ray node state, e.g. resources, ray node status.

    The two states are linked by the cloud instance id, which should be set
    when the ray node is started.
    """

    # The cloud instance id. It could be None if the instance hasn't been assigned
    # a cloud instance id, e.g. the instance is still in QUEUED or REQUESTED status.
    cloud_instance_id: Optional[str] = None

    # The ray node state status. It could be None when no ray node is running
    # or has run on the cloud instance: for example, ray is still being installed
    # or the instance manager hasn't had a cloud instance assigned (e.g. QUEUED,
    # REQUESTED).
    ray_node: Optional[NodeState] = None

    # The instance manager instance state. It would be None when the ray_node is not
    # None.
    # It could be None iff:
    #   1. There's a ray node, but the instance manager hasn't discovered the
    #   cloud instance that's running this ray process yet. This could happen since
    #   the instance manager only discovers instances periodically.
    #
    #   2. There was a ray node running on the cloud instance, which was already stopped
    #   and removed from the instance manager state. But the ray state is still lagging
    #   behind.
    #
    #   3. There is a ray node that's unmanaged by the instance manager.
    #
    im_instance: Optional[Instance] = None

    # | cloud_instance_id | ray_node | im_instance |
    # |-------------------|----------|-------------|
    # | None              | None     | None        | Not possible.
    # | None              | None     | not None    | OK. An instance hasn't had ray running on it yet. # noqa E501
    # | None              | Not None | None        | OK. Possible if the ray node is not started by autoscaler. # noqa E501
    # | None              | Not None | not None    | Not possible - no way to link im instance with ray node. # noqa E501
    # | not None          | None     | None        | Not possible since cloud instance id is either part of im state or ray node. # noqa E501
    # | not None          | None     | not None    | OK. e.g. An instance that's not running ray yet. # noqa E501
    # | not None          | Not None | None        | OK. See scenario 1, 2, 3 above.
    # | not None          | Not None | not None    | OK. An instance that's running ray.
    def validate(self) -> Tuple[bool, str]:
        """Validate the autoscaler instance state.

        Returns:
            A tuple of (valid, error_msg) where:
            - valid is whether the state is valid
            - error_msg is the error message for the validation results.
        """

        state_combinations = {
            # (cloud_instance_id is None, ray_node is None, im_instance is None): (valid, error_msg) # noqa E501
            (True, True, True): (False, "Not possible"),
            (True, True, False): (True, ""),
            (True, False, True): (
                True,
                "There's a ray node w/o cloud instance id, must be started not "
                "by autoscaler",
            ),
            (True, False, False): (
                False,
                "Not possible - no way to link im instance with ray node",
            ),
            (False, True, True): (
                False,
                "Not possible since cloud instance id is either part of "
                "im state or ray node",
            ),
            (False, True, False): (True, ""),
            (False, False, True): (True, ""),
            (False, False, False): (True, ""),
        }

        valid, error_msg = state_combinations[
            (
                self.cloud_instance_id is None,
                self.ray_node is None,
                self.im_instance is None,
            )
        ]
        if not valid:
            return valid, error_msg

        if self.im_instance is not None and self.ray_node is None:
            # We don't see a ray node, but tracking an im instance.
            if self.cloud_instance_id is None:
                if InstanceUtil.is_cloud_instance_allocated(self.im_instance.status):
                    return (
                        False,
                        "instance should be in a status where cloud instance "
                        "is not allocated.",
                    )
            else:
                if not InstanceUtil.is_cloud_instance_allocated(
                    self.im_instance.status
                ):
                    return (
                        False,
                        "instance should be in a status where cloud instance is "
                        "allocated.",
                    )

        if self.ray_node is not None:
            if self.cloud_instance_id != self.ray_node.instance_id:
                return False, "cloud instance id doesn't match."

        if self.im_instance is not None and self.cloud_instance_id is not None:
            if self.cloud_instance_id != self.im_instance.cloud_instance_id:
                return False, "cloud instance id doesn't match."

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
