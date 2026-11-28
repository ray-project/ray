import copy
import logging
import time
import uuid
import warnings
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from functools import total_ordering
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import ray
from ray._raylet import (  # type: ignore[attr-defined]
    IMPLICIT_RESOURCE_PREFIX,
    node_labels_match_selector,
)
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import (
    GANG_PG_NAME_PREFIX,
    CreatePlacementGroupRequest,
    DeploymentID,
    GangPlacementGroupRequest,
    GangReservationResult,
    ReplicaID,
)
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import (
    RAY_SERVE_COMPACTION_MAX_BACKOFF_TIME_S,
    RAY_SERVE_COMPACTION_TIMEOUT_S,
    RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES,
    RAY_SERVE_MIN_REPLICA_NODES,
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.domain_probe import DomainProbe
from ray.serve._private.scheduling_rules import (
    RAY_NODE_ID_LABEL,
    ExcludeNodesConstraint,
    MinTopologyDomainsConstraint,
    SchedulingConstraint,
    SchedulingContext,
    label_value,
    merge_selectors,
    rule_tiers,
)
from ray.serve._private.usage import ServeUsageTag
from ray.util import metrics as ray_metrics
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    LabelMatchExpressionsT,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)

# How long to wait before scanning a stable cluster for a compactable node again.
# A scan simulates a bin pack for every worker node, so don't run it every loop.
COMPACTION_SCAN_INTERVAL_S = 10.0


class SpreadDeploymentSchedulingPolicy:
    """A scheduling policy that spreads replicas with best effort."""

    pass


@total_ordering
class Resources(dict):
    """Base for per-node availability vs replica demand resource maps.

    Do not instantiate directly; use ``AvailableNodeResources`` or
    ``RequestedResources``.
    """

    # Custom resource priority from environment variable
    CUSTOM_PRIORITY: List[str] = RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES
    EPSILON = 1e-9

    def __new__(cls, *args, **kwargs):
        if cls is Resources:
            raise TypeError(
                "Resources cannot be instantiated directly; use "
                "AvailableNodeResources or RequestedResources."
            )
        return super().__new__(cls)

    def __eq__(self, other):
        keys = set(self.keys()) | set(other.keys())
        return all([self.get(k) == other.get(k) for k in keys])

    def __add__(self, other):
        keys = set(self.keys()) | set(other.keys())

        kwargs = dict()
        for key in keys:
            if key.startswith(IMPLICIT_RESOURCE_PREFIX):
                kwargs[key] = min(1.0, self.get(key) + other.get(key))
            else:
                kwargs[key] = self.get(key) + other.get(key)

        return type(self)(kwargs)

    def __sub__(self, other):
        keys = set(self.keys()) | set(other.keys())
        kwargs = {key: self.get(key) - other.get(key) for key in keys}
        return type(self)(kwargs)

    def can_fit(self, other):
        keys = set(self.keys()) | set(other.keys())
        # We add a small epsilon to avoid floating point precision issues.
        return all(
            (self.get(k) or 0) + self.EPSILON >= (other.get(k) or 0) for k in keys
        )

    def __lt__(self, other):
        """Determines priority when sorting a list of SoftResources.
        1. Custom resources defined in RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES (sorted by priority)
        2. GPU
        3. CPU
        4. memory
        5. Other custom resources
        This means a resource with a larger number of high-priority resources is always
        sorted higher than one with fewer, regardless of other types.
        """

        keys = set(self.keys()) | set(other.keys())
        custom_keys = keys - {"GPU", "CPU", "memory"}

        for key in self.CUSTOM_PRIORITY:
            if self.get(key) < other.get(key):
                return True
            elif self.get(key) > other.get(key):
                return False

        if self.get("GPU") < other.get("GPU"):
            return True
        elif self.get("GPU") > other.get("GPU"):
            return False

        if self.get("CPU") < other.get("CPU"):
            return True
        elif self.get("CPU") > other.get("CPU"):
            return False

        if self.get("memory") < other.get("memory"):
            return True
        elif self.get("memory") > other.get("memory"):
            return False

        for key in custom_keys - set(self.CUSTOM_PRIORITY):
            if self.get(key) < other.get(key):
                return True
            elif self.get(key) > other.get(key):
                return False

        return False


def _format_resources_for_scheduling_log(resources: Resources) -> str:
    """Compact resource summary for pack scheduling logs."""
    priority_keys = list(Resources.CUSTOM_PRIORITY) + ["GPU", "CPU", "memory"]
    seen = set()
    parts = []
    for key in priority_keys:
        if key in seen:
            continue
        seen.add(key)
        val = resources.get(key)
        if val:
            parts.append(f"{key}={val:g}" if isinstance(val, float) else f"{key}={val}")
    for key in sorted(set(resources.keys()) - seen):
        val = resources.get(key)
        if val:
            parts.append(f"{key}={val:g}" if isinstance(val, float) else f"{key}={val}")
    return ", ".join(parts) if parts else "none"


class AvailableNodeResources(Resources):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, key: str, default: Any = None):
        val = super().get(key, default)
        if val is not None:
            return val

        # Implicit resources by default have 1 total
        # NOTE(zcin): Implicit resources are automatically and
        # artificially injected into each node and are used to limit how
        # many replicas of the same deployment can run on a single node.
        # This is used to enforce `max_replicas_per_node`.
        if key.startswith(IMPLICIT_RESOURCE_PREFIX):
            return 1

        # Otherwise by default there is 0 of this resource
        return 0


class RequestedResources(Resources):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, key: str, default: Any = None):
        # We DON'T inject implicit resources for required resources.
        val = super().get(key, default)
        if val is not None:
            return val

        return 0


class ReplicaSchedulingRequestStatus(str, Enum):
    """The status of a replica scheduling request."""

    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    ACTOR_CREATION_FAILED = "ACTOR_CREATION_FAILED"
    PLACEMENT_GROUP_CREATION_FAILED = "PLACEMENT_GROUP_CREATION_FAILED"


@dataclass
class ReplicaSchedulingRequest:
    """Request to schedule a single replica.

    The scheduler is responsible for scheduling
    based on the deployment scheduling policy.
    """

    replica_id: ReplicaID
    actor_def: ray.actor.ActorClass
    actor_resources: Dict
    actor_options: Dict
    actor_init_args: Tuple
    on_scheduled: Callable
    status: ReplicaSchedulingRequestStatus = ReplicaSchedulingRequestStatus.IN_PROGRESS
    # Placement group bundles and strategy *for this replica*.
    # These are optional: by default replicas do not have a placement group.
    placement_group_bundles: Optional[List[Dict[str, float]]] = None
    placement_group_strategy: Optional[str] = None
    placement_group_bundle_label_selector: Optional[List[Dict[str, str]]] = None
    placement_group_fallback_strategy: Optional[List[Dict[str, Any]]] = None
    max_replicas_per_node: Optional[int] = None
    # Gang scheduling fields -- if set, replica should be scheduled on
    # the reserved gang placement group at the specified bundle index.
    gang_placement_group: Optional[PlacementGroup] = None
    # Bundle index inside gang_placement_group where this replica actor is scheduled.
    # Example: If each replica uses 2 bundles, ranks 0 and 1 use indices 0 and 2 respectively.
    gang_pg_index: Optional[int] = None
    # If set, schedule this replica onto this node with hard node affinity. The
    # ingress request router sets it to co-locate a replica with each proxy.
    target_node_id: Optional[str] = None

    @property
    def requested_resources(self) -> RequestedResources:
        """The resources required to schedule this replica on a node.

        STRICT_PACK placement group: sum of all bundles.
        Other placement groups: bundle 0.
        Otherwise: actor resources.
        """

        if (
            self.placement_group_bundles is not None
            and self.placement_group_strategy == "STRICT_PACK"
        ):
            return sum(
                [RequestedResources(bundle) for bundle in self.placement_group_bundles],
                RequestedResources(),
            )
        elif self.placement_group_bundles is not None:
            return RequestedResources(self.placement_group_bundles[0])
        else:
            required = RequestedResources(self.actor_resources)

            # Using implicit resource (resources that every node
            # implicitly has and total is 1)
            # to limit the number of replicas on a single node.
            if (
                self.max_replicas_per_node is not None
                and self.max_replicas_per_node > 0
            ):
                deployment_id = self.replica_id.deployment_id
                implicit_resource = (
                    f"{IMPLICIT_RESOURCE_PREFIX}"
                    f"{deployment_id.app_name}:{deployment_id.name}"
                )
                required[implicit_resource] = 1.0 / self.max_replicas_per_node

            return required


@dataclass
class DeploymentDownscaleRequest:
    """Request to stop a certain number of replicas.

    The scheduler is responsible for choosing the replicas to stop.
    """

    deployment_id: DeploymentID
    num_to_stop: int

    # The scheduler uses these to select complete gangs to stop.
    gang_id_by_replica: Optional[Dict[ReplicaID, str]] = None
    replicas_by_gang_id: Optional[Dict[str, Set[ReplicaID]]] = None
    gang_size: Optional[int] = None


@dataclass
class DeploymentSchedulingInfo:
    deployment_id: DeploymentID
    scheduling_policy: Any
    actor_resources: Optional[RequestedResources] = None
    label_selector: Optional[Dict[str, str]] = None
    placement_group_bundles: Optional[List[RequestedResources]] = None
    bundle_label_selector: Optional[List[Dict[str, str]]] = None
    fallback_strategy: Optional[List[Dict[str, Any]]] = None
    placement_group_strategy: Optional[str] = None
    max_replicas_per_node: Optional[int] = None
    is_gang: bool = False
    # Replicas are pinned to specific nodes, so they can't be moved.
    pins_replicas: bool = False
    rules: Tuple[SchedulingConstraint, ...] = ()

    @property
    def required_resources(self) -> RequestedResources:
        """The resources required to schedule a replica of this deployment on a node.

        STRICT_PACK placement group: sum of all bundles.
        Other placement groups: bundle 0.
        Otherwise: actor resources.
        """

        if (
            self.placement_group_bundles is not None
            and self.placement_group_strategy == "STRICT_PACK"
        ):
            return sum(self.placement_group_bundles, RequestedResources())
        elif self.placement_group_bundles is not None:
            return RequestedResources(self.placement_group_bundles[0])
        else:
            if self.actor_resources is None:
                required = RequestedResources()
            else:
                required = RequestedResources(self.actor_resources)

            # Using implicit resource (resources that every node
            # implicitly has and total is 1)
            # to limit the number of replicas on a single node.
            if (
                self.max_replicas_per_node is not None
                and self.max_replicas_per_node > 0
            ):
                implicit_resource = (
                    f"{IMPLICIT_RESOURCE_PREFIX}"
                    f"{self.deployment_id.app_name}:{self.deployment_id.name}"
                )
                required[implicit_resource] = 1.0 / self.max_replicas_per_node

            return required

    def is_non_strict_pack_pg(self) -> bool:
        return (
            self.placement_group_bundles is not None
            and self.placement_group_strategy != "STRICT_PACK"
        )


@dataclass
class LaunchingReplicaInfo:
    """Describes a replica for which a schedule request has been sent to
    core but has not been scheduled (placed on a node) yet.

    Args:
        target_node_id: The exact node that's been requested for this
            replica. This is best effort and may not be fulfilled.
        target_labels: The node labels that have been requested for this
            replica. This is best effort and may not be fulfilled.
    """

    target_node_id: Optional[str] = None
    target_labels: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class RebalanceMove:
    """A planned move of a running replica to a node where it holds more rules."""

    replica_id: ReplicaID
    source_node_id: str
    target_node_id: str
    reason: str


@dataclass
class MoveInfo:
    """A rebalance move in progress."""

    target_node_id: str
    start_timestamp_s: float
    replacement_id: Optional[ReplicaID] = None


@dataclass
class CompactingNodeInfo:
    target_node_id: str
    start_timestamp_s: float
    cached_running_replicas_on_target_node: Set[ReplicaID]
    last_logged_timeout_warning: Optional[float] = None
    # Set once every movable replica has left the node. The compaction is only
    # complete when the node itself leaves the active set, since until then it
    # could still take new replicas.
    migration_done_timestamp_s: Optional[float] = None


def _flatten(
    deployment_to_replicas: Dict[DeploymentID, Dict[ReplicaID, Any]],
) -> Dict[ReplicaID, Any]:
    """Flattens a dict of {deployment_id: {replica_id: val}} to {replica_id: val}."""

    return {
        replica_id: val
        for replicas in deployment_to_replicas.values()
        for replica_id, val in replicas.items()
    }


GiB = 2**30


def _compaction_node_size(total_resources: Dict[str, float]) -> AvailableNodeResources:
    """Reduce a node's total resources to what tells node sizes apart.

    Every node carries its own `node:<ip>` label, and `memory` and
    `object_store_memory` are sized from the memory free when the raylet started,
    so two nodes of the same type never compare equal on raw totals. Drop the
    label and the object store, and round memory to whole GiB, so same sized
    nodes tie and the fewest migrations tie break gets a say.
    """
    size = {
        k: v
        for k, v in total_resources.items()
        if not k.startswith("node:") and k != "object_store_memory"
    }
    if "memory" in size:
        size["memory"] = round(size["memory"] / GiB) * GiB
    return AvailableNodeResources(size)


def _best_fit_node(
    required_resources: RequestedResources,
    available_resources: Dict[str, AvailableNodeResources],
    tie_break_key: Optional[Callable[[str], Any]] = None,
) -> Optional[str]:
    """Chooses a node using best fit strategy.

    This strategy picks the node where, if the required resources
    were to be scheduled on that node, it will leave the smallest
    remaining space. This minimizes fragmentation of resources.

    If multiple nodes tie on remaining space and `tie_break_key` is
    provided, the node with the smallest `tie_break_key` value wins.
    Remaining ties go to the lowest node id.
    """

    min_key = None
    chosen_node = None

    for node_id in available_resources:
        if not available_resources[node_id].can_fit(required_resources):
            continue

        # TODO(zcin): We can make this better by only considering
        # custom resources that required_resources has.
        remaining_space = available_resources[node_id] - required_resources
        # Tuple compares remaining space first, then the tie break key only on a tie.
        current_key = (
            (remaining_space, tie_break_key(node_id), node_id)
            if tie_break_key
            else (remaining_space, node_id)
        )

        if min_key is None or current_key < min_key:
            min_key = current_key
            chosen_node = node_id

    return chosen_node


@dataclass
class ClusterView:
    """Cluster state shared by every replica placed in one update."""

    node_labels: Dict[str, Dict[str, str]]
    available_resources: Dict[str, AvailableNodeResources]
    node_to_assigned_replicas: Dict[str, Set[ReplicaID]]
    total_resources: Dict[str, AvailableNodeResources] = field(default_factory=dict)
    replicas_per_node: DefaultDict[DeploymentID, "Counter[str]"] = field(
        default_factory=lambda: defaultdict(Counter)
    )

    def take(
        self, node_id: str, resources: RequestedResources, replica_id: ReplicaID
    ) -> None:
        if node_id in self.available_resources:
            self.available_resources[node_id] = (
                self.available_resources[node_id] - resources
            )
        self.node_to_assigned_replicas.setdefault(node_id, set()).add(replica_id)
        self.replicas_per_node[replica_id.deployment_id][node_id] += 1


class NodeScorer(ABC):
    """Picks one node for a replica among the filtered candidates."""

    default_scheduling_strategy: str = "DEFAULT"
    picks_nodes: bool = True

    @abstractmethod
    def choose(
        self,
        required_resources: RequestedResources,
        candidates: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        ctx: SchedulingContext,
    ) -> Optional[str]:
        """Returns the chosen node, or None if no candidate fits."""
        raise NotImplementedError


class PackNodeScorer(NodeScorer):
    """Best fit, preferring nodes that already run replicas."""

    def choose(
        self,
        required_resources: RequestedResources,
        candidates: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        ctx: SchedulingContext,
    ) -> Optional[str]:
        non_idle_nodes: Dict[str, AvailableNodeResources] = {}
        idle_nodes: Dict[str, AvailableNodeResources] = {}
        for node_id, resources in candidates.items():
            if node_to_assigned_replicas.get(node_id):
                non_idle_nodes[node_id] = resources
            else:
                idle_nodes[node_id] = resources
        return _best_fit_node(required_resources, non_idle_nodes) or _best_fit_node(
            required_resources, idle_nodes
        )


class SpreadNodeScorer(NodeScorer):
    """The node with the fewest replicas of the deployment, then the most room."""

    default_scheduling_strategy = "SPREAD"
    picks_nodes = False

    def choose(
        self,
        required_resources: RequestedResources,
        candidates: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        ctx: SchedulingContext,
    ) -> Optional[str]:
        chosen_node = None
        chosen_count = 0
        chosen_room: Optional[Resources] = None
        for node_id in sorted(candidates):
            available = candidates[node_id]
            if not available.can_fit(required_resources):
                continue
            count = ctx.replicas_per_node.get(node_id, 0)
            room = available - required_resources
            if (
                chosen_room is None
                or count < chosen_count
                or (count == chosen_count and room > chosen_room)
            ):
                chosen_node, chosen_count, chosen_room = node_id, count, room
        return chosen_node


def _topology_rules(
    topology_spread: Optional[List[Dict[str, Any]]],
) -> Tuple[SchedulingConstraint, ...]:
    """The rules for a `topology_spread` option. A floor of 1 adds no rule."""
    return tuple(
        MinTopologyDomainsConstraint(
            rule["label_key"],
            rule["min_domains"],
            required=rule.get("required", False),
            wait_timeout_s=rule.get("wait_timeout_s"),
        )
        for rule in topology_spread or []
        if rule["min_domains"] > 1
    )


class DeploymentScheduler:
    """A centralized scheduler for all Serve deployments.

    It makes a batch of scheduling decisions in each update cycle.
    """

    def __init__(
        self,
        cluster_node_info_cache: ClusterNodeInfoCache,
        head_node_id: str,
        create_placement_group_fn: Callable,
        create_domain_probe_fn: Optional[Callable] = None,
    ):
        # {deployment_id: scheduling_policy}
        self._deployments: Dict[DeploymentID, DeploymentSchedulingInfo] = {}
        # Replicas that are waiting to be scheduled.
        # {deployment_id: {replica_id: deployment_upscale_request}}
        self._pending_replicas: Dict[
            DeploymentID, Dict[ReplicaID, ReplicaSchedulingRequest]
        ] = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors have been submitted.
        # {deployment_id: {replica_id: target_node_id}}
        self._launching_replicas: Dict[
            DeploymentID, Dict[ReplicaID, LaunchingReplicaInfo]
        ] = defaultdict(dict)
        # Replicas that are recovering.
        # We don't know where those replicas are running.
        # {deployment_id: {replica_id}}
        self._recovering_replicas: DefaultDict[
            DeploymentID, Set[ReplicaID]
        ] = defaultdict(set)
        # Replicas that are running.
        # We know where those replicas are running.
        # {deployment_id: {replica_id: running_node_id}}
        self._running_replicas: DefaultDict[
            DeploymentID, Dict[ReplicaID, str]
        ] = defaultdict(dict)
        self._outdated_replicas: DefaultDict[
            DeploymentID, Set[ReplicaID]
        ] = defaultdict(set)
        # Dedupes pack scheduling order logs across control loops.
        self._last_pack_schedule_order_log_key: Optional[tuple] = None
        # Dedupes repeated pack placement failure logs for stuck replicas.
        self._logged_pack_placement_failures: Set[ReplicaID] = set()
        self._held_replicas: DefaultDict[DeploymentID, Set[ReplicaID]] = defaultdict(
            set
        )
        self._held_since: Dict[ReplicaID, float] = {}

        self._cluster_node_info_cache = cluster_node_info_cache
        self._head_node_id = head_node_id
        self._create_placement_group_fn = create_placement_group_fn
        self._create_domain_probe_fn = create_domain_probe_fn or DomainProbe
        self._probes: Dict[DeploymentID, Any] = {}
        self._unmet_rules: Dict[DeploymentID, List[str]] = {}
        self._outdated_to_stop: Dict[DeploymentID, Set[ReplicaID]] = {}
        self._stopped_for_held: Dict[ReplicaID, ReplicaID] = {}
        self._moves: Dict[ReplicaID, MoveInfo] = {}
        self._num_failed_moves: DefaultDict[DeploymentID, int] = defaultdict(int)
        self._next_move_timestamp_s: Dict[DeploymentID, float] = {}

        # Not checkpointed: a restarted controller reconciles first, then
        # re-detects compaction opportunities.
        self._compacting_node: Optional[CompactingNodeInfo] = None
        self._compaction_rule = ExcludeNodesConstraint(self._compacting_node_ids)
        self._compaction_scorer = PackNodeScorer()
        # Set after a scan finds nothing, cleared as soon as the cluster changes.
        self._next_compaction_scan_timestamp_s: float = 0
        self._num_consecutive_failed_compactions: int = 0
        self._next_allowed_compaction_timestamp_s: float = 0
        self._num_succeeded_compactions: int = 0
        self._num_compacted_nodes_counter = ray_metrics.Counter(
            "serve_num_compacted_nodes",
            description="The number of nodes that have been compacted.",
        )
        rule_tag_keys = ("deployment", "application", "rule")
        self._covered_domains_gauge = ray_metrics.Gauge(
            "serve_topology_spread_covered_domains",
            description=(
                "The domains a deployment's replicas cover, for each "
                "topology_spread rule."
            ),
            tag_keys=rule_tag_keys,
        )
        self._floor_domains_gauge = ray_metrics.Gauge(
            "serve_topology_spread_floor_domains",
            description=(
                "The domains each topology_spread rule asks for, capped at the "
                "replica count and, for a best effort rule, at the domains "
                "there are."
            ),
            tag_keys=rule_tag_keys,
        )

    def on_deployment_created(
        self,
        deployment_id: DeploymentID,
        scheduling_policy: SpreadDeploymentSchedulingPolicy,
    ) -> None:
        """Called whenever a new deployment is created."""
        assert deployment_id not in self._pending_replicas
        assert deployment_id not in self._launching_replicas
        assert deployment_id not in self._recovering_replicas
        assert deployment_id not in self._running_replicas
        self._deployments[deployment_id] = DeploymentSchedulingInfo(
            deployment_id=deployment_id, scheduling_policy=scheduling_policy
        )

    def on_deployment_deployed(
        self,
        deployment_id: DeploymentID,
        replica_config: ReplicaConfig,
        is_gang: bool = False,
        pins_replicas: bool = False,
    ) -> None:
        assert deployment_id in self._deployments

        info = self._deployments[deployment_id]
        info.is_gang = is_gang
        info.pins_replicas = pins_replicas
        info.actor_resources = RequestedResources(replica_config.resource_dict)
        info.label_selector = replica_config.ray_actor_options.get("label_selector")
        info.bundle_label_selector = (
            replica_config.placement_group_bundle_label_selector
        )
        info.fallback_strategy = replica_config.ray_actor_options.get(
            "fallback_strategy"
        )
        info.max_replicas_per_node = replica_config.max_replicas_per_node
        if replica_config.placement_group_bundles:
            info.placement_group_bundles = [
                RequestedResources(bundle)
                for bundle in replica_config.placement_group_bundles
            ]
        if replica_config.placement_group_strategy:
            info.placement_group_strategy = replica_config.placement_group_strategy
        info.rules = _topology_rules(self._topology_spread_for(info, replica_config))

    @staticmethod
    def _topology_spread_for(
        info: DeploymentSchedulingInfo, replica_config: ReplicaConfig
    ) -> Optional[List[Dict[str, Any]]]:
        """The deployment's `topology_spread`, or the default node floor."""
        if info.is_gang:
            return None
        topology_spread = getattr(replica_config, "topology_spread", None)
        if topology_spread is not None:
            return topology_spread
        if info.pins_replicas:
            return None
        return [
            {"label_key": RAY_NODE_ID_LABEL, "min_domains": RAY_SERVE_MIN_REPLICA_NODES}
        ]

    def on_deployment_deleted(self, deployment_id: DeploymentID) -> None:
        """Called whenever a deployment is deleted."""
        assert not self._pending_replicas[deployment_id]
        self._pending_replicas.pop(deployment_id, None)

        assert not self._launching_replicas[deployment_id]
        self._launching_replicas.pop(deployment_id, None)

        assert not self._recovering_replicas[deployment_id]
        self._recovering_replicas.pop(deployment_id, None)

        assert not self._running_replicas[deployment_id]
        self._running_replicas.pop(deployment_id, None)

        self._outdated_replicas.pop(deployment_id, None)
        self._num_failed_moves.pop(deployment_id, None)
        self._next_move_timestamp_s.pop(deployment_id, None)
        self._held_replicas.pop(deployment_id, None)
        for held_id in list(self._stopped_for_held):
            if held_id.deployment_id == deployment_id:
                del self._stopped_for_held[held_id]
        self._unmet_rules.pop(deployment_id, None)
        self._stop_probe(deployment_id)

        del self._deployments[deployment_id]

    def on_replica_stopping(self, replica_id: ReplicaID) -> None:
        """Called whenever a deployment replica is being stopped."""
        deployment_id = replica_id.deployment_id
        self._pending_replicas[deployment_id].pop(replica_id, None)
        self._launching_replicas[deployment_id].pop(replica_id, None)
        self._recovering_replicas[deployment_id].discard(replica_id)
        self._running_replicas[deployment_id].pop(replica_id, None)
        self._outdated_replicas[deployment_id].discard(replica_id)
        self._held_replicas[deployment_id].discard(replica_id)
        self._held_since.pop(replica_id, None)
        self._stopped_for_held.pop(replica_id, None)
        self._on_move_replica_stopping(replica_id)
        self._logged_pack_placement_failures.discard(replica_id)

    def on_replicas_outdated(
        self, deployment_id: DeploymentID, replica_ids: Set[ReplicaID]
    ) -> None:
        """Sets the replicas a rollout will stop. They don't count toward floors."""
        self._outdated_replicas[deployment_id] = set(replica_ids)

    def on_replica_running(self, replica_id: ReplicaID, node_id: str) -> None:
        """Called whenever a deployment replica is running with a known node id."""
        deployment_id = replica_id.deployment_id
        assert replica_id not in self._pending_replicas[deployment_id]

        self._launching_replicas[deployment_id].pop(replica_id, None)
        self._recovering_replicas[deployment_id].discard(replica_id)

        self._running_replicas[deployment_id][replica_id] = node_id

    def on_replica_recovering(self, replica_id: ReplicaID) -> None:
        """Called whenever a deployment replica is recovering."""
        deployment_id = replica_id.deployment_id
        assert replica_id not in self._pending_replicas[deployment_id]
        assert replica_id not in self._launching_replicas[deployment_id]
        assert replica_id not in self._running_replicas[deployment_id]
        assert replica_id not in self._recovering_replicas[deployment_id]

        self._recovering_replicas[deployment_id].add(replica_id)

    def _on_replica_launching(
        self,
        replica_id: ReplicaID,
        target_node_id: Optional[str] = None,
        target_labels: Optional[Dict[str, Any]] = None,
    ):
        deployment_id = replica_id.deployment_id
        self._held_since.pop(replica_id, None)
        self._stopped_for_held.pop(replica_id, None)
        self._launching_replicas[deployment_id][replica_id] = LaunchingReplicaInfo(
            target_node_id=target_node_id, target_labels=target_labels
        )

    def _get_node_to_running_replicas(
        self, deployment_id: Optional[DeploymentID] = None
    ) -> Dict[str, Set[ReplicaID]]:
        res = defaultdict(set)
        if deployment_id:
            for replica_id, node_id in self._running_replicas[deployment_id].items():
                res[node_id].add(replica_id)
        else:
            for _, replicas in self._running_replicas.items():
                for replica_id, node_id in replicas.items():
                    res[node_id].add(replica_id)

        return res

    def _get_available_resources_per_node(self) -> Dict[str, AvailableNodeResources]:
        """Gets current available resources per node.

        This returns a conservative view of the available resources
        currently in the cluster. It returns the minimum of:

        1. The available resources per node fetched and cached from the
           GCS every control loop.
        2. The remaining resources left over on each node after
           subtracting the resources taken up by running (already
           scheduled by core) and launching (to-be-scheduled and soft
           targeting that node) replicas.

        Note that (1) may not be accurate because it uses cached info
        and there is a delay from fetching data from GCS, and (2) may
        not be accurate because there can be other actors (not replicas)
        running in the cluster, and launching replicas may not end up on
        the node we're targeting. So the information returned from this
        method is only best effort.
        """

        available_resources = (
            self._cluster_node_info_cache.get_available_resources_per_node()
        )
        total_resources = self._cluster_node_info_cache.get_total_resources_per_node()

        gcs_info = {
            node_id: AvailableNodeResources(r)
            for node_id, r in available_resources.items()
        }

        # Manually calculate available resources per node by subtracting
        # launching and running replicas from total resources
        total_minus_replicas = {
            node_id: AvailableNodeResources(resources)
            for node_id, resources in total_resources.items()
        }
        for deployment_id, replicas in self._launching_replicas.items():
            deployment = self._deployments[deployment_id]
            for info in replicas.values():
                target_node_id = info.target_node_id
                if not target_node_id or target_node_id not in total_minus_replicas:
                    continue

                total_minus_replicas[target_node_id] -= deployment.required_resources

        for deployment_id, replica_nodes in self._running_replicas.items():
            deployment = self._deployments[deployment_id]
            for node_id in replica_nodes.values():
                if node_id not in total_minus_replicas:
                    continue

                total_minus_replicas[node_id] -= deployment.required_resources

        def custom_min(a: AvailableNodeResources, b: AvailableNodeResources):
            keys = set(a.keys()) | set(b.keys())
            res = AvailableNodeResources()
            for key in keys:
                res[key] = min(a.get(key), b.get(key))
            return res

        # Filter by active node ids (alive but not draining)
        return {
            node_id: custom_min(
                gcs_info.get(node_id, AvailableNodeResources()),
                total_minus_replicas.get(node_id, AvailableNodeResources()),
            )
            for node_id in self._cluster_node_info_cache.get_active_node_ids()
        }

    def _schedule_replica(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        default_scheduling_strategy: str,
        target_node_id: Optional[str] = None,
        target_labels: Optional[LabelMatchExpressionsT] = None,
        label_selector_chain: Optional[List[Dict[str, str]]] = None,
    ) -> bool:
        """Schedule a replica from a scheduling request.

        The following special scheduling strategies will be used, in
        order of highest to lowest priority.
        1. If a replica requires gang scheduling, we will use a reserved
           gang placement group.
        2. If a replica requires placement groups, we will choose to use
           a `PlacementGroupSchedulingStrategy`. This can also take a
           target node into consideration (soft target), if provided.
           However it cannot take into account target labels.
        3. If a `target_node_id` is provided, we will choose to use a
           `NodeAffinitySchedulingStrategy`.
        4. If `target_labels` is provided, we will choose to use a
           `NodeLabelSchedulingStrategy`.

        Args:
            scheduling_request: A request to schedule a replica.
            default_scheduling_strategy: The scheduling strategy to fall
                back to if no special scheduling strategy is necessary.
            target_node_id: Attempt to schedule this replica onto this
                target node.
            target_labels: Attempt to schedule this replica onto nodes
                with these target labels.
            label_selector_chain: Label selectors Ray Core tries in order.

        Returns:
            True if the replica was successfully scheduled, False otherwise.
        """

        replica_id = scheduling_request.replica_id
        deployment_id = replica_id.deployment_id
        placement_group = None
        per_replica_pg = None

        scheduling_strategy: Any = default_scheduling_strategy

        # The request may carry an explicit node. The ingress request router
        # pins each replica to a proxy node with hard affinity. It wins over any
        # node the caller passed in.
        pin_to_target_node = scheduling_request.target_node_id is not None
        if pin_to_target_node:
            target_node_id = scheduling_request.target_node_id

        if scheduling_request.gang_placement_group is not None:
            # Gang scheduling -- use the reserved gang placement group
            placement_group = scheduling_request.gang_placement_group
            assert scheduling_request.gang_pg_index is not None
            scheduling_strategy = PlacementGroupSchedulingStrategy(
                placement_group=placement_group,
                placement_group_bundle_index=scheduling_request.gang_pg_index,
                placement_group_capture_child_tasks=True,
            )
            # TODO (jeffreywang): Add support for target labels and node affinity
            target_labels = None
            target_node_id = None
        elif scheduling_request.placement_group_bundles is not None:
            placement_group_strategy = (
                scheduling_request.placement_group_strategy
                if scheduling_request.placement_group_strategy
                else "PACK"
            )
            bundle_label_selector = (
                scheduling_request.placement_group_bundle_label_selector
            )
            fallback_strategy = None
            if label_selector_chain:
                rest = list(
                    (
                        bundle_label_selector
                        or [{}] * len(scheduling_request.placement_group_bundles)
                    )[1:]
                )
                bundle_label_selector = [label_selector_chain[0]] + rest
                fallback_strategy = [
                    {"bundle_label_selector": [selector] + rest}
                    for selector in label_selector_chain[1:]
                ] or None
            try:
                per_replica_pg = self._create_placement_group_fn(
                    CreatePlacementGroupRequest(
                        bundles=scheduling_request.placement_group_bundles,
                        strategy=placement_group_strategy,
                        target_node_id=(
                            target_node_id
                            if placement_group_strategy == "STRICT_PACK"
                            else None
                        ),
                        name=scheduling_request.actor_options["name"],
                        bundle_label_selector=bundle_label_selector,
                        fallback_strategy=fallback_strategy,
                    )
                )
            except Exception:
                # We add a defensive exception here, so the controller can
                # make progress even if the placement group isn't created.
                # See https://github.com/ray-project/ray/issues/43888.
                logger.exception(
                    f"Failed to create a placement group for {replica_id}."
                )
                scheduling_request.status = (
                    ReplicaSchedulingRequestStatus.PLACEMENT_GROUP_CREATION_FAILED
                )
                return False
            # Pin the actor as a subset of bundle 0. ReplicaConfig
            # validates that actor resources fit in bundle 0, and
            # required_resources assumes this pin.
            scheduling_strategy = PlacementGroupSchedulingStrategy(
                placement_group=per_replica_pg,
                placement_group_bundle_index=0,
                placement_group_capture_child_tasks=True,
            )
            target_labels = None
        elif target_node_id is not None:
            scheduling_strategy = NodeAffinitySchedulingStrategy(
                node_id=target_node_id,
                soft=not pin_to_target_node,
                _spill_on_unavailable=not pin_to_target_node,
            )
            target_labels = None
        elif target_labels is not None:
            scheduling_strategy = NodeLabelSchedulingStrategy(
                hard={}, soft=target_labels
            )
            target_node_id = None

        actor_options = copy.deepcopy(scheduling_request.actor_options)
        if label_selector_chain and placement_group is None and per_replica_pg is None:
            first, *rest = label_selector_chain
            if first or "label_selector" in actor_options:
                actor_options["label_selector"] = first
            if rest:
                actor_options["fallback_strategy"] = [
                    {"label_selector": selector} for selector in rest
                ]
            else:
                actor_options.pop("fallback_strategy", None)
        if (
            scheduling_request.max_replicas_per_node is not None
            and scheduling_request.max_replicas_per_node > 0
        ):
            if "resources" not in actor_options:
                actor_options["resources"] = {}
            # Using implicit resource (resources that every node
            # implicitly has and total is 1)
            # to limit the number of replicas on a single node.
            actor_options["resources"][
                f"{IMPLICIT_RESOURCE_PREFIX}"
                f"{deployment_id.app_name}:{deployment_id.name}"
            ] = (1.0 / scheduling_request.max_replicas_per_node)

        try:
            actor_handle = scheduling_request.actor_def.options(
                scheduling_strategy=scheduling_strategy,
                **actor_options,
            ).remote(*scheduling_request.actor_init_args)
        except Exception:
            # We add a defensive exception here, so the controller can
            # make progress even if the actor options are misconfigured.
            logger.exception(f"Failed to create an actor for {replica_id}.")
            # Remove the per replica PG so it doesn't leak resources.
            # Gang PGs are not removed here.
            if per_replica_pg is not None:
                try:
                    ray.util.remove_placement_group(per_replica_pg)
                except Exception:
                    logger.exception(
                        f"Failed to clean up placement group for {replica_id} "
                        "after actor creation failure."
                    )
            scheduling_request.status = (
                ReplicaSchedulingRequestStatus.ACTOR_CREATION_FAILED
            )
            return False

        del self._pending_replicas[deployment_id][replica_id]
        self._on_replica_launching(
            replica_id, target_node_id=target_node_id, target_labels=target_labels
        )

        if isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy):
            placement_group = scheduling_strategy.placement_group

        scheduling_request.status = ReplicaSchedulingRequestStatus.SUCCEEDED
        scheduling_request.on_scheduled(actor_handle, placement_group=placement_group)
        return True

    def schedule_gang_placement_groups(
        self,
        gang_requests: Dict[DeploymentID, GangPlacementGroupRequest],
    ) -> Dict[DeploymentID, GangReservationResult]:
        """Reserve gang placement groups for gang scheduling.

        Creates gang placement groups before replicas are created, allowing
        the scheduler to verify resource feasibility upfront.

        Args:
            gang_requests: A dictionary of deployment ID to gang placement group request.

        Returns:
            A dictionary of deployment ID to gang reservation result.
        """
        return {
            deployment_id: self._prepare_gangs_for_deployment(deployment_id, request)
            for deployment_id, request in gang_requests.items()
        }

    def _prepare_gangs_for_deployment(
        self,
        deployment_id: DeploymentID,
        request: GangPlacementGroupRequest,
    ) -> GangReservationResult:
        """Create gang placement groups for a single deployment.

        Example:
        - Case 1: Per-replica bundles are defined
        gang_size=2, replica_placement_group_bundles=[{"GPU":1,"CPU":1},{"CPU":1}]

        Requested gang placement group:
        [{"GPU":1,"CPU":1}, {"CPU":1}, {"GPU":1,"CPU":1}, {"CPU":1}]
         ^^^^^^^ replica 0 ^^^^^^^^^^  ^^^^^^^^ replica 1 ^^^^^^^^^
        Replica 0 actor → bundle index 0, replica 1 actor → bundle index 2.
        Remaining bundles (1, 3) are used by child tasks/actors.

        - Case 2: Per-replica bundles are not defined
        gang_size=2, replica_resource_dict={"CPU":2,"GPU":1}

        Requested gang placement group:
        [{"CPU":2,"GPU":1}, {"CPU":2,"GPU":1}]
         ^^^ replica 0 ^^^  ^^^ replica 1 ^^^
        Replica 0 actor → bundle index 0, replica 1 actor → bundle index 1.

        Args:
            deployment_id: The deployment to create gangs for.
            request: Contains gang config and number of replicas to add.

        Returns:
            GangReservationResult with all created gang PGs.
        """
        gang_size = request.gang_size

        if request.num_replicas_to_add % gang_size != 0:
            logger.error(
                f"num_replicas_to_add {request.num_replicas_to_add} "
                f"is not divisible by gang_size {gang_size}."
            )
            return GangReservationResult(
                success=False,
                error_message=(
                    f"num_replicas_to_add {request.num_replicas_to_add} "
                    f"is not divisible by gang_size {gang_size}."
                ),
            )
        num_gangs = request.num_replicas_to_add // gang_size

        per_replica_bundles = request.replica_placement_group_bundles
        has_pg_bundles = (
            per_replica_bundles is not None and len(per_replica_bundles) > 0
        )

        # Flatten per-replica bundles to form a placement group to atomically reserve resources
        # required for each gang
        gang_pgs: List[PlacementGroup] = []
        gang_ids: List[str] = []
        gang_pg_names: List[str] = []
        for gang_index in range(num_gangs):
            if has_pg_bundles:
                assert per_replica_bundles is not None
                bundles = [
                    bundle.copy()
                    for _ in range(gang_size)
                    for bundle in per_replica_bundles
                ]
                label_selector = (
                    [
                        selector.copy()
                        for _ in range(gang_size)
                        for selector in request.replica_pg_bundle_label_selector
                    ]
                    if request.replica_pg_bundle_label_selector is not None
                    else None
                )
                fallback_strategy = (
                    [
                        strategy.copy()
                        for _ in range(gang_size)
                        for strategy in request.replica_pg_fallback_strategy
                    ]
                    if request.replica_pg_fallback_strategy is not None
                    else None
                )
            else:
                bundles = [
                    request.replica_resource_dict.copy() for _ in range(gang_size)
                ]
                label_selector = None
                fallback_strategy = None

            gang_id = uuid.uuid4().hex[:8]
            pg_name = (
                f"{GANG_PG_NAME_PREFIX}{deployment_id.app_name}"
                f"_{deployment_id.name}"
                f"_{gang_index}_{gang_id}"
            )

            try:
                pg = self._create_placement_group_fn(
                    CreatePlacementGroupRequest(
                        bundles=bundles,
                        strategy=request.gang_placement_strategy,
                        target_node_id=None,
                        name=pg_name,
                        bundle_label_selector=label_selector,
                        fallback_strategy=fallback_strategy,
                    )
                )
                gang_pgs.append(pg)
                gang_ids.append(gang_id)
                gang_pg_names.append(pg_name)
            except Exception:
                # Follow the same pattern as single-replica PG creation failure:
                # log and skip this gang so the controller can make progress with
                # the other gangs. The missing replicas will be retried on the next
                # reconciliation loop.
                logger.exception(
                    f"Failed to create gang placement group "
                    f"{gang_index} for {deployment_id}."
                )
                continue

        if not gang_pgs:
            return GangReservationResult(
                success=False,
                error_message=(
                    f"Failed to create any gang placement groups for {deployment_id}."
                ),
            )

        logger.info(
            f"Created {len(gang_pgs)} of {num_gangs} gang PG(s) for "
            f"{deployment_id}. Actors will wait for resource allocation."
        )
        return GangReservationResult(
            success=True,
            gang_pgs=gang_pgs,
            gang_ids=gang_ids,
            gang_pg_names=gang_pg_names,
        )

    def schedule(
        self,
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]],
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest],
    ) -> Dict[DeploymentID, Set[ReplicaID]]:
        """Called for each update cycle to do batch scheduling.

        Args:
            upscales: a dict of deployment name to a list of replicas to schedule.
            downscales: a dict of deployment name to a downscale request.

        Returns:
            The IDs of replicas to stop for each deployment.
        """
        # Update pending replicas from upscales.
        for upscale in upscales.values():
            for scheduling_request in upscale:
                replica_id = scheduling_request.replica_id
                deployment_id = replica_id.deployment_id
                self._pending_replicas[deployment_id][replica_id] = scheduling_request

        # Check for deprecated environment variable usage
        if RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY:
            warnings.warn(
                "The environment variable 'RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY' "
                "is deprecated and will be removed in a v2.55.0 release. "
                "Please use 'RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY' instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        self._update_moves()
        self._schedule_pending_replicas()
        self._update_rule_status()

        # Handle downscales
        deployment_to_replicas_to_stop = {}
        for downscale in downscales.values():
            deployment_to_replicas_to_stop[
                downscale.deployment_id
            ] = self._get_replicas_to_stop(
                downscale.deployment_id,
                downscale.num_to_stop,
                gang_id_by_replica=downscale.gang_id_by_replica,
                replicas_by_gang_id=downscale.replicas_by_gang_id,
                gang_size=downscale.gang_size,
            )

        return deployment_to_replicas_to_stop

    def _use_pack_strategy(self) -> bool:
        return RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY

    def _node_scorer(self) -> NodeScorer:
        return PackNodeScorer() if self._use_pack_strategy() else SpreadNodeScorer()

    def _cluster_view(self) -> ClusterView:
        active_nodes = self._cluster_node_info_cache.get_active_node_ids()
        return ClusterView(
            node_labels={
                node_id: self._cluster_node_info_cache.get_node_labels(node_id)
                for node_id in active_nodes
            },
            # Compute available resources once upfront and update incrementally
            # as replicas are scheduled.
            # Complexity is O(launching + running + nodes + requests * nodes).
            available_resources=self._get_available_resources_per_node(),
            # Seed with running replicas; updated incrementally as replicas are
            # scheduled so that newly occupied nodes are treated as non-idle.
            node_to_assigned_replicas=self._get_node_to_running_replicas(),
            total_resources={
                node_id: AvailableNodeResources(resources)
                for node_id, resources in (
                    self._cluster_node_info_cache.get_total_resources_per_node().items()
                )
                if node_id in active_nodes
            },
            replicas_per_node=self._get_replicas_per_node(active_nodes),
        )

    def _get_replicas_per_node(
        self, active_nodes: Set[str]
    ) -> DefaultDict[DeploymentID, "Counter[str]"]:
        """Running and targeted launching replicas per node, per deployment."""
        replicas_per_node: DefaultDict[DeploymentID, Counter[str]] = defaultdict(
            Counter
        )
        for deployment_id, replica_nodes in self._running_replicas.items():
            outdated = self._outdated_replicas.get(deployment_id, set())
            for replica_id, node_id in replica_nodes.items():
                if node_id in active_nodes and replica_id not in outdated:
                    replicas_per_node[deployment_id][node_id] += 1
        for deployment_id, launching_replicas in self._launching_replicas.items():
            outdated = self._outdated_replicas.get(deployment_id, set())
            for replica_id, info in launching_replicas.items():
                if replica_id in outdated:
                    continue
                target = info.target_node_id
                if target is not None and target in active_nodes:
                    replicas_per_node[deployment_id][target] += 1
        self._leave_out_compacting_replicas(replicas_per_node)
        return replicas_per_node

    def _leave_out_compacting_replicas(
        self, replicas_per_node: DefaultDict[DeploymentID, "Counter[str]"]
    ) -> None:
        """Drops replicas leaving the compacting node from the layout."""
        if self._compacting_node is None:
            return
        node_id = self._compacting_node.target_node_id
        for replica_id in self._compacting_node.cached_running_replicas_on_target_node:
            counts = replicas_per_node[replica_id.deployment_id]
            if counts[node_id] > 0:
                counts[node_id] -= 1

    def _num_replicas(self, deployment_id: DeploymentID) -> int:
        """Replicas in every state, leaving out outdated ones."""
        return max(
            0,
            len(self._pending_replicas[deployment_id])
            + len(self._launching_replicas[deployment_id])
            + len(self._recovering_replicas[deployment_id])
            + len(self._running_replicas[deployment_id])
            - len(self._outdated_replicas.get(deployment_id, ())),
        )

    def _context_for(
        self,
        deployment_id: DeploymentID,
        placement_candidates: List[Tuple[RequestedResources, List[Dict[str, str]]]],
        view: ClusterView,
    ) -> SchedulingContext:
        candidate_nodes = {
            node_id
            for node_id, total in view.total_resources.items()
            if any(
                total.can_fit(resources)
                and all(
                    node_labels_match_selector(
                        view.node_labels.get(node_id, {}), labels
                    )
                    for labels in required_labels
                )
                for resources, required_labels in placement_candidates
            )
        }
        return SchedulingContext(
            deployment_id=deployment_id,
            node_labels=view.node_labels,
            replicas_per_node=view.replicas_per_node[deployment_id],
            num_replicas=self._num_replicas(deployment_id),
            candidate_nodes=candidate_nodes,
        )

    def _schedule_pending_replicas(self) -> None:
        """Filters, scores and binds every pending replica, largest first."""
        scorer = self._node_scorer()
        # Flatten dict of deployment replicas into all replicas,
        # then sort by decreasing resource size
        all_scheduling_requests = sorted(
            sorted(
                _flatten(self._pending_replicas).values(),
                key=lambda r: str(r.replica_id),
            ),
            key=lambda r: r.requested_resources,
            reverse=True,
        )
        self._held_replicas.clear()
        if not all_scheduling_requests:
            return
        if scorer.picks_nodes:
            self._log_schedule_order(all_scheduling_requests)

        view = self._cluster_view()
        for scheduling_request in all_scheduling_requests:
            target_node = self._schedule_one(scheduling_request, scorer, view)
            # Incrementally update available resources for the target node.
            # This is slightly conservative compared to recomputing from
            # scratch (since we subtract from the min of GCS and calculated
            # resources rather than only from the calculated side), but
            # _get_available_resources_per_node is already best-effort.
            if target_node:
                view.take(
                    target_node,
                    scheduling_request.requested_resources,
                    scheduling_request.replica_id,
                )

    def _log_schedule_order(
        self, all_scheduling_requests: List[ReplicaSchedulingRequest]
    ) -> None:
        order_log_key = tuple(r.replica_id for r in all_scheduling_requests)
        if order_log_key == self._last_pack_schedule_order_log_key:
            return
        self._last_pack_schedule_order_log_key = order_log_key
        if Resources.CUSTOM_PRIORITY:
            priority_desc = (
                f"{Resources.CUSTOM_PRIORITY} " f"(then GPU, CPU, memory, other custom)"
            )
        else:
            priority_desc = (
                "GPU, CPU, memory, other custom "
                "(RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES unset)"
            )
        order_desc = ", ".join(
            f"{r.replica_id.deployment_id.name}"
            f"[{_format_resources_for_scheduling_log(r.requested_resources)}]"
            for r in all_scheduling_requests
        )
        logger.info(
            f"Pack scheduling {len(all_scheduling_requests)} pending "
            f"replica(s). Resource priority: {priority_desc}. "
            f"Schedule order (first scheduled first): {order_desc}."
        )

    def _schedule_one(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        scorer: NodeScorer,
        view: ClusterView,
    ) -> Optional[str]:
        """Places one replica. Returns its target node, or None if Serve picked none."""
        deployment_id = scheduling_request.replica_id.deployment_id
        if self._bind_move_replacement(scheduling_request, view):
            return self._launching_replicas[deployment_id][
                scheduling_request.replica_id
            ].target_node_id
        rules = self._rules_for(deployment_id)
        info = self._deployments[deployment_id]
        serve_picks = bool(info.rules) or (
            scorer.picks_nodes and not info.is_non_strict_pack_pg()
        )
        if not serve_picks:
            self._schedule_replica(
                scheduling_request,
                default_scheduling_strategy=scorer.default_scheduling_strategy,
            )
            return None

        placement_candidates = self._build_pack_placement_candidates(scheduling_request)
        ctx = self._context_for(deployment_id, placement_candidates, view)
        target_node = self._select_node(placement_candidates, rules, ctx, scorer, view)
        if target_node is None:
            unmet = self._unmet_required_floors(rules, ctx)
            if unmet:
                if self._hold_for(scheduling_request, unmet, ctx):
                    return None
                rules = tuple(rule for rule in rules if rule not in unmet)
                target_node = self._select_node(
                    placement_candidates, rules, ctx, scorer, view
                )

        replica_id = scheduling_request.replica_id

        if (
            target_node is None
            and self._compacting_node is not None
            and self._pack_decides_placement(scheduling_request)
        ):
            # No node other than the one being compacted has room, so the
            # migration plan is no longer feasible. Either this replica needs
            # the target, or it is a migration replacement whose predecessor
            # still holds the only room in the cluster and would otherwise sit
            # pending until the compaction timed out. Cancel now. Next update
            # the migrating replicas return to RUNNING and this replica either
            # places on the former target or is scaled away as surplus.
            logger.info(
                f"Canceling compaction of {self._compacting_node.target_node_id} "
                f"because {replica_id} "
                f"({_format_resources_for_scheduling_log(scheduling_request.requested_resources)}) "
                f"fits on no other node."
            )
            self._fail_compaction()
            return None

        if target_node is None:
            if replica_id not in self._logged_pack_placement_failures:
                self._logged_pack_placement_failures.add(replica_id)
                logger.info(
                    f"Could not place {replica_id} "
                    f"({_format_resources_for_scheduling_log(scheduling_request.requested_resources)}): "
                    f"no node with room passed the placement rules. "
                    f"Falling back to {scorer.default_scheduling_strategy} scheduling."
                )

        succeeded = self._schedule_replica(
            scheduling_request,
            default_scheduling_strategy=scorer.default_scheduling_strategy,
            target_node_id=target_node,
            label_selector_chain=self._label_selector_chain(
                scheduling_request, rules, ctx, target_node
            ),
        )

        if succeeded and target_node is not None:
            self._logged_pack_placement_failures.discard(replica_id)
            logger.info(
                f"Scheduled {replica_id} "
                f"({_format_resources_for_scheduling_log(scheduling_request.requested_resources)}) "
                f"onto node {target_node}."
            )
        elif not succeeded and target_node is not None:
            if replica_id not in self._logged_pack_placement_failures:
                self._logged_pack_placement_failures.add(replica_id)
                logger.info(
                    f"Failed to launch {replica_id} "
                    f"({_format_resources_for_scheduling_log(scheduling_request.requested_resources)}) "
                    f"on node {target_node}."
                )

        return target_node if succeeded else None

    def _build_pack_placement_candidates(
        self, scheduling_request: ReplicaSchedulingRequest
    ) -> List[Tuple[RequestedResources, List[Dict[str, str]]]]:
        """Returns a list of (resources, labels) tuples to attempt for scheduling."""

        # Collect a list of required resources and labels to try to schedule to
        # support replica compaction when fallback strategies are provided.
        placement_candidates = []
        primary_labels = []
        primary_bundles = scheduling_request.placement_group_bundles

        if primary_bundles:
            # PG: Use PG bundle_label_selector
            if scheduling_request.placement_group_bundle_label_selector:
                pg_strategy = scheduling_request.placement_group_strategy or None
                if pg_strategy == "STRICT_PACK":
                    # All bundle_label_selectors must be satisfied on same node.
                    primary_labels = (
                        scheduling_request.placement_group_bundle_label_selector
                    )
                else:
                    primary_labels = (
                        scheduling_request.placement_group_bundle_label_selector[:1]
                    )
        else:
            # Actor: Use Actor label selector
            if "label_selector" in scheduling_request.actor_options:
                primary_labels = [
                    scheduling_request.actor_options["label_selector"] or {}
                ]

        # If PG is defined on scheduling request, then `requested_resources` represents the sum across all bundles.
        placement_candidates.append(
            (scheduling_request.requested_resources, primary_labels)
        )

        if scheduling_request.placement_group_fallback_strategy:
            # TODO(ryanaoleary@): Add support for placement group fallback_strategy when it's added to options.
            raise NotImplementedError(
                "Placement Group fallback strategies are not yet supported in the Serve scheduler."
            )

        elif scheduling_request.actor_options.get("fallback_strategy"):
            # Fallback strategy provided for Ray Actor.
            for fallback in scheduling_request.actor_options["fallback_strategy"]:
                fallback_labels = [fallback.get("label_selector", {}) or {}]
                placement_candidates.append(
                    (scheduling_request.requested_resources, fallback_labels)
                )

        return placement_candidates

    def _get_replicas_to_stop(
        self,
        deployment_id: DeploymentID,
        max_num_to_stop: int,
        gang_id_by_replica: Optional[Dict[ReplicaID, str]] = None,
        replicas_by_gang_id: Optional[Dict[str, Set[ReplicaID]]] = None,
        gang_size: Optional[int] = None,
    ) -> Set[ReplicaID]:
        """Select which replicas to stop for a downscale request in the following priority:
        1. Prioritize replicas that are not in the RUNNING state.
        2. Prioritize replicas not on the head node because we can't relinquish the head node.
        3. Prioritize replicas on fallback nodes that don't match the label or bundle label selector.
        4. Prioritize replicas on nodes with fewest total replicas so we can relinquish them.
        5. Prioritize newer replicas over older replicas.
        Ties go to the lowest node id, then replica id.
        Note that this algorithm doesn't consider other non-serve actors on the same node.
        See more at https://github.com/ray-project/ray/issues/20599.

        For gang deployments, the same priority order is applied, but entire
        gangs are selected atomically instead of individual replicas.
        """
        replicas_priority: List[ReplicaID] = []

        # Replicas not in running state don't have node id.
        # We will prioritize those first.
        replicas_priority.extend(
            sorted(
                set().union(
                    self._pending_replicas[deployment_id].keys(),
                    self._launching_replicas[deployment_id].keys(),
                    self._recovering_replicas[deployment_id],
                ),
                key=str,
            )
        )
        labels_to_check: List[Dict[str, str]] = []
        if label_selector := self._deployments[deployment_id].label_selector:
            labels_to_check.append(label_selector)
        elif bundle_label_selector := self._deployments[
            deployment_id
        ].bundle_label_selector:
            labels_to_check.extend(bundle_label_selector)
        node_to_running_replicas_of_all_deployments = (
            self._get_node_to_running_replicas()
        )

        # _running_replicas preserves insertion order (oldest → newest).
        # Reverse once so we have newest → oldest, then bucket by node.
        ordered_running_replicas = list(self._running_replicas[deployment_id].items())
        ordered_running_replicas.reverse()
        ordered_running_replicas_of_target_deployment: Dict[
            str, List[ReplicaID]
        ] = defaultdict(list)
        for replica_id, replica_node_id in ordered_running_replicas:
            ordered_running_replicas_of_target_deployment[replica_node_id].append(
                replica_id
            )

        # Prioritize based on following priority:
        # 1. Prioritize replicas not on the head node because we can't relinquish the head node.
        # 2. Prioritize replicas on fallback nodes that don't match the label or bundle label selector.
        # 3. Prioritize replicas on nodes with fewer total replicas so we can relinquish them.
        def scale_down_priority(
            node_and_replicas: Tuple[str, Set[ReplicaID]],
        ) -> Tuple[int, int, int, str]:
            node_id, all_replicas = node_and_replicas
            node_labels = self._cluster_node_info_cache.get_node_labels(node_id)
            match_labels = not labels_to_check or any(
                node_labels_match_selector(node_labels, labels)
                for labels in labels_to_check
            )
            is_head_node = node_id == self._head_node_id
            return int(is_head_node), int(match_labels), len(all_replicas), node_id

        for node_id, _ in sorted(
            node_to_running_replicas_of_all_deployments.items(), key=scale_down_priority
        ):
            if node_id not in ordered_running_replicas_of_target_deployment:
                continue

            # Newest-first list for this node.
            for replica_id in ordered_running_replicas_of_target_deployment[node_id]:
                replicas_priority.append(replica_id)

        replicas_to_stop: Set[ReplicaID] = set()
        if gang_id_by_replica is not None:
            # Gang scheduling is enabled: select entire gangs to stop atomically
            assert gang_size is not None
            assert replicas_by_gang_id is not None
            selected_gangs: Set[str] = set()
            for replica_id in replicas_priority:
                gang_id = gang_id_by_replica.get(replica_id)
                if gang_id is None or gang_id in selected_gangs:
                    continue
                if len(replicas_to_stop) + gang_size > max_num_to_stop:
                    break
                selected_gangs.add(gang_id)
                replicas_to_stop.update(replicas_by_gang_id[gang_id])
        else:
            # Single-replica scheduling: select individual replicas to stop
            replicas_to_stop = self._choose_stops(
                deployment_id, replicas_priority, max_num_to_stop
            )

        return replicas_to_stop

    def _choose_stops(
        self,
        deployment_id: DeploymentID,
        replicas_priority: List[ReplicaID],
        max_num_to_stop: int,
    ) -> Set[ReplicaID]:
        """Picks stops in priority order that keep the deployment's rules.

        If too few stops pass, optional rules are dropped, last listed first.
        """
        max_num_to_stop = min(max_num_to_stop, len(replicas_priority))
        rules = self._deployments[deployment_id].rules
        outdated = self._outdated_replicas.get(deployment_id, set())
        node_by_replica = {
            replica_id: node_id
            for replica_id, node_id in self._running_replicas[deployment_id].items()
            if replica_id not in outdated
        }
        for replica_id, info in self._launching_replicas[deployment_id].items():
            if info.target_node_id is not None and replica_id not in outdated:
                node_by_replica[replica_id] = info.target_node_id
        node_labels = {
            node_id: self._cluster_node_info_cache.get_node_labels(node_id)
            for node_id in self._cluster_node_info_cache.get_active_node_ids()
        }
        num_replicas_after = len(replicas_priority) - max_num_to_stop

        tiers = rule_tiers(rules)
        for tier in tiers:
            replicas_per_node = Counter(node_by_replica.values())
            ctx = SchedulingContext(
                deployment_id=deployment_id,
                node_labels=node_labels,
                replicas_per_node=replicas_per_node,
                num_replicas=num_replicas_after,
            )
            replicas_to_stop: Set[ReplicaID] = set()
            for replica_id in replicas_priority:
                if len(replicas_to_stop) == max_num_to_stop:
                    break
                node_id = node_by_replica.get(replica_id)
                if node_id is not None:
                    if not all(rule.may_stop(node_id, ctx) for rule in tier):
                        continue
                    replicas_per_node[node_id] -= 1
                replicas_to_stop.add(replica_id)
            if len(replicas_to_stop) == max_num_to_stop:
                break

        if tier != tiers[0]:
            dropped = [rule.describe() for rule in tiers[0] if rule not in tier]
            logger.info(
                f"No set of {max_num_to_stop} replica(s) of {deployment_id} could "
                f"stop while keeping every rule, so {dropped} gave way, last "
                "listed first."
            )
        if len(replicas_to_stop) < max_num_to_stop and tier:
            logger.info(
                f"Stopping {len(replicas_to_stop)} of the {max_num_to_stop} "
                f"replica(s) of {deployment_id} asked for, because "
                f"{[rule.describe() for rule in tier]} never give way."
            )
        return replicas_to_stop

    def _filter_nodes_by_label_selector(
        self,
        available_nodes: Dict[str, AvailableNodeResources],
        required_labels: Dict[str, str],
        node_labels: Dict[str, Dict[str, str]],
    ) -> Dict[str, AvailableNodeResources]:
        """Filters available nodes based on label selector constraints."""
        return {
            node_id: resources
            for node_id, resources in available_nodes.items()
            if node_labels_match_selector(node_labels.get(node_id, {}), required_labels)
        }

    def _pack_decides_placement(
        self, scheduling_request: ReplicaSchedulingRequest
    ) -> bool:
        """Whether the pack decision, not the request itself, picks the node.

        A pinned replica follows its proxy and a gang member follows its
        reserved placement group, so where either lands says nothing about
        whether the compaction plan is still feasible.
        """
        return (
            scheduling_request.target_node_id is None
            and scheduling_request.gang_placement_group is None
        )

    def _compacting_node_ids(self) -> Set[str]:
        if self._compacting_node is None:
            return set()
        return {self._compacting_node.target_node_id}

    def _cluster_rules(self) -> Tuple[SchedulingConstraint, ...]:
        return (self._compaction_rule,)

    def _rules_for(
        self, deployment_id: DeploymentID
    ) -> Tuple[SchedulingConstraint, ...]:
        """The deployment's own rules, then the cluster wide rules."""
        return self._deployments[deployment_id].rules + self._cluster_rules()

    def _stop_probe(self, deployment_id: DeploymentID) -> None:
        probe = self._probes.pop(deployment_id, None)
        if probe is not None:
            probe.kill()

    def get_outdated_replicas_to_stop(self) -> Dict[DeploymentID, Set[ReplicaID]]:
        """Outdated replicas that fill a domain a held replica needs."""
        return {d: set(ids) for d, ids in self._outdated_to_stop.items() if ids}

    def get_held_replicas(self) -> Dict[DeploymentID, Set[ReplicaID]]:
        """Replicas held for required floors in the last update."""
        return {d: set(ids) for d, ids in self._held_replicas.items() if ids}

    def get_unmet_rules(self, deployment_id: DeploymentID) -> List[str]:
        """The deployment's unmet topology rules as of the last update."""
        return list(self._unmet_rules.get(deployment_id, []))

    def _update_rule_status(self) -> None:
        """Records each deployment's unmet rules and keeps probes."""
        view: Optional[ClusterView] = None
        self._outdated_to_stop.clear()
        for deployment_id in sorted(self._deployments, key=str):
            info = self._deployments[deployment_id]
            floors = [
                rule
                for rule in info.rules
                if isinstance(rule, MinTopologyDomainsConstraint)
            ]
            placement_candidates = (
                self._get_deployment_placement_candidates(info) if floors else None
            )
            if not floors or placement_candidates is None:
                self._unmet_rules.pop(deployment_id, None)
                self._stop_probe(deployment_id)
                continue

            view = view or self._cluster_view()
            ctx = self._context_for(deployment_id, placement_candidates, view)
            self._unmet_rules[deployment_id] = [
                rule.describe() for rule in floors if not rule.is_met(ctx)
            ]
            for rule in floors:
                tags = {
                    "deployment": deployment_id.name,
                    "application": deployment_id.app_name,
                    "rule": rule.describe(),
                }
                self._covered_domains_gauge.set(
                    len(rule.covered_domains(ctx)), tags=tags
                )
                self._floor_domains_gauge.set(rule.effective_floor(ctx), tags=tags)
            unmet = next(
                (rule for rule in floors if rule.required and not rule.is_met(ctx)),
                None,
            )
            if unmet is not None:
                self._outdated_to_stop[
                    deployment_id
                ] = self._outdated_in_missing_domains(deployment_id, unmet, ctx)
            probe = self._probes.get(deployment_id)
            if unmet is None:
                if probe is not None:
                    logger.info(
                        f"Removing the probe of {deployment_id}, because its "
                        "required rules hold."
                    )
                self._stop_probe(deployment_id)
                continue

            if not self._needs_probe(info, placement_candidates, unmet, ctx, view):
                self._stop_probe(deployment_id)
                continue

            if probe is not None:
                landed_node = probe.landed_node()
                if landed_node is not None:
                    logger.info(
                        f"The probe of {deployment_id} for {unmet.describe()} "
                        f"found room on node {landed_node}."
                    )
                    self._stop_probe(deployment_id)
                continue

            own_selector = (
                (info.bundle_label_selector or [{}])[0]
                if info.placement_group_bundles is not None
                else info.label_selector or {}
            )
            selector = merge_selectors(
                [own_selector, unmet.selector(ctx), self._compaction_rule.selector(ctx)]
            )
            try:
                self._probes[deployment_id] = self._create_domain_probe_fn(
                    deployment_id, dict(info.required_resources), selector
                )
            except Exception:
                logger.exception(f"Failed to start a probe for {deployment_id}.")
                continue
            logger.info(
                f"Started a probe for {deployment_id} asking for {selector}, "
                f"because {unmet.describe()} covers "
                f"{len(unmet.covered_domains(ctx))} domain(s)."
            )

    def _needs_probe(
        self,
        info: DeploymentSchedulingInfo,
        placement_candidates: List[Tuple[RequestedResources, List[Dict[str, str]]]],
        unmet: MinTopologyDomainsConstraint,
        ctx: SchedulingContext,
        view: ClusterView,
    ) -> bool:
        """Whether to ask the autoscaler for a node in a missing domain."""
        if self._recovering_replicas.get(info.deployment_id):
            return False
        for required_resources, required_labels in placement_candidates:
            with_room = {
                node_id: available
                for node_id, available in view.available_resources.items()
                if available.can_fit(required_resources)
            }
            if self._filter_candidate_nodes(
                with_room, required_labels, (unmet,) + self._cluster_rules(), ctx
            ):
                return False
        return True

    def _outdated_in_missing_domains(
        self,
        deployment_id: DeploymentID,
        rule: MinTopologyDomainsConstraint,
        ctx: SchedulingContext,
    ) -> Set[ReplicaID]:
        """One outdated replica per missing domain for each held replica."""
        waiting = sorted(
            (
                held_id
                for held_id in self._held_replicas.get(deployment_id, ())
                if held_id not in self._stopped_for_held
            ),
            key=str,
        )
        num_held = len(waiting)
        outdated = self._outdated_replicas.get(deployment_id, set())
        if not num_held or not outdated:
            return set()
        covered = rule.covered_domains(ctx)
        chosen: Set[ReplicaID] = set()
        chosen_domains: Set[str] = set()
        for replica_id in sorted(outdated, key=str):
            node_id = self._running_replicas[deployment_id].get(replica_id)
            if node_id is None:
                continue
            domain = label_value(node_id, rule.label_key, ctx.node_labels)
            if domain is None or domain in covered or domain in chosen_domains:
                continue
            self._stopped_for_held[waiting[len(chosen)]] = replica_id
            chosen.add(replica_id)
            chosen_domains.add(domain)
            if len(chosen) == num_held:
                break
        return chosen

    @staticmethod
    def _unmet_required_floors(
        rules: Sequence[SchedulingConstraint], ctx: SchedulingContext
    ) -> Tuple[MinTopologyDomainsConstraint, ...]:
        return tuple(
            rule
            for rule in rules
            if isinstance(rule, MinTopologyDomainsConstraint)
            and rule.required
            and not rule.is_met(ctx)
        )

    def _hold_for(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        unmet: Sequence[MinTopologyDomainsConstraint],
        ctx: SchedulingContext,
    ) -> bool:
        """Whether to hold the replica until a missing domain has room.

        At most one replica per missing domain waits, each up to `wait_timeout_s`.
        """
        replica_id = scheduling_request.replica_id
        held_since = self._held_since.get(replica_id)
        waited_s = time.time() - held_since if held_since is not None else 0.0
        waiting_for = [
            rule
            for rule in unmet
            if rule.wait_timeout_s is None or waited_s < rule.wait_timeout_s
        ]
        if len(waiting_for) < len(unmet):
            logger.info(
                f"{replica_id} waited {waited_s:.0f}s, so "
                f"{[rule.describe() for rule in unmet if rule not in waiting_for]} "
                "gave way for it."
            )
        held = self._held_replicas[ctx.deployment_id]
        if not waiting_for or len(held) >= max(
            rule.missing_domains(ctx) for rule in waiting_for
        ):
            return False
        held.add(replica_id)
        self._held_since.setdefault(replica_id, time.time())
        if replica_id not in self._logged_pack_placement_failures:
            self._logged_pack_placement_failures.add(replica_id)
            logger.info(
                f"Holding {replica_id} until a node with room joins a domain "
                f"{[rule.describe() for rule in unmet]} doesn't cover yet."
            )
        return True

    def _label_selector_chain(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        rules: Sequence[SchedulingConstraint],
        ctx: SchedulingContext,
        target_node: Optional[str] = None,
    ) -> Optional[List[Dict[str, str]]]:
        """The rules as Ray Core label selectors, strictest tier first.

        For a placement group the chain is for bundle 0. None when the
        deployment has no rules or the replica can't be steered.
        """
        if (
            not self._deployments[ctx.deployment_id].rules
            or scheduling_request.gang_placement_group is not None
            or scheduling_request.target_node_id is not None
        ):
            return None
        chain: List[Dict[str, str]] = []
        if scheduling_request.placement_group_bundles is not None:
            own_selectors = [
                (scheduling_request.placement_group_bundle_label_selector or [{}])[0]
            ]
            if (
                target_node is not None
                and scheduling_request.placement_group_strategy != "STRICT_PACK"
            ):
                chain.append(
                    merge_selectors(
                        [own_selectors[0], {RAY_NODE_ID_LABEL: f"in({target_node})"}]
                    )
                )
        else:
            options = scheduling_request.actor_options
            own_selectors = [options.get("label_selector") or {}] + [
                fallback.get("label_selector") or {}
                for fallback in options.get("fallback_strategy") or []
            ]
        for tier in rule_tiers(rules):
            tier_selector = merge_selectors(rule.selector(ctx) for rule in tier)
            for own_selector in own_selectors:
                selector = merge_selectors([own_selector, tier_selector])
                if selector not in chain:
                    chain.append(selector)
        return chain

    def _select_node(
        self,
        placement_candidates: List[Tuple[RequestedResources, List[Dict[str, str]]]],
        rules: Sequence[SchedulingConstraint],
        ctx: SchedulingContext,
        scorer: NodeScorer,
        view: ClusterView,
    ) -> Optional[str]:
        """Picks a node under the strictest tier of rules some node passes."""
        chosen = self._select_node_in_tiers(
            placement_candidates, rule_tiers(rules), ctx, scorer, view
        )
        return chosen[0] if chosen else None

    def _select_node_in_tiers(
        self,
        placement_candidates: List[Tuple[RequestedResources, List[Dict[str, str]]]],
        tiers: Sequence[Tuple[SchedulingConstraint, ...]],
        ctx: SchedulingContext,
        scorer: NodeScorer,
        view: ClusterView,
    ) -> Optional[Tuple[str, int]]:
        """The node picked under the first tier some node passes, and that tier."""
        for index, tier in enumerate(tiers):
            for required_resources, required_labels in placement_candidates:
                target_node = scorer.choose(
                    required_resources,
                    self._filter_candidate_nodes(
                        view.available_resources, required_labels, tier, ctx
                    ),
                    view.node_to_assigned_replicas,
                    ctx,
                )
                if target_node:
                    return target_node, index
        return None

    @staticmethod
    def _tier_of_node(
        node_id: str,
        tiers: Sequence[Tuple[SchedulingConstraint, ...]],
        ctx: SchedulingContext,
    ) -> int:
        """The index of the strictest tier whose rules all allow the node."""
        for index, tier in enumerate(tiers):
            if all(rule.allowed_nodes({node_id}, ctx) for rule in tier):
                return index
        return len(tiers)

    @staticmethod
    def _coverage(
        rules: Sequence[SchedulingConstraint], ctx: SchedulingContext
    ) -> Tuple[int, ...]:
        """Covered domains per floor, capped at the floor, required floors first."""
        floors = [r for r in rules if isinstance(r, MinTopologyDomainsConstraint)]
        ordered = [r for r in floors if r.required] + [
            r for r in floors if not r.required
        ]
        return tuple(
            min(len(rule.covered_domains(ctx)), rule.effective_floor(ctx))
            for rule in ordered
        )

    def start_rebalance_move(self, move: RebalanceMove) -> None:
        """Starts moving a replica to its planned node."""
        self._moves[move.replica_id] = MoveInfo(
            target_node_id=move.target_node_id, start_timestamp_s=time.time()
        )
        logger.info(
            f"Moving {move.replica_id} from node {move.source_node_id} to node "
            f"{move.target_node_id}, because {move.reason}. A replacement starts "
            "there first, and the old replica stops once it runs."
        )

    def rebalance(self, deployment_ids: Iterable[DeploymentID]) -> None:
        """Starts at most one move per settled deployment."""
        now = time.time()
        moving = {replica_id.deployment_id for replica_id in self._moves}
        for deployment_id in sorted(deployment_ids, key=str):
            if (
                deployment_id in moving
                or deployment_id not in self._deployments
                or now < self._next_move_timestamp_s.get(deployment_id, 0)
            ):
                continue
            move = self.plan_rebalance_move(deployment_id)
            if move is None:
                self._next_move_timestamp_s[deployment_id] = (
                    now + COMPACTION_SCAN_INTERVAL_S
                )
                continue
            self.start_rebalance_move(move)

    def get_replicas_to_move(self) -> Dict[DeploymentID, Set[ReplicaID]]:
        """Replicas with a move in progress."""
        replicas: DefaultDict[DeploymentID, Set[ReplicaID]] = defaultdict(set)
        for replica_id in self._moves:
            replicas[replica_id.deployment_id].add(replica_id)
        return dict(replicas)

    def _bind_move_replacement(
        self, scheduling_request: ReplicaSchedulingRequest, view: ClusterView
    ) -> bool:
        """Binds a replacement to its move's planned node. Returns whether it did.

        Cancels the move if the node has no room.
        """
        deployment_id = scheduling_request.replica_id.deployment_id
        moving = next(
            (
                (replica_id, info)
                for replica_id, info in sorted(
                    self._moves.items(), key=lambda m: str(m[0])
                )
                if replica_id.deployment_id == deployment_id
                and info.replacement_id is None
            ),
            None,
        )
        if moving is None:
            return False
        moving_id, info = moving
        available = view.available_resources.get(info.target_node_id)
        if available is None or not available.can_fit(
            scheduling_request.requested_resources
        ):
            self._fail_move(moving_id, "the target node no longer has room")
            return False
        if not self._schedule_replica(
            scheduling_request,
            default_scheduling_strategy="DEFAULT",
            target_node_id=info.target_node_id,
            label_selector_chain=(
                None
                if scheduling_request.placement_group_bundles is not None
                else self._pinned_selector_chain(
                    scheduling_request, info.target_node_id
                )
            ),
        ):
            return False
        info.replacement_id = scheduling_request.replica_id
        return True

    @staticmethod
    def _pinned_selector_chain(
        scheduling_request: ReplicaSchedulingRequest, node_id: str
    ) -> List[Dict[str, str]]:
        """The replica's selector and fallbacks, each pinned to one node."""
        options = scheduling_request.actor_options
        own_selectors = [options.get("label_selector") or {}] + [
            fallback.get("label_selector") or {}
            for fallback in options.get("fallback_strategy") or []
        ]
        chain: List[Dict[str, str]] = []
        for own_selector in own_selectors:
            selector = merge_selectors([own_selector, {RAY_NODE_ID_LABEL: node_id}])
            if selector not in chain:
                chain.append(selector)
        return chain

    def _on_move_replica_stopping(self, replica_id: ReplicaID) -> None:
        if replica_id in self._moves:
            info = self._moves.pop(replica_id)
            if info.replacement_id is not None:
                logger.info(f"Moved {replica_id} to node {info.target_node_id}.")
                self._num_failed_moves.pop(replica_id.deployment_id, None)
            return
        for moving_id, info in list(self._moves.items()):
            if info.replacement_id == replica_id:
                self._fail_move(moving_id, f"its replacement {replica_id} stopped")

    def _fail_move(self, replica_id: ReplicaID, reason: str) -> None:
        info = self._moves.pop(replica_id, None)
        if info is None:
            return
        deployment_id = replica_id.deployment_id
        self._num_failed_moves[deployment_id] += 1
        backoff_s = min(
            2 ** self._num_failed_moves[deployment_id],
            RAY_SERVE_COMPACTION_MAX_BACKOFF_TIME_S,
        )
        self._next_move_timestamp_s[deployment_id] = time.time() + backoff_s
        logger.info(
            f"Canceling the move of {replica_id} to node {info.target_node_id}, "
            f"because {reason}. Moves of {deployment_id} resume in {backoff_s} "
            "seconds."
        )

    def _update_moves(self) -> None:
        """Fails moves whose replacement didn't run in time."""
        now = time.time()
        for replica_id, info in list(self._moves.items()):
            if now - info.start_timestamp_s >= RAY_SERVE_COMPACTION_TIMEOUT_S:
                self._fail_move(
                    replica_id,
                    f"it didn't finish in {RAY_SERVE_COMPACTION_TIMEOUT_S} seconds",
                )

    def plan_rebalance_move(
        self, deployment_id: DeploymentID
    ) -> Optional[RebalanceMove]:
        """The move that best raises the deployment's coverage, or None.

        A replica only moves to a stricter tier than its node's, so it never
        trades an earlier rule for a later one.
        """
        info = self._deployments[deployment_id]
        if (
            not info.rules
            or info.is_gang
            or info.pins_replicas
            or info.is_non_strict_pack_pg()
        ):
            return None
        placement_candidates = self._get_deployment_placement_candidates(info)
        if placement_candidates is None:
            return None

        view = self._cluster_view()
        layout = view.replicas_per_node[deployment_id]
        tiers = rule_tiers(self._rules_for(deployment_id))
        scorer = self._node_scorer()
        current = self._coverage(
            info.rules, self._context_for(deployment_id, placement_candidates, view)
        )
        outdated = self._outdated_replicas.get(deployment_id, set())

        best: Optional[Tuple[Tuple[int, ...], RebalanceMove]] = None
        for replica_id, node_id in sorted(
            self._running_replicas[deployment_id].items(), key=lambda r: str(r[0])
        ):
            if node_id not in view.available_resources or replica_id in outdated:
                continue
            layout[node_id] -= 1
            ctx = self._context_for(deployment_id, placement_candidates, view)
            source_tier = self._tier_of_node(node_id, tiers, ctx)
            chosen = self._select_node_in_tiers(
                placement_candidates, tiers[:source_tier], ctx, scorer, view
            )
            if chosen is not None:
                target_node, target_tier = chosen
                layout[target_node] += 1
                coverage = self._coverage(info.rules, ctx)
                layout[target_node] -= 1
                if coverage > current and (best is None or coverage > best[0]):
                    gained = [
                        rule.describe()
                        for rule in tiers[target_tier]
                        if source_tier >= len(tiers) or rule not in tiers[source_tier]
                    ]
                    best = (
                        coverage,
                        RebalanceMove(
                            replica_id=replica_id,
                            source_node_id=node_id,
                            target_node_id=target_node,
                            reason=f"node {target_node} holds {gained}",
                        ),
                    )
            layout[node_id] += 1
        return best[1] if best else None

    def _filter_candidate_nodes(
        self,
        available_resources_per_node: Dict[str, AvailableNodeResources],
        required_labels_list: List[Dict[str, str]],
        rules: Sequence[SchedulingConstraint],
        ctx: SchedulingContext,
    ) -> Dict[str, AvailableNodeResources]:
        """Nodes that match every label selector and pass every rule."""
        # Filter feasible nodes by provided label selectors if provided.
        if required_labels_list and ctx.node_labels:
            for required_labels in required_labels_list:
                available_resources_per_node = self._filter_nodes_by_label_selector(
                    available_resources_per_node, required_labels, ctx.node_labels
                )
                if not available_resources_per_node:
                    return {}

        allowed = set(available_resources_per_node)
        for rule in rules:
            allowed = rule.allowed_nodes(allowed, ctx)
        return {
            node_id: res
            for node_id, res in available_resources_per_node.items()
            if node_id in allowed
        }

    def _get_deployment_placement_candidates(
        self, deployment: DeploymentSchedulingInfo
    ) -> Optional[List[Tuple[RequestedResources, List[Dict[str, str]]]]]:
        if deployment.is_gang:
            return None

        actor_options: Dict[str, Any] = {}
        if deployment.label_selector is not None:
            actor_options["label_selector"] = deployment.label_selector
        if deployment.fallback_strategy is not None:
            actor_options["fallback_strategy"] = deployment.fallback_strategy

        return self._build_pack_placement_candidates(
            ReplicaSchedulingRequest(
                replica_id=ReplicaID(
                    unique_id="", deployment_id=deployment.deployment_id
                ),
                actor_def=None,  # type: ignore[arg-type]
                actor_resources=dict(deployment.actor_resources or {}),
                actor_options=actor_options,
                actor_init_args=(),
                on_scheduled=lambda *args, **kwargs: None,
                placement_group_bundles=(
                    [dict(bundle) for bundle in deployment.placement_group_bundles]
                    if deployment.placement_group_bundles
                    else None
                ),
                placement_group_strategy=deployment.placement_group_strategy,
                placement_group_bundle_label_selector=deployment.bundle_label_selector,
                max_replicas_per_node=deployment.max_replicas_per_node,
            )
        )

    def _launching_replicas_on_node_id(self, target_node_id: str) -> Set[ReplicaID]:
        return {
            replica_id
            for replicas in self._launching_replicas.values()
            for replica_id, info in replicas.items()
            if info.target_node_id == target_node_id
        }

    def _running_replicas_on_node_id(self, target_node_id: str) -> Set[ReplicaID]:
        return {
            replica_id
            for replicas in self._running_replicas.values()
            for replica_id, node_id in replicas.items()
            if node_id == target_node_id
        }

    def _is_pinned(self, replica_id: ReplicaID) -> bool:
        """Pinned replicas follow their proxy node, so compaction never moves them.

        The proxy leaves a node once its other replicas are gone and takes the
        pinned replica with it, so they neither need a destination nor count as
        new arrivals on the target.
        """
        return self._deployments[replica_id.deployment_id].pins_replicas

    def _movable_replicas(self, replica_ids: Set[ReplicaID]) -> Set[ReplicaID]:
        return {r for r in replica_ids if not self._is_pinned(r)}

    def _fail_compaction(self):
        self._compacting_node = None
        self._num_consecutive_failed_compactions += 1
        backoff_s = min(
            2**self._num_consecutive_failed_compactions,
            RAY_SERVE_COMPACTION_MAX_BACKOFF_TIME_S,
        )
        self._next_allowed_compaction_timestamp_s = time.time() + backoff_s
        if self._num_consecutive_failed_compactions >= 2:
            logger.info(
                f"Compaction failed {self._num_consecutive_failed_compactions} "
                f"times in a row. Retrying after {backoff_s} seconds."
            )

    def _update_compacting_node_info(self):
        info = self._compacting_node
        assert info is not None
        target_node = info.target_node_id
        current_replicas = self._running_replicas_on_node_id(
            target_node
        ) | self._launching_replicas_on_node_id(target_node)
        new_replicas = (
            self._movable_replicas(current_replicas)
            - info.cached_running_replicas_on_target_node
        )
        now = time.time()

        if target_node not in self._cluster_node_info_cache.get_active_node_ids():
            if info.migration_done_timestamp_s is None:
                # The node died or a real drain took over mid migration, so
                # there is nothing left to compact and nothing to count as a
                # success or failure.
                logger.info(
                    f"Dropping compaction of {target_node} because the node is "
                    "no longer active."
                )
                self._compacting_node = None
            else:
                # The emptied node is draining or gone, so it can no longer
                # take new replicas. Only now has the compaction succeeded.
                logger.info(
                    f"Successfully compacted {target_node}. The node is no "
                    "longer active."
                )
                self._compacting_node = None
                self._num_consecutive_failed_compactions = 0
                self._next_allowed_compaction_timestamp_s = 0
                self._num_succeeded_compactions += 1
                self._num_compacted_nodes_counter.inc()
                ServeUsageTag.NUM_NODE_COMPACTIONS.record(
                    str(self._num_succeeded_compactions)
                )
        elif new_replicas:
            logger.info(
                f"Canceling compaction of {target_node} because new replicas "
                f"have been scheduled on {target_node}: {new_replicas}."
            )
            self._fail_compaction()
        elif now >= info.start_timestamp_s + RAY_SERVE_COMPACTION_TIMEOUT_S:
            if info.migration_done_timestamp_s is None:
                logger.info(
                    f"Migrating replicas off of node {target_node} timed out after "
                    f"{RAY_SERVE_COMPACTION_TIMEOUT_S} seconds. Canceling compaction "
                    f"of node {target_node}. Replicas still running on node "
                    f"{target_node}: {current_replicas}."
                )
            else:
                logger.warning(
                    f"Migrated every replica off of node {target_node}, but the "
                    f"node was not drained within {RAY_SERVE_COMPACTION_TIMEOUT_S} "
                    f"seconds. Canceling compaction of node {target_node}. Check "
                    "that the autoscaler can release idle nodes."
                )
            self._fail_compaction()
        elif not current_replicas:
            if info.migration_done_timestamp_s is None:
                # Every replica has been told to stop, but the actors may still
                # be shutting down and the node is still active. Keep it marked
                # so pack scheduling stays off it until the autoscaler drains it.
                info.migration_done_timestamp_s = now
                logger.info(
                    f"Migrated every replica off of {target_node}. Keeping new "
                    "replicas off the node until it is drained."
                )
        else:
            for threshold_s, amount in ((600, "10 minutes"), (60, "1 minute")):
                threshold = info.start_timestamp_s + threshold_s
                already_logged = (
                    info.last_logged_timeout_warning is not None
                    and info.last_logged_timeout_warning >= threshold
                )
                if now > threshold and not already_logged:
                    logger.warning(
                        f"The controller has been trying to compact {target_node} "
                        f"for more than {amount}. Resources may be unexpectedly "
                        "constrained, or migration is slow because new replicas "
                        "are taking a long time to initialize. Compaction will "
                        "time out and be cancelled if it takes more than "
                        f"{RAY_SERVE_COMPACTION_TIMEOUT_S / 60} minutes."
                    )
                    info.last_logged_timeout_warning = now
                    break

    def get_node_to_compact(
        self, allow_new_compaction: bool
    ) -> Optional[Tuple[str, float]]:
        if not self._use_pack_strategy():
            if self._compacting_node:
                logger.info(
                    f"Canceling compaction of {self._compacting_node.target_node_id} "
                    "because pack scheduling is no longer in effect."
                )
                self._compacting_node = None
            return None

        if self._compacting_node:
            self._update_compacting_node_info()
        if self._compacting_node:
            return self._compacting_node.target_node_id, float("inf")

        if not allow_new_compaction:
            # The cluster is changing, so scan again as soon as it settles.
            self._next_compaction_scan_timestamp_s = 0
            return None

        now = time.time()
        if (
            now < self._next_allowed_compaction_timestamp_s
            or now < self._next_compaction_scan_timestamp_s
            or any(
                set(pending) - self._held_replicas.get(deployment_id, set())
                for deployment_id, pending in self._pending_replicas.items()
            )
            or any(self._launching_replicas.values())
            or any(self._recovering_replicas.values())
        ):
            return None

        target_node_id = self._find_best_node_to_compact()
        if not target_node_id:
            self._next_compaction_scan_timestamp_s = now + COMPACTION_SCAN_INTERVAL_S
            return None

        self._compacting_node = CompactingNodeInfo(
            target_node_id=target_node_id,
            start_timestamp_s=now,
            cached_running_replicas_on_target_node=self._movable_replicas(
                self._running_replicas_on_node_id(target_node_id)
            ),
        )
        return target_node_id, float("inf")

    def _simulate_move(
        self, replica_id: ReplicaID, source_node_id: str, view: ClusterView
    ) -> Optional[Tuple[str, RequestedResources]]:
        """Where the replica would pack without losing a rule it holds, or None.

        `view` must already leave the replica out of its deployment's layout.
        """
        deployment = self._deployments[replica_id.deployment_id]
        if deployment.is_non_strict_pack_pg():
            return None
        placement_candidates = self._get_deployment_placement_candidates(deployment)
        if placement_candidates is None:
            return None

        ctx = self._context_for(replica_id.deployment_id, placement_candidates, view)
        held_rules = tuple(
            rule
            for rule in deployment.rules
            if rule.allowed_nodes({source_node_id}, ctx)
        )
        for required_resources, required_labels in placement_candidates:
            chosen_node = self._compaction_scorer.choose(
                required_resources,
                self._filter_candidate_nodes(
                    view.available_resources,
                    required_labels,
                    held_rules + self._cluster_rules(),
                    ctx,
                ),
                view.node_to_assigned_replicas,
                ctx,
            )
            if chosen_node:
                return chosen_node, required_resources
        return None

    def _find_best_node_to_compact(self) -> Optional[str]:
        node_to_running_replicas = self._get_node_to_running_replicas()
        available_resources_per_node = self._get_available_resources_per_node()
        total_resources_per_node = {
            node_id: _compaction_node_size(resources)
            for node_id, resources in (
                self._cluster_node_info_cache.get_total_resources_per_node().items()
            )
        }
        active_nodes = self._cluster_node_info_cache.get_active_node_ids()
        all_node_labels = {
            node_id: self._cluster_node_info_cache.get_node_labels(node_id)
            for node_id in active_nodes
        }
        active_total_resources = {
            node_id: AvailableNodeResources(resources)
            for node_id, resources in (
                self._cluster_node_info_cache.get_total_resources_per_node().items()
            )
            if node_id in active_nodes
        }
        replicas_per_node = self._get_replicas_per_node(active_nodes)

        best: Optional[Tuple[str, Dict[ReplicaID, str]]] = None
        for target_node_id in sorted(available_resources_per_node):
            if target_node_id == self._head_node_id:
                continue

            # Best-fit binpack the target's replicas onto the other non-idle nodes.
            available_resources = {
                n: v
                for n, v in available_resources_per_node.items()
                if n != target_node_id and len(node_to_running_replicas[n]) > 0
            }
            replicas_on_target = sorted(
                sorted(
                    self._movable_replicas(node_to_running_replicas[target_node_id]),
                    key=str,
                ),
                key=lambda r: self._deployments[r.deployment_id].required_resources,
                reverse=True,
            )
            view = ClusterView(
                node_labels=all_node_labels,
                available_resources=available_resources,
                node_to_assigned_replicas=defaultdict(
                    set, {n: set(r) for n, r in node_to_running_replicas.items()}
                ),
                total_resources=active_total_resources,
                replicas_per_node=defaultdict(
                    Counter,
                    {d: Counter(c) for d, c in replicas_per_node.items()},
                ),
            )
            assignment: Dict[ReplicaID, str] = {}
            for replica_id in replicas_on_target:
                view.replicas_per_node[replica_id.deployment_id][target_node_id] -= 1
                placed = self._simulate_move(replica_id, target_node_id, view)
                if placed is None:
                    assignment.clear()
                    break

                chosen_node, required_resources = placed
                assignment[replica_id] = chosen_node
                view.take(chosen_node, required_resources, replica_id)

            if not assignment:
                continue

            # Prefer the largest compactable node, then the fewest migrations.
            if best is None:
                take = True
            else:
                current_total = total_resources_per_node.get(
                    target_node_id, AvailableNodeResources()
                )
                best_total = total_resources_per_node.get(
                    best[0], AvailableNodeResources()
                )
                take = current_total > best_total or (
                    current_total == best_total and len(assignment) < len(best[1])
                )
            if take:
                best = (target_node_id, assignment)

        if best is None:
            return None

        best_node, best_assignment = best
        node_to_assigned: Dict[str, List[ReplicaID]] = defaultdict(list)
        for replica_id, node_id in best_assignment.items():
            node_to_assigned[node_id].append(replica_id)
        plan = ", ".join(
            f"{replicas} -> {node_id}" for node_id, replicas in node_to_assigned.items()
        )
        logger.info(
            f"Found compactable node '{best_node}' with migration plan: {{{plan}}}."
        )
        return best_node
