import copy
import logging
import uuid
import warnings
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from functools import total_ordering
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set, Tuple

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
    RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES,
    RAY_SERVE_MIN_REPLICA_NODES,
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY,
    SERVE_LOGGER_NAME,
)
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    LabelMatchExpressionsT,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)

RAY_NODE_ID_LABEL = "ray.io/node-id"


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

    def is_non_strict_pack_pg(self) -> bool:
        return (
            self.placement_group_bundles is not None
            and self.placement_group_strategy != "STRICT_PACK"
        )


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


def _flatten(
    deployment_to_replicas: Dict[DeploymentID, Dict[ReplicaID, Any]],
) -> Dict[ReplicaID, Any]:
    """Flattens a dict of {deployment_id: {replica_id: val}} to {replica_id: val}."""

    return {
        replica_id: val
        for replicas in deployment_to_replicas.values()
        for replica_id, val in replicas.items()
    }


def _best_fit_node(
    required_resources: RequestedResources,
    available_resources: Dict[str, AvailableNodeResources],
    tie_break_key: Optional[Callable[[str], Any]] = None,
) -> Optional[str]:
    """Picks the fitting node that would be left with the least free space."""
    min_key = None
    chosen_node = None
    for node_id, available in available_resources.items():
        if not available.can_fit(required_resources):
            continue
        remaining_space = available - required_resources
        current_key = (
            (remaining_space, tie_break_key(node_id))
            if tie_break_key
            else (remaining_space,)
        )
        if min_key is None or current_key < min_key:
            min_key = current_key
            chosen_node = node_id
    return chosen_node


def _filter_nodes_by_label_selector(
    candidates: Dict[str, AvailableNodeResources],
    required_labels: Dict[str, str],
    node_labels: Dict[str, Dict[str, str]],
) -> Dict[str, AvailableNodeResources]:
    return {
        node_id: resources
        for node_id, resources in candidates.items()
        if node_labels_match_selector(node_labels.get(node_id, {}), required_labels)
    }


@dataclass
class SchedulingContext:
    """The cluster facts a filter needs to evaluate its rule for one replica."""

    deployment_id: DeploymentID
    num_replicas: int
    nodes_occupied_by_deployment: Set[str]
    node_labels: Dict[str, Dict[str, str]]


@dataclass
class DownscaleContext:
    """The state a constraint needs to veto the stop of one replica.

    `replicas_per_node` counts only running replicas and shrinks as the
    scheduler picks replicas to stop, so each veto sees the layout that the
    already picked replicas would leave behind.
    """

    deployment_id: DeploymentID
    target_num_replicas: int
    node_by_replica: Dict[ReplicaID, str]
    replicas_per_node: "Counter[str]"
    node_labels: Dict[str, Dict[str, str]]


class SchedulingConstraint:
    """One placement rule, applied at every point where it has an opinion.

    `nodes_to_avoid` names nodes the replica must not use. The scheduler both
    drops them from the candidates and unions them with every other rule's
    set into one `ray.io/node-id` selector for Ray Core, so two rules that
    each forbid a node compose. `eligible` narrows the candidates by any
    other test against cached node state. `required_labels` adds a selector
    on any other label key; the scheduler raises if two rules set the same
    key. `may_stop` vetoes a downscale that would break the rule. Each default
    is a no-op, so a constraint overrides only the points it cares about.
    """

    def applies_to(self, scheduling_request: ReplicaSchedulingRequest) -> bool:
        return True

    def nodes_to_avoid(self, ctx: SchedulingContext) -> Set[str]:
        return set()

    def eligible(
        self,
        candidates: Dict[str, AvailableNodeResources],
        ctx: SchedulingContext,
    ) -> Dict[str, AvailableNodeResources]:
        return candidates

    def required_labels(self, ctx: SchedulingContext) -> Dict[str, str]:
        return {}

    def may_stop(self, replica_id: ReplicaID, ctx: DownscaleContext) -> bool:
        return True


class MinReplicaNodesConstraint(SchedulingConstraint):
    """Spreads a deployment over at least `min_replica_nodes` nodes."""

    def __init__(self, min_replica_nodes: int):
        self._min_replica_nodes = min_replica_nodes

    def applies_to(self, scheduling_request: ReplicaSchedulingRequest) -> bool:
        """A gang's placement group is reserved before the replica exists, so
        no label can be attached to it and the floor cannot be enforced."""
        return scheduling_request.gang_placement_group is None

    def nodes_to_avoid(self, ctx: SchedulingContext) -> Set[str]:
        floor = min(self._min_replica_nodes, ctx.num_replicas)
        occupied = ctx.nodes_occupied_by_deployment
        return set(occupied) if len(occupied) < floor else set()

    def may_stop(self, replica_id: ReplicaID, ctx: DownscaleContext) -> bool:
        node_id = ctx.node_by_replica.get(replica_id)
        if node_id is None or ctx.replicas_per_node[node_id] > 1:
            return True
        floor = min(self._min_replica_nodes, ctx.target_num_replicas)
        return len(ctx.replicas_per_node) > floor


class LabelSelectorConstraint(SchedulingConstraint):
    """Applies a replica's own label selector.

    `required_labels` stays empty because Ray already enforces this selector from
    `ray_actor_options` or from the placement group bundles.
    """

    def __init__(self, label_selector: Dict[str, str]):
        self._label_selector = label_selector

    def eligible(
        self,
        candidates: Dict[str, AvailableNodeResources],
        ctx: SchedulingContext,
    ) -> Dict[str, AvailableNodeResources]:
        return _filter_nodes_by_label_selector(
            candidates, self._label_selector, ctx.node_labels
        )


class SchedulingPreference:
    """One soft rule. It orders otherwise equal choices and never excludes one.

    `node_preference` breaks ties between nodes the scorer rates equally, and
    `stop_preference` breaks ties between nodes the downscale order rates
    equally. Lower keys win. Each default is neutral.
    """

    def node_preference(self, node_id: str, ctx: SchedulingContext) -> Any:
        return 0

    def stop_preference(self, node_id: str, ctx: DownscaleContext) -> Any:
        return 0


class NodeScorer(ABC):
    """Ranks the nodes that passed filtering and picks one for a replica."""

    fallback_scheduling_strategy: str = "DEFAULT"

    @abstractmethod
    def choose(
        self,
        deployment_id: DeploymentID,
        required_resources: RequestedResources,
        candidates: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        tie_break_key: Optional[Callable[[str], Any]] = None,
    ) -> Optional[str]:
        raise NotImplementedError


class PackNodeScorer(NodeScorer):
    """Best fit, preferring nodes that already run replicas."""

    def choose(
        self,
        deployment_id: DeploymentID,
        required_resources: RequestedResources,
        candidates: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        tie_break_key: Optional[Callable[[str], Any]] = None,
    ) -> Optional[str]:
        non_idle_nodes = {
            node_id: resources
            for node_id, resources in candidates.items()
            if node_to_assigned_replicas.get(node_id)
        }
        idle_nodes = {
            node_id: resources
            for node_id, resources in candidates.items()
            if not node_to_assigned_replicas.get(node_id)
        }
        return _best_fit_node(
            required_resources, non_idle_nodes, tie_break_key
        ) or _best_fit_node(required_resources, idle_nodes, tie_break_key)


class SpreadNodeScorer(NodeScorer):
    """Fewest replicas of the same deployment, then the most free space."""

    fallback_scheduling_strategy = "SPREAD"

    def choose(
        self,
        deployment_id: DeploymentID,
        required_resources: RequestedResources,
        candidates: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        tie_break_key: Optional[Callable[[str], Any]] = None,
    ) -> Optional[str]:
        chosen_node = None
        chosen_key: Optional[Tuple[int, Resources, Any]] = None
        for node_id, available in candidates.items():
            if not available.can_fit(required_resources):
                continue
            num_same_deployment = sum(
                1
                for replica_id in node_to_assigned_replicas.get(node_id, ())
                if replica_id.deployment_id == deployment_id
            )
            current_key = (
                num_same_deployment,
                available - required_resources,
                tie_break_key(node_id) if tie_break_key else 0,
            )
            if chosen_key is None or self._prefers(current_key, chosen_key):
                chosen_key = current_key
                chosen_node = node_id
        return chosen_node

    @staticmethod
    def _prefers(key: Tuple[int, Resources, Any], other: Tuple[int, Resources, Any]):
        if key[0] != other[0]:
            return key[0] < other[0]
        if key[1] != other[1]:
            return key[1] > other[1]
        return key[2] < other[2]


@dataclass(frozen=True)
class SchedulingProfile:
    """The constraints and the scorer that one scheduling strategy uses.

    Strategies share constraint objects rather than reimplement them. A strategy
    that wants no floor omits `MinReplicaNodesConstraint`; one that wants a
    different floor names it with a different value. A rule is hard, and a
    constraint, or soft, and a preference; the scheduler has no other hook.
    """

    constraints: List[SchedulingConstraint]
    scorer: NodeScorer
    preferences: List[SchedulingPreference] = field(default_factory=list)


def default_scheduling_profile() -> SchedulingProfile:
    """The profile built from the cluster's environment variables."""
    return SchedulingProfile(
        constraints=[MinReplicaNodesConstraint(RAY_SERVE_MIN_REPLICA_NODES)],
        scorer=PackNodeScorer()
        if RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY
        else SpreadNodeScorer(),
    )


class DeploymentScheduler(ABC):
    """A centralized scheduler for all Serve deployments.

    It makes a batch of scheduling decisions in each update cycle.
    """

    def __init__(
        self,
        cluster_node_info_cache: ClusterNodeInfoCache,
        head_node_id: str,
        create_placement_group_fn: Callable,
        profile: Optional[SchedulingProfile] = None,
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
        self._last_schedule_order_log_key: Optional[tuple] = None
        self._logged_placement_failures: Set[ReplicaID] = set()
        self._logged_skipped_rules: Set[Tuple[DeploymentID, str]] = set()

        self._cluster_node_info_cache = cluster_node_info_cache
        self._head_node_id = head_node_id
        self._create_placement_group_fn = create_placement_group_fn
        self._profile = profile or default_scheduling_profile()

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
    ) -> None:
        assert deployment_id in self._deployments

        info = self._deployments[deployment_id]
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

        self._logged_skipped_rules = {
            key for key in self._logged_skipped_rules if key[0] != deployment_id
        }
        del self._deployments[deployment_id]

    def on_replica_stopping(self, replica_id: ReplicaID) -> None:
        """Called whenever a deployment replica is being stopped."""
        deployment_id = replica_id.deployment_id
        self._pending_replicas[deployment_id].pop(replica_id, None)
        self._launching_replicas[deployment_id].pop(replica_id, None)
        self._recovering_replicas[deployment_id].discard(replica_id)
        self._running_replicas[deployment_id].pop(replica_id, None)
        self._logged_placement_failures.discard(replica_id)

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

    def _best_fit_node(
        self,
        required_resources: RequestedResources,
        available_resources: Dict[str, AvailableNodeResources],
        tie_break_key: Optional[Callable[[str], Any]] = None,
    ) -> Optional[str]:
        return _best_fit_node(required_resources, available_resources, tie_break_key)

    @abstractmethod
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
            The name of replicas to stop for each deployment.
        """
        raise NotImplementedError

    def _schedule_replica(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        default_scheduling_strategy: str,
        target_node_id: Optional[str] = None,
        target_labels: Optional[LabelMatchExpressionsT] = None,
        required_labels: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Binds a replica to Ray Core.

        Strategies in priority order: reserved gang placement group, a new
        placement group (soft target node only), node affinity to
        `target_node_id`, soft node labels, then `default_scheduling_strategy`.

        Args:
            scheduling_request: A request to schedule a replica.
            default_scheduling_strategy: Strategy used when no target applies.
            target_node_id: Node to place the replica on with soft affinity.
            target_labels: Node labels to prefer with a soft constraint.
            required_labels: Label selector the filters demand, applied as a
                hard constraint. The replica then waits for the autoscaler
                rather than landing on a node that breaks a filter's rule.

        Returns:
            True if the replica was successfully scheduled, False otherwise.
        """

        replica_id = scheduling_request.replica_id
        deployment_id = replica_id.deployment_id
        placement_group = None
        required_labels = required_labels or {}

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
            if required_labels:
                bundle_label_selector = [
                    {**(selector or {}), **required_labels}
                    for selector in (bundle_label_selector or [{}])
                ]
            try:
                pg = self._create_placement_group_fn(
                    CreatePlacementGroupRequest(
                        bundles=scheduling_request.placement_group_bundles,
                        strategy=placement_group_strategy,
                        target_node_id=target_node_id,
                        name=scheduling_request.actor_options["name"],
                        bundle_label_selector=bundle_label_selector,
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
                placement_group=pg,
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
        if required_labels and not isinstance(
            scheduling_strategy, PlacementGroupSchedulingStrategy
        ):
            actor_options["label_selector"] = {
                **(actor_options.get("label_selector") or {}),
                **required_labels,
            }
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

    @abstractmethod
    def get_node_to_compact(
        self, allow_new_compaction: bool
    ) -> Optional[Tuple[str, float]]:
        """Returns a node ID to be compacted and a compaction deadlne."""
        raise NotImplementedError

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


class DefaultDeploymentScheduler(DeploymentScheduler):
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
        for upscale in upscales.values():
            for scheduling_request in upscale:
                replica_id = scheduling_request.replica_id
                self._pending_replicas[replica_id.deployment_id][
                    replica_id
                ] = scheduling_request

        if RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY:
            warnings.warn(
                "The environment variable 'RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY' "
                "is deprecated and will be removed in a v2.55.0 release. "
                "Please use 'RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY' instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        self._schedule_pending_replicas()

        return {
            downscale.deployment_id: self._get_replicas_to_stop(
                downscale.deployment_id,
                downscale.num_to_stop,
                gang_id_by_replica=downscale.gang_id_by_replica,
                replicas_by_gang_id=downscale.replicas_by_gang_id,
                gang_size=downscale.gang_size,
            )
            for downscale in downscales.values()
        }

    def _schedule_pending_replicas(self) -> None:
        """Filters, scores and binds every pending replica, largest first."""
        scheduling_requests = sorted(
            _flatten(self._pending_replicas).values(),
            key=lambda r: r.requested_resources,
            reverse=True,
        )
        if not scheduling_requests:
            return
        self._log_schedule_order(scheduling_requests)

        active_nodes = self._cluster_node_info_cache.get_active_node_ids()
        node_labels = {
            node_id: self._cluster_node_info_cache.get_node_labels(node_id)
            for node_id in active_nodes
        }
        available_resources_per_node = self._get_available_resources_per_node()
        node_to_assigned_replicas = self._get_node_to_running_replicas()
        nodes_by_deployment = self._get_active_nodes_by_deployment(active_nodes)
        constraints = self._profile.constraints

        for scheduling_request in scheduling_requests:
            deployment_id = scheduling_request.replica_id.deployment_id
            hosting_nodes = nodes_by_deployment[deployment_id]
            ctx = SchedulingContext(
                deployment_id=deployment_id,
                num_replicas=self._num_replicas(deployment_id),
                nodes_occupied_by_deployment=hosting_nodes,
                node_labels=node_labels,
            )
            active = self._applicable_constraints(constraints, scheduling_request)
            if scheduling_request.is_non_strict_pack_pg():
                # Ray places these bundles, so Serve cannot choose the node, but
                # the selector half of each rule still travels with the group.
                target_node = None
                _, required_labels = self._collect_rules(active, ctx)
            else:
                target_node, required_labels = self._select_node(
                    scheduling_request,
                    available_resources_per_node,
                    node_to_assigned_replicas,
                    ctx,
                    active,
                )
            succeeded = self._bind_replica(
                scheduling_request, target_node, required_labels
            )
            if not succeeded or target_node is None:
                continue

            if target_node in available_resources_per_node:
                available_resources_per_node[target_node] = (
                    available_resources_per_node[target_node]
                    - scheduling_request.requested_resources
                )
            node_to_assigned_replicas.setdefault(target_node, set()).add(
                scheduling_request.replica_id
            )
            hosting_nodes.add(target_node)

    def _log_schedule_order(
        self, scheduling_requests: List[ReplicaSchedulingRequest]
    ) -> None:
        order_log_key = tuple(r.replica_id for r in scheduling_requests)
        if order_log_key == self._last_schedule_order_log_key:
            return
        self._last_schedule_order_log_key = order_log_key
        if Resources.CUSTOM_PRIORITY:
            priority_desc = (
                f"{Resources.CUSTOM_PRIORITY} (then GPU, CPU, memory, other custom)"
            )
        else:
            priority_desc = (
                "GPU, CPU, memory, other custom "
                "(RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES unset)"
            )
        order_desc = ", ".join(
            f"{r.replica_id.deployment_id.name}"
            f"[{_format_resources_for_scheduling_log(r.requested_resources)}]"
            for r in scheduling_requests
        )
        logger.info(
            f"Scheduling {len(scheduling_requests)} pending replica(s) with "
            f"{type(self._profile.scorer).__name__}. Resource priority: {priority_desc}. "
            f"Schedule order (first scheduled first): {order_desc}."
        )

    def _get_active_nodes_by_deployment(
        self, active_nodes: Set[str]
    ) -> DefaultDict[DeploymentID, Set[str]]:
        nodes_by_deployment: DefaultDict[DeploymentID, Set[str]] = defaultdict(set)
        for deployment_id, replica_nodes in self._running_replicas.items():
            nodes_by_deployment[deployment_id].update(
                node_id for node_id in replica_nodes.values() if node_id in active_nodes
            )
        for deployment_id, launching_replicas in self._launching_replicas.items():
            for info in launching_replicas.values():
                if (
                    node_id := info.target_node_id
                ) is not None and node_id in active_nodes:
                    nodes_by_deployment[deployment_id].add(node_id)
        return nodes_by_deployment

    def _num_replicas(self, deployment_id: DeploymentID) -> int:
        return (
            len(self._pending_replicas[deployment_id])
            + len(self._launching_replicas[deployment_id])
            + len(self._recovering_replicas[deployment_id])
            + len(self._running_replicas[deployment_id])
        )

    def _applicable_constraints(
        self,
        constraints: List[SchedulingConstraint],
        scheduling_request: ReplicaSchedulingRequest,
    ) -> List[SchedulingConstraint]:
        active = []
        for constraint in constraints:
            if constraint.applies_to(scheduling_request):
                active.append(constraint)
                continue
            key = (
                scheduling_request.replica_id.deployment_id,
                type(constraint).__name__,
            )
            if key not in self._logged_skipped_rules:
                self._logged_skipped_rules.add(key)
                logger.info(
                    f"{key[1]} does not apply to replicas of {key[0]} and is "
                    "skipped for them."
                )
        return active

    @staticmethod
    def _collect_rules(
        constraints: List[SchedulingConstraint], ctx: SchedulingContext
    ) -> Tuple[Set[str], Dict[str, str]]:
        """Unions every node exclusion and merges every other label selector."""
        nodes_to_avoid: Set[str] = set()
        required_labels: Dict[str, str] = {}
        for constraint in constraints:
            nodes_to_avoid |= constraint.nodes_to_avoid(ctx)
            for key, value in constraint.required_labels(ctx).items():
                if key == RAY_NODE_ID_LABEL or key in required_labels:
                    raise ValueError(
                        f"{type(constraint).__name__} sets label {key!r}, which "
                        "another rule in the same profile already sets. Use "
                        "nodes_to_avoid for node exclusions."
                    )
                required_labels[key] = value
        if nodes_to_avoid:
            required_labels[
                RAY_NODE_ID_LABEL
            ] = f"!in({','.join(sorted(nodes_to_avoid))})"
        return nodes_to_avoid, required_labels

    def _select_node(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        available_resources_per_node: Dict[str, AvailableNodeResources],
        node_to_assigned_replicas: Dict[str, Set[ReplicaID]],
        ctx: SchedulingContext,
        constraints: List[SchedulingConstraint],
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """Returns the chosen node, if any, and the labels the bind must carry.

        Every constraint runs both halves of its rule here, so the candidates
        the scorer sees and the selector Ray receives agree on which rules
        applied. Candidate narrowing never stops early for that reason.
        """
        nodes_to_avoid, required_labels = self._collect_rules(constraints, ctx)
        eligible = {
            node_id: resources
            for node_id, resources in available_resources_per_node.items()
            if node_id not in nodes_to_avoid
        }
        for constraint in constraints:
            eligible = constraint.eligible(eligible, ctx)

        tie_break_key = self._node_preference_key(ctx)
        for required_resources, label_selectors in self._build_placement_candidates(
            scheduling_request
        ):
            candidates = eligible
            for selector in label_selectors:
                candidates = LabelSelectorConstraint(selector).eligible(candidates, ctx)
            target_node = self._profile.scorer.choose(
                ctx.deployment_id,
                required_resources,
                candidates,
                node_to_assigned_replicas,
                tie_break_key,
            )
            if target_node:
                return target_node, required_labels
        return None, required_labels

    def _node_preference_key(
        self, ctx: SchedulingContext
    ) -> Optional[Callable[[str], Any]]:
        preferences = self._profile.preferences
        if not preferences:
            return None
        return lambda node_id: tuple(
            p.node_preference(node_id, ctx) for p in preferences
        )

    def _bind_replica(
        self,
        scheduling_request: ReplicaSchedulingRequest,
        target_node: Optional[str],
        required_labels: Dict[str, str],
    ) -> bool:
        replica_id = scheduling_request.replica_id
        resources_desc = _format_resources_for_scheduling_log(
            scheduling_request.requested_resources
        )
        if (
            target_node is None
            and not scheduling_request.is_non_strict_pack_pg()
            and replica_id not in self._logged_placement_failures
        ):
            self._logged_placement_failures.add(replica_id)
            labels_desc = f" satisfying {required_labels}" if required_labels else ""
            logger.info(
                f"Could not place {replica_id} ({resources_desc}): no node with "
                f"sufficient resources{labels_desc}. Falling back to "
                f"{self._profile.scorer.fallback_scheduling_strategy} scheduling."
            )

        succeeded = self._schedule_replica(
            scheduling_request,
            default_scheduling_strategy=self._profile.scorer.fallback_scheduling_strategy,
            target_node_id=target_node,
            required_labels=required_labels or None,
        )
        if target_node is None:
            return succeeded
        if succeeded:
            self._logged_placement_failures.discard(replica_id)
            logger.info(
                f"Scheduled {replica_id} ({resources_desc}) onto node {target_node}."
            )
        elif replica_id not in self._logged_placement_failures:
            self._logged_placement_failures.add(replica_id)
            logger.info(
                f"Failed to launch {replica_id} ({resources_desc}) on node "
                f"{target_node}."
            )
        return succeeded

    def _build_placement_candidates(
        self, scheduling_request: ReplicaSchedulingRequest
    ) -> List[Tuple[RequestedResources, List[Dict[str, str]]]]:
        """Returns (resources, label selectors) to try, primary first then fallbacks."""
        primary_labels: List[Dict[str, str]] = []
        if scheduling_request.placement_group_bundles:
            if scheduling_request.placement_group_bundle_label_selector:
                if scheduling_request.placement_group_strategy == "STRICT_PACK":
                    primary_labels = (
                        scheduling_request.placement_group_bundle_label_selector
                    )
                else:
                    raise NotImplementedError(
                        "Placement Group strategy 'PACK' with bundle_label_selector "
                        "is not yet supported in the Serve scheduler."
                    )
        elif "label_selector" in scheduling_request.actor_options:
            primary_labels = [scheduling_request.actor_options["label_selector"] or {}]

        placement_candidates = [
            (scheduling_request.requested_resources, primary_labels)
        ]

        if scheduling_request.placement_group_fallback_strategy:
            raise NotImplementedError(
                "Placement Group fallback strategies are not yet supported in the Serve scheduler."
            )
        for fallback in scheduling_request.actor_options.get("fallback_strategy") or []:
            placement_candidates.append(
                (
                    scheduling_request.requested_resources,
                    [fallback.get("label_selector", {}) or {}],
                )
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
        A replica is skipped if stopping it would leave the deployment on fewer
        nodes than its floor, so downscaling never undoes the spread.
        Note that this algorithm doesn't consider other non-serve actors on the same node.
        See more at https://github.com/ray-project/ray/issues/20599.

        For gang deployments, the same priority order is applied, but entire
        gangs are selected atomically instead of individual replicas.
        """
        replicas_priority: List[ReplicaID] = list(
            set().union(
                self._pending_replicas[deployment_id].keys(),
                self._launching_replicas[deployment_id].keys(),
                self._recovering_replicas[deployment_id],
            )
        )
        labels_to_check: List[Dict[str, str]] = []
        if label_selector := self._deployments[deployment_id].label_selector:
            labels_to_check.append(label_selector)
        elif bundle_label_selector := self._deployments[
            deployment_id
        ].bundle_label_selector:
            labels_to_check.extend(bundle_label_selector)

        running_nodes = self._running_replicas[deployment_id]
        newest_first_by_node: DefaultDict[str, List[ReplicaID]] = defaultdict(list)
        for replica_id, node_id in reversed(list(running_nodes.items())):
            newest_first_by_node[node_id].append(replica_id)

        ctx = DownscaleContext(
            deployment_id=deployment_id,
            target_num_replicas=len(replicas_priority)
            + len(running_nodes)
            - max_num_to_stop,
            node_by_replica=running_nodes,
            replicas_per_node=Counter(running_nodes.values()),
            node_labels={
                node_id: self._cluster_node_info_cache.get_node_labels(node_id)
                for node_id in set(running_nodes.values())
            },
        )
        preferences = self._profile.preferences

        def scale_down_priority(
            node_and_replicas: Tuple[str, Set[ReplicaID]],
        ) -> Tuple[Any, ...]:
            node_id, all_replicas = node_and_replicas
            node_labels = self._cluster_node_info_cache.get_node_labels(node_id)
            match_labels = not labels_to_check or any(
                node_labels_match_selector(node_labels, labels)
                for labels in labels_to_check
            )
            return (
                int(node_id == self._head_node_id),
                int(match_labels),
                len(all_replicas),
                *(p.stop_preference(node_id, ctx) for p in preferences),
            )

        for node_id, _ in sorted(
            self._get_node_to_running_replicas().items(), key=scale_down_priority
        ):
            replicas_priority.extend(newest_first_by_node.get(node_id, []))

        replicas_to_stop: Set[ReplicaID] = set()
        if gang_id_by_replica is not None:
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
            return replicas_to_stop

        for replica_id in replicas_priority:
            if not all(c.may_stop(replica_id, ctx) for c in self._profile.constraints):
                continue
            replicas_to_stop.add(replica_id)
            stopped_node_id = ctx.node_by_replica.get(replica_id)
            if stopped_node_id is not None:
                ctx.replicas_per_node[stopped_node_id] -= 1
                if ctx.replicas_per_node[stopped_node_id] == 0:
                    del ctx.replicas_per_node[stopped_node_id]
            if len(replicas_to_stop) == max_num_to_stop:
                break
        return replicas_to_stop

    def get_node_to_compact(
        self, allow_new_compaction: bool
    ) -> Optional[Tuple[str, float]]:
        return None
