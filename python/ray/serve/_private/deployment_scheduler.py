import copy
import logging
import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import total_ordering
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import ray
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import (
    CreatePlacementGroupRequest,
    DeploymentID,
    ReplicaID,
)
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.constants import (
    RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES,
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    SERVE_LOGGER_NAME,
)
from ray.util.scheduling_strategies import (
    LabelMatchExpressionsT,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class SpreadDeploymentSchedulingPolicy:
    """A scheduling policy that spreads replicas with best effort."""

    pass


@total_ordering
class Resources(dict):
    # Custom resource priority from environment variable
    CUSTOM_PRIORITY: List[str] = RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES
    EPSILON = 1e-9

    def get(self, key: str):
        val = super().get(key)
        if val is not None:
            return val

        # Implicit resources by default have 1 total
        if key.startswith(ray._raylet.IMPLICIT_RESOURCE_PREFIX):
            return 1

        # Otherwise by default there is 0 of this resource
        return 0

    def can_fit(self, other):
        keys = set(self.keys()) | set(other.keys())
        # We add a small epsilon to avoid floating point precision issues.
        return all(self.get(k) + self.EPSILON >= other.get(k) for k in keys)

    def __eq__(self, other):
        keys = set(self.keys()) | set(other.keys())
        return all([self.get(k) == other.get(k) for k in keys])

    def __add__(self, other):
        keys = set(self.keys()) | set(other.keys())

        kwargs = dict()
        for key in keys:
            if key.startswith(ray._raylet.IMPLICIT_RESOURCE_PREFIX):
                kwargs[key] = min(1.0, self.get(key) + other.get(key))
            else:
                kwargs[key] = self.get(key) + other.get(key)

        return Resources(kwargs)

    def __sub__(self, other):
        keys = set(self.keys()) | set(other.keys())
        kwargs = {key: self.get(key) - other.get(key) for key in keys}
        return Resources(kwargs)

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
    max_replicas_per_node: Optional[int] = None

    @property
    def required_resources(self) -> Resources:
        """The resources required to schedule this replica on a node.

        If this replica uses a strict pack placement group, the
        required resources is the sum of the placement group bundles.
        Otherwise, required resources is simply the actor resources.
        """

        if (
            self.placement_group_bundles is not None
            and self.placement_group_strategy == "STRICT_PACK"
        ):
            return sum(
                [Resources(bundle) for bundle in self.placement_group_bundles],
                Resources(),
            )
        else:
            required = Resources(self.actor_resources)

            # Using implicit resource (resources that every node
            # implicitly has and total is 1)
            # to limit the number of replicas on a single node.
            if self.max_replicas_per_node is not None:
                deployment_id = self.replica_id.deployment_id
                implicit_resource = (
                    f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}"
                    f"{deployment_id.app_name}:{deployment_id.name}"
                )
                required[implicit_resource] = 1.0 / self.max_replicas_per_node

            return required


@dataclass
class DeploymentDownscaleRequest:
    """Request to stop a certain number of replicas.

    The scheduler is responsible for
    choosing the replicas to stop.
    """

    deployment_id: DeploymentID
    num_to_stop: int


@dataclass
class DeploymentSchedulingInfo:
    deployment_id: DeploymentID
    scheduling_policy: Any
    actor_resources: Optional[Resources] = None
    placement_group_bundles: Optional[List[Resources]] = None
    placement_group_strategy: Optional[str] = None
    max_replicas_per_node: Optional[int] = None

    @property
    def required_resources(self) -> Resources:
        """The resources required to schedule a replica of this deployment on a node.

        If this replicas uses a strict pack placement group, the
        required resources is the sum of the placement group bundles.
        Otherwise, required resources is simply the actor resources.
        """

        if (
            self.placement_group_bundles is not None
            and self.placement_group_strategy == "STRICT_PACK"
        ):
            return sum(self.placement_group_bundles, Resources())
        else:
            required = self.actor_resources

            # Using implicit resource (resources that every node
            # implicitly has and total is 1)
            # to limit the number of replicas on a single node.
            if self.max_replicas_per_node:
                implicit_resource = (
                    f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}"
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
    deployment_to_replicas: Dict[DeploymentID, Dict[ReplicaID, Any]]
) -> Dict[ReplicaID, Any]:
    """Flattens a dict of {deployment_id: {replica_id: val}} to {replica_id: val}."""

    return {
        replica_id: val
        for replicas in deployment_to_replicas.values()
        for replica_id, val in replicas.items()
    }


class DeploymentScheduler(ABC):
    """A centralized scheduler for all Serve deployments.

    It makes a batch of scheduling decisions in each update cycle.
    """

    def __init__(
        self,
        cluster_node_info_cache: ClusterNodeInfoCache,
        head_node_id: str,
        create_placement_group_fn: Callable,
    ):
        # {deployment_id: scheduling_policy}
        self._deployments: Dict[DeploymentID, DeploymentSchedulingInfo] = {}
        # Replicas that are waiting to be scheduled.
        # {deployment_id: {replica_id: deployment_upscale_request}}
        self._pending_replicas: Dict[
            DeploymentID, Dict[str, ReplicaSchedulingRequest]
        ] = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors have been submitted.
        # {deployment_id: {replica_id: target_node_id}}
        self._launching_replicas: Dict[
            DeploymentID, Dict[str, LaunchingReplicaInfo]
        ] = defaultdict(dict)
        # Replicas that are recovering.
        # We don't know where those replicas are running.
        # {deployment_id: {replica_id}}
        self._recovering_replicas = defaultdict(set)
        # Replicas that are running.
        # We know where those replicas are running.
        # {deployment_id: {replica_id: running_node_id}}
        self._running_replicas = defaultdict(dict)

        self._cluster_node_info_cache = cluster_node_info_cache
        self._head_node_id = head_node_id
        self._create_placement_group_fn = create_placement_group_fn

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
        info.actor_resources = Resources(replica_config.resource_dict)
        info.max_replicas_per_node = replica_config.max_replicas_per_node
        if replica_config.placement_group_bundles:
            info.placement_group_bundles = [
                Resources(bundle) for bundle in replica_config.placement_group_bundles
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

        del self._deployments[deployment_id]

    def on_replica_stopping(self, replica_id: ReplicaID) -> None:
        """Called whenever a deployment replica is being stopped."""
        deployment_id = replica_id.deployment_id
        self._pending_replicas[deployment_id].pop(replica_id, None)
        self._launching_replicas[deployment_id].pop(replica_id, None)
        self._recovering_replicas[deployment_id].discard(replica_id)
        self._running_replicas[deployment_id].pop(replica_id, None)

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

    def _get_available_resources_per_node(self) -> Dict[str, Resources]:
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

        gcs_info = {node_id: Resources(r) for node_id, r in available_resources.items()}

        # Manually calculate available resources per node by subtracting
        # launching and running replicas from total resources
        total_minus_replicas = {
            node_id: Resources(resources)
            for node_id, resources in total_resources.items()
        }
        for deployment_id, replicas in self._launching_replicas.items():
            deployment = self._deployments[deployment_id]
            for info in replicas.values():
                target_node_id = info.target_node_id
                if not target_node_id or target_node_id not in total_minus_replicas:
                    continue

                total_minus_replicas[target_node_id] -= deployment.required_resources

        for deployment_id, replicas in self._running_replicas.items():
            deployment = self._deployments[deployment_id]
            for node_id in replicas.values():
                if node_id not in total_minus_replicas:
                    continue

                total_minus_replicas[node_id] -= deployment.required_resources

        def custom_min(a: Resources, b: Resources):
            keys = set(a.keys()) | set(b.keys())
            res = Resources()
            for key in keys:
                res[key] = min(a.get(key), b.get(key))
            return res

        # Filter by active node ids (alive but not draining)
        return {
            node_id: custom_min(
                gcs_info.get(node_id, Resources()),
                total_minus_replicas.get(node_id, Resources()),
            )
            for node_id in self._cluster_node_info_cache.get_active_node_ids()
        }

    def _best_fit_node(
        self, required_resources: Resources, available_resources: Dict[str, Resources]
    ) -> Optional[str]:
        """Chooses a node using best fit strategy.

        This strategy picks the node where, if the required resources
        were to be scheduled on that node, it will leave the smallest
        remaining space. This minimizes fragmentation of resources.
        """

        min_remaining_space = None
        chosen_node = None

        for node_id in available_resources:
            if not available_resources[node_id].can_fit(required_resources):
                continue

            # TODO(zcin): We can make this better by only considering
            # custom resources that required_resources has.
            remaining_space = available_resources[node_id] - required_resources
            if min_remaining_space is None or remaining_space < min_remaining_space:
                min_remaining_space = remaining_space
                chosen_node = node_id

        return chosen_node

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
    ):
        """Schedule a replica from a scheduling request.

        The following special scheduling strategies will be used, in
        order of highest to lowest priority.
        1. If a replica requires placement groups, we will choose to use
           a `PlacementGroupSchedulingStrategy`. This can also take a
           target node into consideration (soft target), if provided.
           However it cannot take into account target labels.
        2. If a `target_node_id` is provided, we will choose to use a
           `NodeAffinitySchedulingStrategy`.
        3. If `target_labels` is provided, we will choose to use a
           `NodeLabelSchedulingStrategy`.

        Args:
            scheduling_request: A request to schedule a replica.
            default_scheduling_strategy: The scheduling strategy to fall
                back to if no special scheduling strategy is necessary.
            target_node_id: Attempt to schedule this replica onto this
                target node.
            target_labels: Attempt to schedule this replica onto nodes
                with these target labels.
        """

        replica_id = scheduling_request.replica_id
        deployment_id = replica_id.deployment_id
        placement_group = None

        scheduling_strategy = default_scheduling_strategy
        if scheduling_request.placement_group_bundles is not None:
            placement_group_strategy = (
                scheduling_request.placement_group_strategy
                if scheduling_request.placement_group_strategy
                else "PACK"
            )
            try:
                pg = self._create_placement_group_fn(
                    CreatePlacementGroupRequest(
                        bundles=scheduling_request.placement_group_bundles,
                        strategy=placement_group_strategy,
                        target_node_id=target_node_id,
                        name=scheduling_request.actor_options["name"],
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
                return
            scheduling_strategy = PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_capture_child_tasks=True,
            )
            target_labels = None
        elif target_node_id is not None:
            scheduling_strategy = NodeAffinitySchedulingStrategy(
                node_id=target_node_id, soft=True, _spill_on_unavailable=True
            )
            target_labels = None
        elif target_labels is not None:
            scheduling_strategy = NodeLabelSchedulingStrategy(
                hard={}, soft=target_labels
            )
            target_node_id = None

        actor_options = copy.copy(scheduling_request.actor_options)
        if scheduling_request.max_replicas_per_node is not None:
            if "resources" not in actor_options:
                actor_options["resources"] = {}
            # Using implicit resource (resources that every node
            # implicitly has and total is 1)
            # to limit the number of replicas on a single node.
            actor_options["resources"][
                f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}"
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
            return

        del self._pending_replicas[deployment_id][replica_id]
        self._on_replica_launching(
            replica_id, target_node_id=target_node_id, target_labels=target_labels
        )

        if isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy):
            placement_group = scheduling_strategy.placement_group

        scheduling_request.status = ReplicaSchedulingRequestStatus.SUCCEEDED
        scheduling_request.on_scheduled(actor_handle, placement_group=placement_group)

    @abstractmethod
    def get_node_to_compact(
        self, allow_new_compaction: bool
    ) -> Optional[Tuple[str, float]]:
        """Returns a node ID to be compacted and a compaction deadlne."""
        raise NotImplementedError


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
                deployment_id = replica_id.deployment_id
                self._pending_replicas[deployment_id][replica_id] = scheduling_request

        non_strict_pack_pgs_exist = any(
            d.is_non_strict_pack_pg() for d in self._deployments.values()
        )
        # Schedule replicas using compact strategy.
        if RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY and not non_strict_pack_pgs_exist:
            # Flatten dict of deployment replicas into all replicas,
            # then sort by decreasing resource size
            all_scheduling_requests = sorted(
                _flatten(self._pending_replicas).values(),
                key=lambda r: r.required_resources,
                reverse=True,
            )

            # Schedule each replica
            for scheduling_request in all_scheduling_requests:
                target_node = self._find_best_available_node(
                    scheduling_request.required_resources,
                    self._get_available_resources_per_node(),
                )

                self._schedule_replica(
                    scheduling_request,
                    default_scheduling_strategy="DEFAULT",
                    target_node_id=target_node,
                )

        else:
            for pending_replicas in self._pending_replicas.values():
                if not pending_replicas:
                    continue

                for scheduling_request in list(pending_replicas.values()):
                    self._schedule_replica(
                        scheduling_request=scheduling_request,
                        default_scheduling_strategy="SPREAD",
                    )

        deployment_to_replicas_to_stop = {}
        for downscale in downscales.values():
            deployment_to_replicas_to_stop[
                downscale.deployment_id
            ] = self._get_replicas_to_stop(
                downscale.deployment_id, downscale.num_to_stop
            )

        return deployment_to_replicas_to_stop

    def _get_replicas_to_stop(
        self, deployment_id: DeploymentID, max_num_to_stop: int
    ) -> Set[ReplicaID]:
        """Prioritize replicas running on a node with fewest replicas of
            all deployments.

        This algorithm helps to scale down more intelligently because it can
        relinquish nodes faster. Note that this algorithm doesn't consider
        other non-serve actors on the same node. See more at
        https://github.com/ray-project/ray/issues/20599.
        """
        replicas_to_stop = set()

        # Replicas not in running state don't have node id.
        # We will prioritize those first.
        pending_launching_recovering_replicas = set().union(
            self._pending_replicas[deployment_id].keys(),
            self._launching_replicas[deployment_id].keys(),
            self._recovering_replicas[deployment_id],
        )
        for (
            pending_launching_recovering_replica
        ) in pending_launching_recovering_replicas:
            if len(replicas_to_stop) == max_num_to_stop:
                return replicas_to_stop
            else:
                replicas_to_stop.add(pending_launching_recovering_replica)

        node_to_running_replicas_of_target_deployment = (
            self._get_node_to_running_replicas(deployment_id)
        )
        node_to_running_replicas_of_all_deployments = (
            self._get_node_to_running_replicas()
        )

        # Replicas on the head node has the lowest priority for downscaling
        # since we cannot relinquish the head node.
        def key(node_and_num_running_replicas_of_all_deployments):
            return (
                len(node_and_num_running_replicas_of_all_deployments[1])
                if node_and_num_running_replicas_of_all_deployments[0]
                != self._head_node_id
                else sys.maxsize
            )

        for node_id, _ in sorted(
            node_to_running_replicas_of_all_deployments.items(), key=key
        ):
            if node_id not in node_to_running_replicas_of_target_deployment:
                continue
            for running_replica in node_to_running_replicas_of_target_deployment[
                node_id
            ]:
                if len(replicas_to_stop) == max_num_to_stop:
                    return replicas_to_stop
                else:
                    replicas_to_stop.add(running_replica)

        return replicas_to_stop

    def _find_best_available_node(
        self,
        required_resources: Resources,
        available_resources_per_node: Dict[str, Resources],
    ) -> Optional[str]:
        """Chooses best available node to schedule the required resources.

        If there are available nodes, returns the node ID of the best
        available node, minimizing fragmentation. Prefers non-idle nodes
        over idle nodes.
        """

        node_to_running_replicas = self._get_node_to_running_replicas()

        non_idle_nodes = {
            node_id: res
            for node_id, res in available_resources_per_node.items()
            if len(node_to_running_replicas.get(node_id, set())) > 0
        }
        idle_nodes = {
            node_id: res
            for node_id, res in available_resources_per_node.items()
            if len(node_to_running_replicas.get(node_id, set())) == 0
        }

        # 1. Prefer non-idle nodes
        chosen_node = self._best_fit_node(required_resources, non_idle_nodes)
        if chosen_node:
            return chosen_node

        # 2. Consider idle nodes last
        chosen_node = self._best_fit_node(required_resources, idle_nodes)
        if chosen_node:
            return chosen_node

    def get_node_to_compact(
        self, allow_new_compaction: bool
    ) -> Optional[Tuple[str, float]]:
        return None
