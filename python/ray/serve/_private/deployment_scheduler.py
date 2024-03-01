import copy
import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from functools import total_ordering
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import ray
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import ReplicaConfig
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


class SpreadDeploymentSchedulingPolicy:
    """A scheduling policy that spreads replicas with best effort."""

    pass


@total_ordering
class Resources(dict):
    @classmethod
    def from_ray_resource_dict(cls, ray_resource_dict: Dict):
        num_cpus = ray_resource_dict.get("CPU", 0)
        num_gpus = ray_resource_dict.get("GPU", 0)
        memory = ray_resource_dict.get("memory", 0)
        custom_resources = ray_resource_dict.get("resources", dict())

        return cls(CPU=num_cpus, GPU=num_gpus, memory=memory, **custom_resources)

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
        return all(self.get(k) >= other.get(k) for k in keys)

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
        1. GPU
        2. CPU
        3. memory
        4. custom resources
        This means a resource with a larger number of GPUs is always
        sorted higher than a resource with a smaller number of GPUs,
        regardless of the values of the other resource types. Similarly
        for CPU next, memory next, etc.
        """

        keys = set(self.keys()) | set(other.keys())
        keys = keys - {"GPU", "CPU", "memory"}

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

        for key in keys:
            if self.get(key) < other.get(key):
                return True
            elif self.get(key) > other.get(key):
                return False

        return False


@dataclass
class ReplicaSchedulingRequest:
    """Request to schedule a single replica.

    The scheduler is responsible for scheduling
    based on the deployment scheduling policy.
    """

    deployment_id: DeploymentID
    replica_name: str
    actor_def: ray.actor.ActorClass
    actor_resources: Dict
    actor_options: Dict
    actor_init_args: Tuple
    on_scheduled: Callable
    # Placement group bundles and strategy *for this replica*.
    # These are optional: by default replicas do not have a placement group.
    placement_group_bundles: Optional[List[Dict[str, float]]] = None
    placement_group_strategy: Optional[str] = None
    max_replicas_per_node: Optional[int] = None


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
    scheduling_policy: Any
    actor_resources: Optional[Resources] = None
    placement_group_bundles: Optional[List[Resources]] = None
    placement_group_strategy: Optional[str] = None

    @property
    def required_resources(self) -> Resources:
        if (
            self.placement_group_bundles is not None
            and self.placement_group_strategy == "STRICT_PACK"
        ):
            return sum(self.placement_group_bundles, Resources())
        else:
            return self.actor_resources

    def is_non_strict_pack_pg(self) -> bool:
        return (
            self.placement_group_bundles is not None
            and self.placement_group_strategy != "STRICT_PACK"
        )


class DeploymentScheduler(ABC):
    """A centralized scheduler for all Serve deployments.

    It makes a batch of scheduling decisions in each update cycle.
    """

    def __init__(
        self,
        cluster_node_info_cache: ClusterNodeInfoCache,
        head_node_id: str,
    ):
        # {deployment_id: scheduling_policy}
        self._deployments: Dict[DeploymentID, DeploymentSchedulingInfo] = {}
        # Replicas that are waiting to be scheduled.
        # {deployment_id: {replica_name: deployment_upscale_request}}
        self._pending_replicas = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors have been submitted.
        # {deployment_id: {replica_name: target_node_id}}
        self._launching_replicas = defaultdict(dict)
        # Replicas that are recovering.
        # We don't know where those replicas are running.
        # {deployment_id: {replica_name}}
        self._recovering_replicas = defaultdict(set)
        # Replicas that are running.
        # We know where those replicas are running.
        # {deployment_id: {replica_name: running_node_id}}
        self._running_replicas = defaultdict(dict)

        self._cluster_node_info_cache = cluster_node_info_cache

        self._head_node_id = head_node_id

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
            scheduling_policy=scheduling_policy
        )

    def on_deployment_deployed(
        self,
        deployment_id: DeploymentID,
        replica_config: ReplicaConfig,
    ) -> None:
        assert deployment_id in self._deployments

        self._deployments[
            deployment_id
        ].actor_resources = Resources.from_ray_resource_dict(
            replica_config.resource_dict
        )
        if replica_config.placement_group_bundles:
            self._deployments[deployment_id].placement_group_bundles = [
                Resources.from_ray_resource_dict(bundle)
                for bundle in replica_config.placement_group_bundles
            ]
        if replica_config.placement_group_strategy:
            self._deployments[
                deployment_id
            ].placement_group_strategy = replica_config.placement_group_strategy

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

    def on_replica_stopping(
        self, deployment_id: DeploymentID, replica_name: str
    ) -> None:
        """Called whenever a deployment replica is being stopped."""
        self._pending_replicas[deployment_id].pop(replica_name, None)
        self._launching_replicas[deployment_id].pop(replica_name, None)
        self._recovering_replicas[deployment_id].discard(replica_name)
        self._running_replicas[deployment_id].pop(replica_name, None)

    def on_replica_running(
        self, deployment_id: DeploymentID, replica_name: str, node_id: str
    ) -> None:
        """Called whenever a deployment replica is running with a known node id."""
        assert replica_name not in self._pending_replicas[deployment_id]

        self._launching_replicas[deployment_id].pop(replica_name, None)
        self._recovering_replicas[deployment_id].discard(replica_name)

        self._running_replicas[deployment_id][replica_name] = node_id

    def on_replica_recovering(
        self, deployment_id: DeploymentID, replica_name: str
    ) -> None:
        """Called whenever a deployment replica is recovering."""
        assert replica_name not in self._pending_replicas[deployment_id]
        assert replica_name not in self._launching_replicas[deployment_id]
        assert replica_name not in self._running_replicas[deployment_id]
        assert replica_name not in self._recovering_replicas[deployment_id]

        self._recovering_replicas[deployment_id].add(replica_name)

    def _get_node_to_running_replicas(
        self, deployment_id: Optional[DeploymentID] = None
    ) -> Dict[str, Set[str]]:
        res = defaultdict(set)
        if deployment_id:
            for replica_id, node_id in self._running_replicas[deployment_id].items():
                res[node_id].add(replica_id)
        else:
            for _, replicas in self._running_replicas.items():
                for replica_id, node_id in replicas.items():
                    res[node_id].add(replica_id)

        return res

    @abstractmethod
    def schedule(
        self,
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]],
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest],
    ) -> Dict[DeploymentID, Set[str]]:
        """Called for each update cycle to do batch scheduling.

        Args:
            upscales: a dict of deployment name to a list of replicas to schedule.
            downscales: a dict of deployment name to a downscale request.

        Returns:
            The name of replicas to stop for each deployment.
        """
        raise NotImplementedError

    @abstractmethod
    def detect_compact_opportunities(self) -> Tuple[str, float]:
        """Returns a node ID to be compacted and a compaction deadlne."""
        raise NotImplementedError


class DefaultDeploymentScheduler(DeploymentScheduler):
    def schedule(
        self,
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]],
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest],
    ) -> Dict[DeploymentID, Set[str]]:
        """Called for each update cycle to do batch scheduling.

        Args:
            upscales: a dict of deployment name to a list of replicas to schedule.
            downscales: a dict of deployment name to a downscale request.

        Returns:
            The name of replicas to stop for each deployment.
        """
        for upscale in upscales.values():
            for replica_scheduling_request in upscale:
                self._pending_replicas[replica_scheduling_request.deployment_id][
                    replica_scheduling_request.replica_name
                ] = replica_scheduling_request

        for deployment_id, pending_replicas in self._pending_replicas.items():
            if not pending_replicas:
                continue

            self._schedule_spread_deployment(deployment_id)

        deployment_to_replicas_to_stop = {}
        for downscale in downscales.values():
            deployment_to_replicas_to_stop[
                downscale.deployment_id
            ] = self._get_replicas_to_stop(
                downscale.deployment_id, downscale.num_to_stop
            )

        return deployment_to_replicas_to_stop

    def _schedule_spread_deployment(self, deployment_id: DeploymentID) -> None:
        for pending_replica_name in list(self._pending_replicas[deployment_id].keys()):
            replica_scheduling_request = self._pending_replicas[deployment_id][
                pending_replica_name
            ]

            placement_group = None
            if replica_scheduling_request.placement_group_bundles is not None:
                strategy = (
                    replica_scheduling_request.placement_group_strategy
                    if replica_scheduling_request.placement_group_strategy
                    else "PACK"
                )
                placement_group = ray.util.placement_group(
                    replica_scheduling_request.placement_group_bundles,
                    strategy=strategy,
                    lifetime="detached",
                    name=replica_scheduling_request.actor_options["name"],
                )
                scheduling_strategy = PlacementGroupSchedulingStrategy(
                    placement_group=placement_group,
                    placement_group_capture_child_tasks=True,
                )
            else:
                scheduling_strategy = "SPREAD"

            actor_options = copy.copy(replica_scheduling_request.actor_options)
            if replica_scheduling_request.max_replicas_per_node is not None:
                if "resources" not in actor_options:
                    actor_options["resources"] = {}
                # Using implicit resource (resources that every node
                # implicitly has and total is 1)
                # to limit the number of replicas on a single node.
                actor_options["resources"][
                    f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}"
                    f"{deployment_id.app_name}:{deployment_id.name}"
                ] = (1.0 / replica_scheduling_request.max_replicas_per_node)
            actor_handle = replica_scheduling_request.actor_def.options(
                scheduling_strategy=scheduling_strategy,
                **actor_options,
            ).remote(*replica_scheduling_request.actor_init_args)

            del self._pending_replicas[deployment_id][pending_replica_name]
            self._launching_replicas[deployment_id][pending_replica_name] = None
            replica_scheduling_request.on_scheduled(
                actor_handle, placement_group=placement_group
            )

    def _get_replicas_to_stop(
        self, deployment_id: DeploymentID, max_num_to_stop: int
    ) -> Set[str]:
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

    def detect_compact_opportunities(self) -> Tuple[str, float]:
        return None, None
