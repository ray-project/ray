import sys
import copy
from typing import Callable, Dict, Tuple, List, Optional, Union, Set
from dataclasses import dataclass
from collections import defaultdict
from abc import ABC, abstractmethod

import ray
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

from ray.serve._private.common import DeploymentID
from ray.serve._private.utils import (
    get_head_node_id,
)
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache


class SpreadDeploymentSchedulingPolicy:
    """A scheduling policy that spreads replicas with best effort."""

    pass


class DriverDeploymentSchedulingPolicy:
    """A scheduling policy that schedules exactly one replica on each node."""

    pass


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


class DeploymentScheduler(ABC):
    """A centralized scheduler for all Serve deployments.

    It makes a batch of scheduling decisions in each update cycle.
    """

    @abstractmethod
    def on_deployment_created(
        self,
        deployment_id: DeploymentID,
        scheduling_policy: Union[
            SpreadDeploymentSchedulingPolicy, DriverDeploymentSchedulingPolicy
        ],
    ) -> None:
        """Called whenever a new deployment is created."""
        raise NotImplementedError

    @abstractmethod
    def on_deployment_deleted(self, deployment_id: DeploymentID) -> None:
        """Called whenever a deployment is deleted."""
        raise NotImplementedError

    @abstractmethod
    def on_replica_stopping(
        self, deployment_id: DeploymentID, replica_name: str
    ) -> None:
        """Called whenever a deployment replica is being stopped."""
        raise NotImplementedError

    @abstractmethod
    def on_replica_running(
        self, deployment_id: DeploymentID, replica_name: str, node_id: str
    ) -> None:
        """Called whenever a deployment replica is running with a known node id."""
        raise NotImplementedError

    @abstractmethod
    def on_replica_recovering(
        self, deployment_id: DeploymentID, replica_name: str
    ) -> None:
        """Called whenever a deployment replica is recovering."""
        raise NotImplementedError

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


class DefaultDeploymentScheduler(DeploymentScheduler):
    def __init__(self, cluster_node_info_cache: ClusterNodeInfoCache):
        # {deployment_id: scheduling_policy}
        self._deployments = {}
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

        self._head_node_id = get_head_node_id()

    def on_deployment_created(
        self,
        deployment_id: DeploymentID,
        scheduling_policy: Union[
            SpreadDeploymentSchedulingPolicy, DriverDeploymentSchedulingPolicy
        ],
    ) -> None:
        """Called whenever a new deployment is created."""
        assert deployment_id not in self._pending_replicas
        assert deployment_id not in self._launching_replicas
        assert deployment_id not in self._recovering_replicas
        assert deployment_id not in self._running_replicas
        self._deployments[deployment_id] = scheduling_policy

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

            deployment_scheduling_policy = self._deployments[deployment_id]
            if isinstance(
                deployment_scheduling_policy, SpreadDeploymentSchedulingPolicy
            ):
                self._schedule_spread_deployment(deployment_id)
            else:
                assert isinstance(
                    deployment_scheduling_policy, DriverDeploymentSchedulingPolicy
                )
                self._schedule_driver_deployment(deployment_id)

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
                    f"{deployment_id.app}:{deployment_id.name}"
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

    def _schedule_driver_deployment(self, deployment_id: DeploymentID) -> None:
        if self._recovering_replicas[deployment_id]:
            # Wait until recovering is done before scheduling new replicas
            # so that we can make sure we don't schedule two replicas on the same node.
            return

        all_active_nodes = self._cluster_node_info_cache.get_active_node_ids()
        scheduled_nodes = set()
        for node_id in self._launching_replicas[deployment_id].values():
            assert node_id is not None
            scheduled_nodes.add(node_id)
        for node_id in self._running_replicas[deployment_id].values():
            assert node_id is not None
            scheduled_nodes.add(node_id)
        unscheduled_nodes = all_active_nodes - scheduled_nodes

        for pending_replica_name in list(self._pending_replicas[deployment_id].keys()):
            if not unscheduled_nodes:
                return

            replica_scheduling_request = self._pending_replicas[deployment_id][
                pending_replica_name
            ]

            target_node_id = unscheduled_nodes.pop()
            actor_handle = replica_scheduling_request.actor_def.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    target_node_id, soft=False
                ),
                **replica_scheduling_request.actor_options,
            ).remote(*replica_scheduling_request.actor_init_args)
            del self._pending_replicas[deployment_id][pending_replica_name]
            self._launching_replicas[deployment_id][
                pending_replica_name
            ] = target_node_id
            replica_scheduling_request.on_scheduled(actor_handle, placement_group=None)

    def _get_replicas_to_stop(
        self, deployment_id: DeploymentID, max_num_to_stop: int
    ) -> Set[str]:
        """Prioritize replicas running on a node with fewest replicas of all deployments.

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

        node_to_running_replicas_of_target_deployment = defaultdict(set)
        for running_replica, node_id in self._running_replicas[deployment_id].items():
            node_to_running_replicas_of_target_deployment[node_id].add(running_replica)

        node_to_num_running_replicas_of_all_deployments = {}
        for _, running_replicas in self._running_replicas.items():
            for running_replica, node_id in running_replicas.items():
                node_to_num_running_replicas_of_all_deployments[node_id] = (
                    node_to_num_running_replicas_of_all_deployments.get(node_id, 0) + 1
                )

        # Replicas on the head node has the lowest priority for downscaling
        # since we cannot relinquish the head node.
        def key(node_and_num_running_replicas_of_all_deployments):
            return (
                node_and_num_running_replicas_of_all_deployments[1]
                if node_and_num_running_replicas_of_all_deployments[0]
                != self._head_node_id
                else sys.maxsize
            )

        for node_id, _ in sorted(
            node_to_num_running_replicas_of_all_deployments.items(), key=key
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
