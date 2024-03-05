# Copyright (2023 and onwards) Anyscale, Inc.

import copy
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import ray
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import DeploymentID
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentScheduler,
    ReplicaSchedulingRequest,
    SpreadDeploymentSchedulingPolicy,
)
from ray.serve._private.utils import get_head_node_id
from ray.util.scheduling_strategies import (
    In,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


class AnyscaleDeploymentScheduler(DeploymentScheduler):
    """AZ-aware deployment scheduler.

    Upscaling:
        Deployment replicas have both hard and soft scheduling constraints.
        Hard constraints are resource constraint and max_replicas_per_node constraint.
        Soft constraints are spreading across AZs and
        spreading across nodes within an AZ.
        The scheduler guarantees that all the hard constraints are satisfied
        and tries to satisfy soft constraints as well but those are not guaranteed.
        The scheduler also prioritizes saving cost over satisfying soft constraints
        meaning that it avoids adding new nodes during scheduling as long as
        hard constraints can be satisfied.

        Currently AZ-aware spread scheduling doesn't support deployments
        with placement groups meaning that the scheduler won't try to spread
        those replicas across AZs.

    Downscaling:
        The scheduler first chooses replicas that are not in the RUNNING state,
        then it chooses replicas on nodes with fewest replicas of all kinds with
        the intention to free the node.
        If there is a tie, it prefers nodes in AZs with most replicas
        of the target deployment.
    """

    def __init__(
        self,
        cluster_node_info_cache: ClusterNodeInfoCache,
        head_node_id: Optional[str] = None,
    ):
        # {deployment_id: scheduling_policy}
        self._deployments = {}
        # Replicas that are waiting to be scheduled.
        # {deployment_id: {replica_name: deployment_upscale_request}}
        self._pending_replicas = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors have been submitted.
        # {deployment_id: {replica_name, az}}
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

        self._head_node_id = head_node_id or get_head_node_id()

    def schedule(
        self,
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]],
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest],
    ) -> Dict[DeploymentID, Set[str]]:
        # Schedule spread deployments.
        for upscale in upscales.values():
            for replica_scheduling_request in upscale:
                self._pending_replicas[replica_scheduling_request.deployment_id][
                    replica_scheduling_request.replica_name
                ] = replica_scheduling_request

        for deployment_id, pending_replicas in self._pending_replicas.items():
            if not pending_replicas:
                continue

            assert isinstance(
                self._deployments[deployment_id].scheduling_policy,
                SpreadDeploymentSchedulingPolicy,
            )
            self._schedule_spread_deployment(deployment_id)

        deployment_to_replicas_to_stop = {}
        for downscale in downscales.values():
            deployment_to_replicas_to_stop[
                downscale.deployment_id
            ] = self._get_replicas_to_stop(
                downscale.deployment_id, downscale.num_to_stop
            )

        return deployment_to_replicas_to_stop

    def _calculate_az_to_num_replicas_mapping(
        self, deployment_id: DeploymentID
    ) -> Dict[str, int]:
        """Calculate the mapping from az to the number of replicas of the
        target deployment running in that az.

        If there is no az information, this returns an empty dict.
        """
        az_to_num_replicas = {}
        for node_id in self._cluster_node_info_cache.get_active_node_ids():
            az = self._cluster_node_info_cache.get_node_az(node_id)
            if az is not None:
                az_to_num_replicas[az] = 0
        for _, node_id in self._running_replicas[deployment_id].items():
            az = self._cluster_node_info_cache.get_node_az(node_id)
            if az is not None:
                az_to_num_replicas[az] = az_to_num_replicas.get(az, 0) + 1
        for _, az in self._launching_replicas[deployment_id].items():
            if az is not None:
                az_to_num_replicas[az] = az_to_num_replicas.get(az, 0) + 1
        return az_to_num_replicas

    def _schedule_spread_deployment(self, deployment_id: DeploymentID):
        """A simple implementation of trying to spread replicas across AZs
           and across nodes within an AZ.

        Spreading across AZs is best effort and assumes that nodes are
        spreaded across AZs already.

        In the future, we can evolve this scheduling algorithm to be more smart
        and consider factors like colocation.
        """
        az_to_num_replicas = self._calculate_az_to_num_replicas_mapping(deployment_id)

        for pending_replica_name in list(self._pending_replicas[deployment_id].keys()):
            replica_scheduling_request = self._pending_replicas[deployment_id][
                pending_replica_name
            ]

            placement_group = None
            az = None
            if replica_scheduling_request.placement_group_bundles is not None:
                # PG is currently not AZ-aware.
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
            elif az_to_num_replicas:
                # Prefer the AZ with fewest replicas.
                az, _ = min(
                    az_to_num_replicas.items(),
                    key=lambda az_and_num_running_replicas: az_and_num_running_replicas[
                        1
                    ],
                )
                # Use soft node label scheduling strategy
                # so that if the specified AZ has available resources,
                # the replica will run there.
                # Otherwise, it will run in other AZs that have available resources.
                # It will only upscales a new node when
                # there are no available resources in the entire cluster.
                scheduling_strategy = NodeLabelSchedulingStrategy(
                    hard={}, soft={ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: In(az)}
                )
                az_to_num_replicas[az] = az_to_num_replicas[az] + 1
            else:
                # No AZ, fallback to SPREAD scheduling.
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
            self._launching_replicas[deployment_id][pending_replica_name] = az
            replica_scheduling_request.on_scheduled(
                actor_handle, placement_group=placement_group
            )

    def _get_replicas_to_stop(
        self, deployment_id: DeploymentID, max_num_to_stop: int
    ) -> Set[str]:
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
        az_to_num_running_replicas_of_target_deployment = {}
        for running_replica, node_id in self._running_replicas[deployment_id].items():
            node_to_running_replicas_of_target_deployment[node_id].add(running_replica)
            az = self._cluster_node_info_cache.get_node_az(node_id)
            az_to_num_running_replicas_of_target_deployment[az] = (
                az_to_num_running_replicas_of_target_deployment.get(az, 0) + 1
            )

        node_to_num_running_replicas_of_all_deployments = {}
        for _, running_replicas in self._running_replicas.items():
            for running_replica, node_id in running_replicas.items():
                node_to_num_running_replicas_of_all_deployments[node_id] = (
                    node_to_num_running_replicas_of_all_deployments.get(node_id, 0) + 1
                )

        nodes_with_running_replicas_of_target_deployment = list(
            node_to_running_replicas_of_target_deployment.keys()
        )

        # Overall we prefer nodes with fewest replicas of all deployments.
        # When there is a tie, we prefer nodes in AZs with most replicas
        # of the target deployment.

        # Prefer nodes in AZs with most replicas of the target deployment.
        def key(node_id):
            return az_to_num_running_replicas_of_target_deployment[
                self._cluster_node_info_cache.get_node_az(node_id)
            ]

        nodes_with_running_replicas_of_target_deployment.sort(key=key, reverse=True)

        # Prefer nodes with fewest replicas of all deployments.
        # Replicas on the head node has the lowest priority for downscaling
        # since we cannot relinquish the head node.
        def key(node_id):
            return (
                node_to_num_running_replicas_of_all_deployments[node_id]
                if node_id != self._head_node_id
                else sys.maxsize
            )

        nodes_with_running_replicas_of_target_deployment.sort(key=key, reverse=False)

        for node_id in nodes_with_running_replicas_of_target_deployment:
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
