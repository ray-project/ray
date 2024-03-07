# Copyright (2023 and onwards) Anyscale, Inc.

import sys
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set, Tuple

import ray
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentScheduler,
    DeploymentSchedulingInfo,
    LaunchingReplicaInfo,
    ReplicaSchedulingRequest,
    Resources,
)
from ray.serve._private.utils import get_head_node_id
from ray.util.scheduling_strategies import In


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
        create_placement_group_fn: Callable = None,
    ):
        # {deployment_id: scheduling_policy}
        self._deployments: Dict[DeploymentID, DeploymentSchedulingInfo] = {}
        # Replicas that are waiting to be scheduled.
        # {deployment_id: {replica_name: deployment_upscale_request}}
        self._pending_replicas: Dict[
            DeploymentID, Dict[str, ReplicaSchedulingRequest]
        ] = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors have been submitted.
        # {deployment_id: {replica_name, az}}
        self._launching_replicas: Dict[
            DeploymentID, Dict[str, LaunchingReplicaInfo]
        ] = defaultdict(dict)
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
        self._create_placement_group_fn = (
            create_placement_group_fn or ray.util.placement_group
        )

    def schedule(
        self,
        upscales: Dict[DeploymentID, List[ReplicaSchedulingRequest]],
        downscales: Dict[DeploymentID, DeploymentDownscaleRequest],
    ) -> Dict[DeploymentID, Set[ReplicaID]]:
        # Schedule spread deployments.
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
                [
                    scheduling_request
                    for replicas in self._pending_replicas.values()
                    for scheduling_request in replicas.values()
                ],
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

                az_to_num_replicas = self._calculate_az_to_num_replicas_mapping(
                    deployment_id
                )
                for scheduling_request in list(pending_replicas.values()):
                    target_labels = None
                    if az_to_num_replicas:
                        # Prefer the AZ with fewest replicas.
                        az, _ = min(az_to_num_replicas.items(), key=lambda r: r[1])
                        # Use soft node label scheduling strategy so that if the AZ has
                        # available resources, the replica will run there. Otherwise, it
                        # will run in other AZs that have available resources, and a new
                        # node will only upscale when there are no available resources
                        # in the entire cluster.
                        target_labels = {
                            ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL: In(az)
                        }
                        az_to_num_replicas[az] = az_to_num_replicas[az] + 1

                    self._schedule_replica(
                        scheduling_request=scheduling_request,
                        default_scheduling_strategy="SPREAD",
                        target_labels=target_labels,
                    )

        deployment_to_replicas_to_stop = {}
        for downscale in downscales.values():
            deployment_id = downscale.deployment_id
            deployment_to_replicas_to_stop[deployment_id] = self._get_replicas_to_stop(
                deployment_id, downscale.num_to_stop
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
        for _, info in self._launching_replicas[deployment_id].items():
            if info.target_labels is None:
                continue

            az = info.target_labels.get(ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL)
            if az is not None:
                az_to_num_replicas[az] = az_to_num_replicas.get(az, 0) + 1
        return az_to_num_replicas

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
            if len(node_to_running_replicas[node_id]) > 0
        }
        idle_nodes = {
            node_id: res
            for node_id, res in available_resources_per_node.items()
            if len(node_to_running_replicas[node_id]) == 0
        }

        # 1. Prefer non-idle nodes
        chosen_node = self._best_fit_node(required_resources, non_idle_nodes)
        if chosen_node:
            return chosen_node

        # 2. Consider idle nodes last
        chosen_node = self._best_fit_node(required_resources, idle_nodes)
        if chosen_node:
            return chosen_node

    def detect_compact_opportunities(self) -> Optional[Tuple[str, float]]:
        return None, None
