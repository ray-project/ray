# Copyright (2023 and onwards) Anyscale, Inc.

import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple

from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.anyscale.serve._private.constants import (
    ANYSCALE_RAY_SERVE_COMPACTION_TIMEOUT_S,
)
from ray.serve import metrics
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import (
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_scheduler import (
    DeploymentDownscaleRequest,
    DeploymentScheduler,
    ReplicaSchedulingRequest,
    Resources,
)
from ray.serve._private.usage import ServeUsageTag
from ray.util.scheduling_strategies import In

logger = logging.getLogger(SERVE_LOGGER_NAME)

MAX_BACKOFF_TIME_S = int(
    os.environ.get("ANYSCALE_RAY_SERVE_COMPACTION_MAX_BACKOFF_TIME_S", 3600)
)


@dataclass
class CompactingNodeInfo:
    """Describes an in-progress active compaction.

    Args:
        target_node_id: The target node to compact away. This node can
            become idle by moving all its replicas onto other available
            nodes in the cluster.
        start_timestamp_s: The time at which the compaction opportunity
            was identified.
        cached_running_replicas_on_target_node: The set of replicas that
            were running on the target compact node when the compaction
            opportunity was identified.
    """

    target_node_id: str
    start_timestamp_s: float
    cached_running_replicas_on_target_node: Set[ReplicaID]
    last_logged_timeout_warning: Optional[float] = None


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
        super().__init__(
            cluster_node_info_cache=cluster_node_info_cache,
            head_node_id=head_node_id,
            create_placement_group_fn=create_placement_group_fn,
        )

        # Contains info on the current in-progress compaction.
        # NOTE(zcin): If there is an in-progress compaction and the
        # controller dies, this state will be lost and the controller
        # will not continue the same compaction when it recovers.
        # Instead it will recover PENDING_MIGRATION replicas as RUNNING,
        # reconcile the deployments towards a healthy state, then
        # re-identify new compaction opportunities.
        self._compacting_node: Optional[CompactingNodeInfo] = None
        # The number of consecutive failed compaction attempts since
        # the last successful compaction
        self._num_consecutive_failed_compactions: int = 0
        self._next_allowed_compaction_timestamp_s: float = 0

        self._num_succeeded_compactions: int = 0
        self._num_compacted_nodes_counter = metrics.Counter(
            "serve_num_compacted_nodes",
            description="The number of nodes that have been compacted.",
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
            for deployment_id, pending_replicas in self._pending_replicas.items():
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

            az_label = info.target_labels.get(ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL)
            if az_label is not None:
                assert isinstance(az_label, In)
                az = az_label.values[0]
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

        target_compact = None
        if self._compacting_node:
            target_compact = self._compacting_node.target_node_id

        non_idle_nodes = {
            node_id: res
            for node_id, res in available_resources_per_node.items()
            if len(node_to_running_replicas[node_id]) > 0 and node_id != target_compact
        }
        idle_nodes = {
            node_id: res
            for node_id, res in available_resources_per_node.items()
            if len(node_to_running_replicas[node_id]) == 0 and node_id != target_compact
        }

        # 1. Prefer non-idle nodes
        chosen_node = self._best_fit_node(required_resources, non_idle_nodes)
        if chosen_node:
            return chosen_node

        # 2. Prefer target node being compacted
        if target_compact:
            chosen_node = self._best_fit_node(
                required_resources,
                {target_compact: available_resources_per_node[target_compact]},
            )
            if chosen_node:
                return chosen_node

        # 3. Consider idle nodes last
        chosen_node = self._best_fit_node(required_resources, idle_nodes)
        if chosen_node:
            return chosen_node

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

    def _update_compacting_node_info(self):
        """Complete or cancel current active compaction.

        Completing the current active compaction:
            If the target node has been made idle, then we have
            successfully migrated all the replicas and the current
            compaction is complete.

        Cancelling the current active compaction:
            If new replicas have been scheduled onto the target node
            since we initially identified the compaction opportunity,
            or if the compaction has been going on for too long, there
            is likely limited resources, so cancel the compaction.
        """

        assert self._compacting_node
        target_node = self._compacting_node.target_node_id

        current_replicas_on_target_compact = set()
        current_replicas_on_target_compact |= self._running_replicas_on_node_id(
            target_node
        )
        current_replicas_on_target_compact |= self._launching_replicas_on_node_id(
            target_node
        )

        # If more replicas have been scheduled onto the node since we
        # identified the compaction opportunity, cancel compaction
        new_replicas = (
            current_replicas_on_target_compact
            - self._compacting_node.cached_running_replicas_on_target_node
        )
        if len(new_replicas):
            logger.info(
                f"Canceling compaction of {target_node} because new replicas "
                f"have been scheduled on {target_node}: {new_replicas}."
            )
            self._compacting_node = None
            self._num_consecutive_failed_compactions += 1
            backoff_s = min(
                2 ** (self._num_consecutive_failed_compactions), MAX_BACKOFF_TIME_S
            )
            self._next_allowed_compaction_timestamp_s = time.time() + backoff_s
            if self._num_consecutive_failed_compactions >= 2:
                logger.info(
                    f"Compaction failed {self._num_consecutive_failed_compactions} "
                    f"times in a row. Retrying after {backoff_s} seconds."
                )

        # If all replicas have migrated off of the node, compaction is
        # complete.
        elif len(current_replicas_on_target_compact) == 0:
            logger.info(f"Successfully migrated replicas off of {target_node}.")
            self._compacting_node = None
            self._num_consecutive_failed_compactions = 0
            self._next_allowed_compaction_timestamp_s = 0

            # Record number of successful node compactions
            self._num_succeeded_compactions += 1
            self._num_compacted_nodes_counter.inc()
            ServeUsageTag.NUM_NODE_COMPACTIONS.record(
                str(self._num_succeeded_compactions)
            )

        # If we have been trying to compact the node for too long and
        # still haven't succeeded in making the target node idle, then
        # there may be resource constrainment issues, cancel compaction
        elif (
            time.time()
            >= self._compacting_node.start_timestamp_s
            + ANYSCALE_RAY_SERVE_COMPACTION_TIMEOUT_S
        ):
            logger.info(
                f"Migrating replicas off of node {target_node} timed out after "
                f"{ANYSCALE_RAY_SERVE_COMPACTION_TIMEOUT_S} seconds. Canceling "
                f"compaction of node {target_node}. Replicas still running on node "
                f"{target_node}: {current_replicas_on_target_compact}."
            )
            self._compacting_node = None
            self._num_consecutive_failed_compactions += 1
            backoff_s = min(
                2 ** (self._num_consecutive_failed_compactions), MAX_BACKOFF_TIME_S
            )
            self._next_allowed_compaction_timestamp_s = time.time() + backoff_s
            if self._num_consecutive_failed_compactions >= 2:
                logger.debug(
                    f"Compaction failed {self._num_consecutive_failed_compactions} "
                    f"times in a row. Retrying after {backoff_s} seconds."
                )

        else:
            # Print warnings at 1min, 10min
            message = (
                f"The controller has been trying to compact {target_node} for more "
                "than {amount}. Resources may be unexpectedly constrained, or "
                "migration is slow because new replicas are taking a long time to "
                "initialize. Compaction will time out and be cancelled if it takes "
                f"more than {ANYSCALE_RAY_SERVE_COMPACTION_TIMEOUT_S / 60} minutes."
            )
            timestamp_10min = self._compacting_node.start_timestamp_s + 600
            timestamp_1min = self._compacting_node.start_timestamp_s + 60
            if time.time() > timestamp_10min and not (
                self._compacting_node.last_logged_timeout_warning
                and self._compacting_node.last_logged_timeout_warning >= timestamp_10min
            ):
                logger.warning(message.format(amount="10 minutes"))
                self._compacting_node.last_logged_timeout_warning = time.time()
            elif time.time() > timestamp_1min and not (
                self._compacting_node.last_logged_timeout_warning
                and self._compacting_node.last_logged_timeout_warning >= timestamp_1min
            ):
                logger.warning(message.format(amount="1 minute"))
                self._compacting_node.last_logged_timeout_warning = time.time()

    def get_node_to_compact(
        self, allow_new_compaction: bool
    ) -> Optional[Tuple[str, float]]:
        """Returns the node to be compacted and the compaction deadline."""

        # Reset active compaction if necessary
        if self._compacting_node:
            self._update_compacting_node_info()

        # If there is an active compaction in progress, return it.
        if self._compacting_node:
            return self._compacting_node.target_node_id, float("inf")

        # Check if we should be starting a new compaction.
        if (
            time.time() < self._next_allowed_compaction_timestamp_s
            or not allow_new_compaction
            or any(len(r) > 0 for r in self._pending_replicas.values())
            or any(len(r) > 0 for r in self._launching_replicas.values())
            or any(len(r) > 0 for r in self._recovering_replicas.values())
        ):
            return None

        # Look for a new node to compact.
        node_with_min_replicas = self._find_best_node_to_compact()

        # It's not possible to compact any of the nodes, meaning we
        # failed to identify a compaction opportunity.
        if not node_with_min_replicas:
            return None

        self._compacting_node = CompactingNodeInfo(
            target_node_id=node_with_min_replicas,
            start_timestamp_s=time.time(),
            cached_running_replicas_on_target_node=self._get_node_to_running_replicas()[
                node_with_min_replicas
            ],
        )
        return self._compacting_node.target_node_id, float("inf")

    def _find_best_node_to_compact(self) -> Optional[str]:
        """Finds the best node to compact, if it exists.

        A node is compactable if it can be made idle by moving all its
        running replicas to other non-idle nodes in the cluster. Out of
        all compactable nodes, we choose the node that has the least
        number of replicas running on it, i.e. the node that requires
        the least number of replica migrations.
        """

        node_to_running_replicas = self._get_node_to_running_replicas()
        node_with_min_replicas = None
        optimal_assignment = None

        available_resources_per_node = self._get_available_resources_per_node()
        total_resources_per_node = {
            node_id: Resources(resources_dict)
            for node_id, resources_dict in (
                self._cluster_node_info_cache.get_total_resources_per_node().items()
            )
        }

        # For each node, check if it can become idle by moving the
        # replicas off of that node - this essentially run best fit
        # binpacking algorithm
        for target_node_id in available_resources_per_node:
            if target_node_id == self._head_node_id:
                continue

            # Only consider resources on other non-idle nodes when running
            # the best fit binpacking algorithm
            available_resources = {
                n: v
                for n, v in available_resources_per_node.items()
                if n != target_node_id and len(node_to_running_replicas[n]) > 0
            }

            # RUN BEST FIT BINPACKING ALGORITHM
            assigned_replicas = dict()
            # 1. Greedily "schedule" replicas. Go through the replicas
            #    that would need to be migrated in DECREASING ORDER by
            #    the size of their required resources.
            replicas_on_curr_node = self._running_replicas_on_node_id(target_node_id)
            replicas_on_curr_node = sorted(
                replicas_on_curr_node,
                key=lambda r: self._deployments[r.deployment_id].required_resources,
                reverse=True,
            )
            for replica_id in replicas_on_curr_node:
                # 2. For each replica, find a node that would minimize fragmentation.
                deployment_id = replica_id.deployment_id
                required_resources = self._deployments[deployment_id].required_resources
                chosen_node = self._find_best_available_node(
                    required_resources, available_resources
                )

                # If no node could fit the replica, the binpacking failed
                if not chosen_node:
                    assigned_replicas = None
                    break
                else:
                    assigned_replicas[replica_id] = chosen_node
                    available_resources[chosen_node] -= required_resources

            # 3. If it's theoretically possible to migrate all replicas
            #    from target_node_id onto other non-idle nodes, then
            #    this node is compactable.
            if assigned_replicas:
                # 4. Choose the largest compactable node (most resources)
                #    since we assume they are more expensive. Upon a tie
                #    choose the one that has the least number of
                #    replicas that require migrating.
                if not node_with_min_replicas or total_resources_per_node.get(
                    target_node_id, Resources()
                ) > total_resources_per_node.get(node_with_min_replicas, Resources()):
                    node_with_min_replicas = target_node_id
                    optimal_assignment = assigned_replicas

                elif not node_with_min_replicas or total_resources_per_node.get(
                    target_node_id, Resources()
                ) == total_resources_per_node.get(node_with_min_replicas, Resources()):
                    if len(assigned_replicas) < len(
                        self._running_replicas_on_node_id(node_with_min_replicas)
                    ):
                        node_with_min_replicas = target_node_id
                        optimal_assignment = assigned_replicas

        if node_with_min_replicas:
            node_to_assigned_replicas = defaultdict(list)
            for replica_id, node_id in optimal_assignment.items():
                node_to_assigned_replicas[node_id].append(replica_id)

            assignment_strs = [
                f"{assigned_replicas} -> {node_id}"
                for node_id, assigned_replicas in node_to_assigned_replicas.items()
            ]
            logger.info(
                f"Found compactable node '{node_with_min_replicas}' with migration "
                f"plan: {{{', '.join(assignment_strs)}}}."
            )
            return node_with_min_replicas
