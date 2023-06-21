import ray
from typing import Callable, Dict, Tuple, Optional, List
from dataclasses import dataclass
from collections import defaultdict
from ray._raylet import GcsClient
from ray.serve._private.utils import get_all_node_ids
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class SpreadDeploymentSchedulingPolicy:
    pass


class DriverDeploymentSchedulingPolicy:
    pass


@dataclass
class DeploymentUpscaleRequest:
    """Request to schedule a single replica.

    The scheduler is responsible for scheduling
    based on the deployment scheduling policy.
    """

    deployment_name: str
    replica_name: str
    actor_def: ray.actor.ActorClass
    actor_resources: Dict
    actor_options: Dict
    actor_init_args: Tuple
    on_scheduled: Callable


@dataclass
class DeploymentDownscaleRequest:
    """Request to stop certain number of replicas.

    The scheduler is responsible for
    choosing the replicas to stop.
    """

    deployment_name: str
    num_to_stop: int


class DeploymentScheduler:
    """A centralized scheduler for all serve deployments.

    It makes scheduling decisions in a batch mode for each update cycle.
    """

    def __init__(self, gcs_client: Optional[GcsClient] = None):
        # {deployment_name: scheduling_policy}
        self._deployments = {}
        # Replicas that are pending to be scheduled.
        # {deployment_name: {replica_name: deployment_upscale_request}}
        self._pending_replicas = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors are submitted.
        # {deployment_name: {replica_name: target_node_id}}
        self._launching_replicas = defaultdict(dict)
        # Replicas that are recovering.
        # We don't know where those replicas are running.
        # {deployment_name: {replica_name}}
        self._recovering_replicas = defaultdict(set)
        # Replicas that are running.
        # We know where those replicas are running.
        # {deployment_name: {replica_name: running_node_id}}
        self._running_replicas = defaultdict(dict)

        if gcs_client:
            self._gcs_client = gcs_client
        else:
            self._gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    def on_deployment_created(self, deployment_name, scheduling_policy):
        """This is called whenver a new deployment is created."""
        assert deployment_name not in self._pending_replicas
        assert deployment_name not in self._launching_replicas
        assert deployment_name not in self._recovering_replicas
        assert deployment_name not in self._running_replicas
        self._deployments[deployment_name] = scheduling_policy

    def on_deployment_deleted(self, deployment_name):
        """This is called whenver a deployment is deleted."""
        assert not self._pending_replicas[deployment_name]
        del self._pending_replicas[deployment_name]

        assert not self._launching_replicas[deployment_name]
        del self._launching_replicas[deployment_name]

        assert not self._recovering_replicas[deployment_name]
        del self._recovering_replicas[deployment_name]

        assert not self._running_replicas[deployment_name]
        del self._running_replicas[deployment_name]

        del self._deployments[deployment_name]

    def on_replica_stopping(self, deployment_name, replica_name):
        """This is called whenver a deployment replica is being stopped."""
        self._pending_replicas[deployment_name].pop(replica_name, None)
        self._launching_replicas[deployment_name].pop(replica_name, None)
        self._recovering_replicas[deployment_name].discard(replica_name)
        self._running_replicas[deployment_name].pop(replica_name, None)

    def on_replica_running(self, deployment_name, replica_name, node_id):
        """This is called whenver a deployment replica is running with known node id."""
        assert replica_name not in self._pending_replicas[deployment_name]

        self._launching_replicas[deployment_name].pop(replica_name, None)
        self._recovering_replicas[deployment_name].discard(replica_name)

        self._running_replicas[deployment_name][replica_name] = node_id

    def on_replica_recovering(self, deployment_name, replica_name):
        """This is called whenver a deployment replica is recovering."""
        assert replica_name not in self._pending_replicas[deployment_name]
        assert replica_name not in self._launching_replicas[deployment_name]
        assert replica_name not in self._running_replicas[deployment_name]

        self._recovering_replicas[deployment_name].add(replica_name)

    def schedule(
        self,
        upscales: List[DeploymentUpscaleRequest],
        downscales: List[DeploymentDownscaleRequest],
    ):
        """This is called for each update cycle to do batch scheduling.

        Args:
            upscales: a list of replicas to schedule.
            downscales: a list of downscale requests.

        Returns:
            The replicas to stop for each deployment.
        """
        for upscale in upscales:
            self._pending_replicas[upscale.deployment_name][
                upscale.replica_name
            ] = upscale

        for deployment_name, pending_replicas in self._pending_replicas.items():
            if not pending_replicas:
                continue

            deployment_scheduling_policy = self._deployments[deployment_name]
            if isinstance(
                deployment_scheduling_policy, SpreadDeploymentSchedulingPolicy
            ):
                self._schedule_spread_deployment(deployment_name)
            else:
                assert isinstance(
                    deployment_scheduling_policy, DriverDeploymentSchedulingPolicy
                )
                self._schedule_driver_deployment(deployment_name)

        deployment_to_replicas_to_stop = {}
        for downscale in downscales:
            deployment_to_replicas_to_stop[
                downscale.deployment_name
            ] = self._get_replicas_to_stop(
                downscale.deployment_name, downscale.num_to_stop
            )

        return deployment_to_replicas_to_stop

    def _schedule_spread_deployment(self, deployment_name):
        for pending_replica_name in list(
            self._pending_replicas[deployment_name].keys()
        ):
            pending_replica = self._pending_replicas[deployment_name][
                pending_replica_name
            ]

            actor_handle = pending_replica.actor_def.options(
                scheduling_strategy="SPREAD",
                **pending_replica.actor_options,
            ).remote(*pending_replica.actor_init_args)
            del self._pending_replicas[deployment_name][pending_replica_name]
            self._launching_replicas[deployment_name][pending_replica_name] = None
            pending_replica.on_scheduled(actor_handle)

    def _schedule_driver_deployment(self, deployment_name):
        if self._recovering_replicas[deployment_name]:
            # Wait until recovering is done before scheduling new replicas
            # so that we can make sure we don't schedule two replicas on the same node.
            return

        all_nodes = {node_id for node_id, _ in get_all_node_ids(self._gcs_client)}
        scheduled_nodes = set()
        for node_id in self._launching_replicas[deployment_name].values():
            assert node_id is not None
            scheduled_nodes.add(node_id)
        for node_id in self._running_replicas[deployment_name].values():
            assert node_id is not None
            scheduled_nodes.add(node_id)
        unscheduled_nodes = all_nodes - scheduled_nodes

        for pending_replica_name in list(
            self._pending_replicas[deployment_name].keys()
        ):
            if not unscheduled_nodes:
                return

            pending_replica = self._pending_replicas[deployment_name][
                pending_replica_name
            ]

            target_node_id = unscheduled_nodes.pop()
            actor_handle = pending_replica.actor_def.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    target_node_id, soft=False
                )
                ** pending_replica.actor_options,
            ).remote(*pending_replica.actor_init_args)
            del self._pending_replicas[deployment_name][pending_replica_name]
            self._launching_replicas[deployment_name][
                pending_replica_name
            ] = target_node_id
            pending_replica.on_scheduled(actor_handle)

    def _get_replicas_to_stop(self, deployment_name, max_num_to_stop):
        """Prioritize replicas that have fewest copies on a node.

        This algorithm helps to scale down more intelligently because it can
        relinquish node faster. Note that this algorithm doesn't consider other
        deployments or other actors on the same node. See more at
        https://github.com/ray-project/ray/issues/20599.
        """
        replicas_to_stop = set()

        pending_launching_recovering_replicas = set().union(
            self._pending_replicas[deployment_name].keys(),
            self._launching_replicas[deployment_name].keys(),
            self._recovering_replicas[deployment_name],
        )
        for (
            pending_launching_recovering_replica
        ) in pending_launching_recovering_replicas:
            if len(replicas_to_stop) == max_num_to_stop:
                return replicas_to_stop
            else:
                replicas_to_stop.add(pending_launching_recovering_replica)

        node_to_running_replicas = defaultdict(set)
        for running_replica, node_id in self._running_replicas[deployment_name].items():
            node_to_running_replicas[node_id].add(running_replica)
        for running_replicas in sorted(
            node_to_running_replicas.values(), key=lambda lst: len(lst)
        ):
            for running_replica in running_replicas:
                if len(replicas_to_stop) == max_num_to_stop:
                    return replicas_to_stop
                else:
                    replicas_to_stop.add(running_replica)

        return replicas_to_stop
