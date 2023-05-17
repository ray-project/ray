import time
import math
import ray
from collections import defaultdict
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class Scheduler:
    def __init__(self, deployment_actor_class, spread_min_nodes):
        # Existing replica placement
        self.deployment_replicas_topology = {}
        self.deployment_actor_class = deployment_actor_class
        # This can be changed to other spread constraints we want to support
        self.spread_min_nodes = spread_min_nodes

    def add_replica(self):
        while True:
            cluster_view = ray._private.state.state.cluster_view()
            target_node_id = self._schedule(
                cluster_view,
                self.deployment_replicas_topology,
                self._deployment_actor_resources(),
                self.spread_min_nodes,
            )
            if target_node_id is not None:
                replica = self.deployment_actor_class.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=target_node_id, soft=False, _fail_on_unavailable=True
                    )
                ).remote()
                try:
                    ray.get(replica.__ray_ready__.remote())
                    self.deployment_replicas_topology[replica] = target_node_id
                    break
                except ray.exceptions.ActorUnschedulableError:
                    # Retry
                    pass
            else:
                # The actor cannot be schedule, needs to add more nodes to the cluster.
                self._trigger_autoscaling(cluster_view)

            # Wait sometime and retry
            time.sleep(0.1)

    def remove_replica(self):
        replica_to_remove = self._select_replica_to_remove(
            self.deployment_replicas_topology
        )
        ray.kill(replica_to_remove)
        del self.deployment_replicas_topology[replica_to_remove]
        return replica_to_remove

    @classmethod
    def _select_replica_to_remove(cls, deployment_replicas_topology):
        node_to_replicas = defaultdict(list)
        for replica, node_id in deployment_replicas_topology.items():
            node_to_replicas[node_id].append(replica)

        # Prefer a node with more than 1 replica
        for node_id, replicas in node_to_replicas.items():
            if len(replicas) > 1:
                return replicas[0]

        # If every node only has 1 replica then kill any replica is fine
        return next(iter(deployment_replicas_topology))

    @classmethod
    def _schedule(
        cls,
        cluster_view,
        deployment_replicas_topology,
        deployment_actor_resources,
        spread_min_nodes,
    ):
        def is_node_available(node_available_resources, deployment_actor_resources):
            for resource, quantity in deployment_actor_resources.items():
                if quantity > node_available_resources.get(resource, 0):
                    return False

            return True

        deployed_nodes = set(deployment_replicas_topology.values())
        if len(deployed_nodes) < spread_min_nodes:
            # We need to schedule onto a new node
            for node_id, resources in cluster_view.items():
                if node_id in deployed_nodes:
                    continue
                if is_node_available(
                    resources["available"], deployment_actor_resources
                ):
                    return node_id
                else:
                    continue

            # No available nodes found
            return None

        else:
            # We already satisfy the spread constraints, we can schedule anywhere
            for node_id, resources in cluster_view.items():
                if is_node_available(
                    resources["available"], deployment_actor_resources
                ):
                    return node_id

            # No available nodes found
            return None

    def _trigger_autoscaling(self, cluster_view):
        bundles = []
        for _, resources in cluster_view.items():
            bundles.append({"CPU": int(resources["total"].get("CPU", 0))})
        # Add one extra bundle for the replica
        bundles.append(
            {
                resource: math.ceil(quantity)
                for resource, quantity in self._deployment_actor_resources().items()
            }
        )
        ray.autoscaler.sdk.request_resources(bundles=bundles)

    def _deployment_actor_resources(self):
        # TODO: support other resources as well
        return {"CPU": self.deployment_actor_class._default_options["num_cpus"]}
