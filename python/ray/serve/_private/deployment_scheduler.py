import ray
import math
from collections import defaultdict
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class DeploymentSchedulingConstraint:
    pass


class SpreadDeploymentSchedulingConstraint(DeploymentSchedulingConstraint):
    def __init__(self):
        self.min_nodes = 2


class DriverDeploymentSchedulingConstraint(DeploymentSchedulingConstraint):
    # TODO: one replica on each existing node, no autoscaling
    pass


class DeploymentScheduler:
    def __init__(self):
        self._deployments = {}
        # Replicas that are pending to be scheduled.
        self._pending_replicas = defaultdict(dict)
        # Replicas that are being scheduled.
        # The underlying actors are submitted but
        # we don't know where they are running.
        self._launching_replicas = defaultdict(dict)
        # Replicas that are recovering.
        # We don't know where those replicas are running.
        self._recovering_replicas = defaultdict(dict)
        # Replicas that are running.
        # We know where those replicas are running.
        self._running_replicas = defaultdict(dict)

    def add_deployment(self, deployment_name, scheduling_constraint):
        self._deployments[deployment_name] = scheduling_constraint

    def remove_deployment(self, deployment_name):
        del self._deployments[deployment_name]

    def schedule_replica(
        self,
        deployment_name,
        replica_name,
        actor_def,
        actor_resources,
        actor_options,
        actor_init_args,
        on_scheduled,
    ):
        self._pending_replicas[deployment_name][replica_name] = (
            actor_def,
            actor_resources,
            actor_options,
            actor_init_args,
            on_scheduled,
        )

    def stop_replica(self, deployment_name, replica_name):
        self._pending_replicas[deployment_name].pop(replica_name, None)
        self._launching_replicas[deployment_name].pop(replica_name, None)
        self._recovering_replicas[deployment_name].pop(replica_name, None)
        self._running_replicas[deployment_name].pop(replica_name, None)

    def on_replica_running(self, deployment_name, replica_name, node_id):
        self._launching_replicas[deployment_name].pop(replica_name, None)
        self._recovering_replicas[deployment_name].pop(replica_name, None)
        self._running_replicas[deployment_name][replica_name] = node_id

    def on_replica_recovering(self, deployment_name, replica_name):
        self._recovering_replicas[deployment_name][replica_name] = None

    def get_replicas_to_stop(self, deployment_name, max_num_to_stop):
        replicas_to_stop = set()

        pending_replicas = self._pending_replicas[deployment_name].keys()
        for pending_replica in pending_replicas:
            if len(replicas_to_stop) == max_num_to_stop:
                return replicas_to_stop
            else:
                replicas_to_stop.add(pending_replica)

        launching_replicas = self._launching_replicas[deployment_name].keys()
        for launching_replica in launching_replicas:
            if len(replicas_to_stop) == max_num_to_stop:
                return replicas_to_stop
            else:
                replicas_to_stop.add(launching_replica)

        # Try to preserve spread by choosing replicas from the node with most replicas.
        node_to_running_replicas = defaultdict(set)
        for running_replica, node_id in self._running_replicas[deployment_name].items():
            node_to_running_replicas[node_id].add(running_replica)
        while len(replicas_to_stop) < max_num_to_stop:
            node_with_most_running_replicas = max(
                node_to_running_replicas, key=lambda k: len(node_to_running_replicas[k])
            )
            replicas_to_stop.add(
                node_to_running_replicas[node_with_most_running_replicas].pop()
            )

        return replicas_to_stop

    def schedule(self, preempting_nodes):
        # Holistically schedule all the pending replicas.
        # TODO: consider colocation.
        pending_autoscaling = []
        cluster_view = self._get_cluster_view()

        for deployment_name, pending_replicas in self._pending_replicas.items():
            scheduling_constraint = self._deployments[deployment_name]
            assert isinstance(
                scheduling_constraint, SpreadDeploymentSchedulingConstraint
            )
            min_nodes = scheduling_constraint.min_nodes

            if len(pending_replicas) == 0:
                continue
            if len(self._recovering_replicas[deployment_name]) > 0:
                # Wait until recovering is done before scheduling new replicas
                # so that we know more actor location information.
                continue

            node_to_replicas = defaultdict(set)
            for launching_replica, node_id in self._launching_replicas[
                deployment_name
            ].items():
                assert node_id is not None
                node_to_replicas[node_id].add(launching_replica)
            for running_replica, node_id in self._running_replicas[
                deployment_name
            ].items():
                assert node_id is not None
                node_to_replicas[node_id].add(running_replica)

            for pending_replica_name in list(pending_replicas.keys()):
                # TODO: this can be pushed down to ray core once we have actor
                # labels, affinity and anti-affinity supports.
                (
                    actor_def,
                    actor_resources,
                    actor_options,
                    actor_init_args,
                    on_scheduled,
                ) = pending_replicas[pending_replica_name]
                actor_handle = None
                for node_id in cluster_view.keys():
                    if (
                        not self._is_node_available(
                            cluster_view[node_id]["available"], actor_resources
                        )
                        or node_id in preempting_nodes
                    ):
                        continue
                    if (
                        len(node_to_replicas) < min_nodes
                        and node_id in node_to_replicas
                    ):
                        continue

                    # TODO integrate with _fail_on_unavailable PR
                    scheduling_strategy = NodeAffinitySchedulingStrategy(
                        node_id, soft=False
                    )
                    actor_handle = actor_def.options(
                        scheduling_strategy=scheduling_strategy, **actor_options
                    ).remote(*actor_init_args)
                    self._subtract_node_available_resources(
                        cluster_view[node_id]["available"], actor_resources
                    )
                    del pending_replicas[pending_replica_name]
                    self._launching_replicas[deployment_name][
                        pending_replica_name
                    ] = node_id
                    on_scheduled(actor_handle)
                    node_to_replicas[node_id].add(pending_replica_name)
                    break

                if actor_handle is None:
                    # We need to autoscale
                    pending_autoscaling.append(
                        {
                            resource: math.ceil(quantity)
                            for resource, quantity in actor_resources.items()
                        }
                    )
                    break

            if pending_autoscaling:
                self._trigger_autoscaling(cluster_view, pending_autoscaling)

    def _trigger_autoscaling(self, cluster_view, pending_autoscaling):
        bundles = []
        for _, resources in cluster_view.items():
            bundles.append({"CPU": int(resources["total"].get("CPU", 0))})
        bundles.extend(pending_autoscaling)
        ray.autoscaler.sdk.request_resources(bundles=bundles)

    def _get_cluster_view(self):
        nodes = {}
        available_resources_per_node = (
            ray._private.state.state._available_resources_per_node()
        )
        for node in ray.nodes():
            node_id = node["NodeID"]
            if node_id not in available_resources_per_node:
                continue
            nodes[node_id] = {
                "available": available_resources_per_node[node_id],
                "total": node["Resources"],
            }

        return nodes

    def _is_node_available(self, node_available_resources, actor_resources):
        for resource, quantity in actor_resources.items():
            if quantity > node_available_resources.get(resource, 0):
                return False

        return True

    def _subtract_node_available_resources(
        self, node_available_resources, actor_resources
    ):
        for resource, quantity in actor_resources.items():
            if resource in node_available_resources:
                node_available_resources[resource] = (
                    node_available_resources[resource] - quantity
                )
