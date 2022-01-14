import sys
import time

import numpy as np
import pytest

import ray
from ray._private.test_utils import get_other_nodes
import ray.ray_constants as ray_constants


@pytest.mark.skip(reason="No reconstruction for objects placed in plasma yet")
@pytest.mark.parametrize(
    "ray_start_cluster",
    [{
        # Force at least one task per node.
        "num_cpus": 1,
        "num_nodes": 4,
        "object_store_memory": 1000 * 1024 * 1024,
        "_system_config": {
            # Raylet codepath is not stable with a shorter timeout.
            "num_heartbeats_timeout": 10,
            "object_manager_pull_timeout_ms": 1000,
            "object_manager_push_timeout_ms": 1000,
        },
    }],
    indirect=True)
def test_object_reconstruction(ray_start_cluster):
    cluster = ray_start_cluster

    # Submit tasks with dependencies in plasma.
    @ray.remote
    def large_value():
        # Sleep for a bit to force tasks onto different nodes.
        time.sleep(0.1)
        return np.zeros(10 * 1024 * 1024)

    @ray.remote
    def g(x):
        return

    # Kill the component on all nodes except the head node as the tasks
    # execute. Do this in a loop while submitting tasks between each
    # component failure.
    time.sleep(0.1)
    worker_nodes = get_other_nodes(cluster)
    assert len(worker_nodes) > 0
    component_type = ray_constants.PROCESS_TYPE_RAYLET
    for node in worker_nodes:
        process = node.all_processes[component_type][0].process
        # Submit a round of tasks with many dependencies.
        num_tasks = len(worker_nodes)
        xs = [large_value.remote() for _ in range(num_tasks)]
        # Wait for the tasks to complete, then evict the objects from the local
        # node.
        for x in xs:
            ray.get(x)
            ray.internal.free([x], local_only=True)

        # Kill a component on one of the nodes.
        process.terminate()
        time.sleep(1)
        process.kill()
        process.wait()
        assert not process.poll() is None

        # Make sure that we can still get the objects after the
        # executing tasks died.
        print("F", xs)
        xs = [g.remote(x) for x in xs]
        print("G", xs)
        ray.get(xs)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 3,
        "do_init": True
    }],
    indirect=True)
def test_actor_creation_node_failure(ray_start_cluster):
    # TODO(swang): Refactor test_raylet_failed, etc to reuse the below code.
    cluster = ray_start_cluster

    @ray.remote
    class Child:
        def __init__(self, death_probability):
            self.death_probability = death_probability

        def ping(self):
            # Exit process with some probability.
            exit_chance = np.random.rand()
            if exit_chance < self.death_probability:
                sys.exit(-1)

    num_children = 25
    # Children actors will die about half the time.
    death_probability = 0.5

    children = [Child.remote(death_probability) for _ in range(num_children)]
    while len(cluster.list_all_nodes()) > 1:
        for j in range(2):
            # Submit some tasks on the actors. About half of the actors will
            # fail.
            children_out = [child.ping.remote() for child in children]
            # Wait a while for all the tasks to complete. This should trigger
            # reconstruction for any actor creation tasks that were forwarded
            # to nodes that then failed.
            ready, _ = ray.wait(
                children_out, num_returns=len(children_out), timeout=5 * 60.0)
            assert len(ready) == len(children_out)

            # Replace any actors that died.
            for i, out in enumerate(children_out):
                try:
                    ray.get(out)
                except ray.exceptions.RayActorError:
                    children[i] = Child.remote(death_probability)
        # Remove a node. Any actor creation tasks that were forwarded to this
        # node must be resubmitted.
        cluster.remove_node(get_other_nodes(cluster, True)[-1])


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
