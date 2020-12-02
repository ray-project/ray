import sys
import time

import numpy as np
import pytest

import ray
import ray.ray_constants as ray_constants
from ray.test_utils import get_other_nodes


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

        def get_probability(self):
            return self.death_probability

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

            children_out = [
                child.get_probability.remote() for child in children
            ]
            # Wait for new created actors to finish creation before
            # removing a node. This is needed because right now we don't
            # support reconstructing actors that died in the process of
            # being created.
            ready, _ = ray.wait(
                children_out, num_returns=len(children_out), timeout=5 * 60.0)
            assert len(ready) == len(children_out)

        # Remove a node. Any actor creation tasks that were forwarded to this
        # node must be restarted.
        cluster.remove_node(get_other_nodes(cluster, True)[-1])


def test_driver_lives_sequential(ray_start_regular):
    ray.worker._global_node.kill_raylet()
    ray.worker._global_node.kill_plasma_store()
    ray.worker._global_node.kill_log_monitor()
    ray.worker._global_node.kill_monitor()
    ray.worker._global_node.kill_gcs_server()

    # If the driver can reach the tearDown method, then it is still alive.


def test_driver_lives_parallel(ray_start_regular):
    all_processes = ray.worker._global_node.all_processes

    process_infos = (all_processes[ray_constants.PROCESS_TYPE_PLASMA_STORE] +
                     all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER] +
                     all_processes[ray_constants.PROCESS_TYPE_RAYLET] +
                     all_processes[ray_constants.PROCESS_TYPE_LOG_MONITOR] +
                     all_processes[ray_constants.PROCESS_TYPE_MONITOR])
    assert len(process_infos) == 5

    # Kill all the components in parallel.
    for process_info in process_infos:
        process_info.process.terminate()

    time.sleep(0.1)
    for process_info in process_infos:
        process_info.process.kill()

    for process_info in process_infos:
        process_info.process.wait()

    # If the driver can reach the tearDown method, then it is still alive.


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
