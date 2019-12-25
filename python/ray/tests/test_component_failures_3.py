from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import time
import json

import numpy as np
import pytest

import ray
import ray.ray_constants as ray_constants

RAY_FORCE_DIRECT = ray_constants.direct_call_enabled()

@pytest.mark.skipif(RAY_FORCE_DIRECT, reason="actor reconstruction not supported for actor creation task failure")
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
    class Child(object):
        def __init__(self, death_probability):
            self.death_probability = death_probability

        def ping(self):
            # Exit process with some probability.
            exit_chance = np.random.rand()
            if exit_chance < self.death_probability:
                sys.exit(-1)

    num_children = 50
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
        # node must be reconstructed.
        cluster.remove_node(cluster.list_all_nodes()[-1])

# This test covers actor creation node failure scenario with the assumption
# that the nodes will only fail after the actors are created or reconstructed.
# NOTE(zhijunfu): This is because right now we only support actor reconstruction
# after it has been created, and cannot cover worker failure whiling executing
# actor creatiion task.
@pytest.mark.skipif(not RAY_FORCE_DIRECT, reason="only test direct actors")
@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 4,
        "num_nodes": 3,
        "do_init": True,
        "_internal_config": json.dumps({
            "initial_reconstruction_timeout_milliseconds": 1000,
            "num_heartbeats_timeout": 100,
        })
    }],
    indirect=True)
def test_actor_creation_node_failure_after_actor_created(ray_start_cluster):
    # TODO(swang): Refactor test_raylet_failed, etc to reuse the below code.
    cluster = ray_start_cluster

    @ray.remote(max_reconstructions=100)
    class Child(object):
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
        # Submit some tasks on the actors. About half of the actors will
        # fail.
        children_out = [child.ping.remote() for child in children]
        # Wait a while for all the tasks to complete, so we can know
        # which actors have died.
        ready, _ = ray.wait(
            children_out, num_returns=len(children_out), timeout=5 * 60.0)
        assert len(ready) == len(children_out)

        # Replace any actors that died.
        for i, out in enumerate(children_out):
            try:
                ray.get(out)
            except ray.exceptions.RayActorError:
                children[i] = Child.remote(death_probability)


        children_out = [child.get_probability.remote() for child in children]
        # Wait for new created actors to finish creation before removing a node.
        # This is needed because right now we don't support reconstructing actors
        # that died in the process of being created.
        ready, _ = ray.wait(
            children_out, num_returns=len(children_out), timeout=5 * 60.0)
        assert len(ready) == len(children_out)

        # Remove a node. Any actor creation tasks that were forwarded to this
        # node must be reconstructed.
        cluster.remove_node(cluster.list_all_nodes()[-1])

@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_driver_lives_sequential(ray_start_regular):
    ray.worker._global_node.kill_raylet()
    ray.worker._global_node.kill_plasma_store()
    ray.worker._global_node.kill_log_monitor()
    ray.worker._global_node.kill_monitor()
    ray.worker._global_node.kill_raylet_monitor()

    # If the driver can reach the tearDown method, then it is still alive.


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_driver_lives_parallel(ray_start_regular):
    all_processes = ray.worker._global_node.all_processes
    process_infos = (all_processes[ray_constants.PROCESS_TYPE_PLASMA_STORE] +
                     all_processes[ray_constants.PROCESS_TYPE_RAYLET] +
                     all_processes[ray_constants.PROCESS_TYPE_LOG_MONITOR] +
                     all_processes[ray_constants.PROCESS_TYPE_MONITOR] +
                     all_processes[ray_constants.PROCESS_TYPE_RAYLET_MONITOR])
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
