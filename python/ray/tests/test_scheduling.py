# coding: utf-8
import collections
import logging
import sys
import time

import numpy as np
import pytest

import ray
import ray.util.accelerators
import ray.cluster_utils
import ray.test_utils

from ray.test_utils import (wait_for_condition, new_scheduler_enabled)

logger = logging.getLogger(__name__)


def attempt_to_load_balance(remote_function,
                            args,
                            total_tasks,
                            num_nodes,
                            minimum_count,
                            num_attempts=100):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get(
            [remote_function.remote(*args) for _ in range(total_tasks)])
        counts = collections.Counter(locations)
        logger.info(f"Counts are {counts}")
        if (len(counts) == num_nodes
                and counts.most_common()[-1][1] >= minimum_count):
            break
        attempts += 1
    assert attempts < num_attempts


def test_load_balancing(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets
    # in a roughly equal manner.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.node.unique_id

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


def test_local_scheduling_first(ray_start_cluster):
    cluster = ray_start_cluster
    num_cpus = 8
    # Disable worker caching.
    cluster.add_node(
        num_cpus=num_cpus,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
        })
    cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.node.unique_id

    def local():
        return ray.get(f.remote()) == ray.worker.global_worker.node.unique_id

    # Wait for a worker to get started.
    wait_for_condition(local)

    # Check that we are scheduling locally while there are resources available.
    for i in range(20):
        assert local()


@pytest.mark.parametrize("fast", [True, False])
def test_load_balancing_with_dependencies(ray_start_cluster, fast):
    if fast and new_scheduler_enabled:
        # Load-balancing on new scheduler can be inefficient if (task
        # duration:heartbeat interval) is small enough.
        pytest.skip()

    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        if fast:
            time.sleep(0.010)
        else:
            time.sleep(0.1)
        return ray.worker.global_worker.node.unique_id

    # This object will be local to one of the raylets. Make sure
    # this doesn't prevent tasks from being scheduled on other raylets.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 25)


def test_load_balancing_under_constrained_memory(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 4
    object_size = 4e7
    num_tasks = 100
    for _ in range(num_nodes):
        cluster.add_node(
            num_cpus=num_cpus,
            memory=(num_cpus - 2) * object_size,
            object_store_memory=(num_cpus - 2) * object_size)
    cluster.add_node(
        num_cpus=0,
        resources={"custom": 1},
        memory=(num_tasks + 1) * object_size,
        object_store_memory=(num_tasks + 1) * object_size)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0, resources={"custom": 1})
    def create_object():
        return np.zeros(int(object_size), dtype=np.uint8)

    @ray.remote
    def f(i, x):
        time.sleep(0.1)
        print(i, ray.worker.global_worker.node.unique_id)
        return ray.worker.global_worker.node.unique_id

    # TODO(swang): Actually test load balancing.
    deps = [create_object.remote() for _ in range(num_tasks)]
    tasks = [f.remote(i, dep) for i, dep in enumerate(deps)]
    for i, dep in enumerate(deps):
        print(i, dep)
    ray.get(tasks)


def test_locality_aware_leasing(ray_start_cluster):
    # This test ensures that a task will run where its task dependencies are
    # located. We run an initial non_local() task that is pinned to a
    # non-local node via a custom resource constraint, and then we run an
    # unpinned task f() that depends on the output of non_local(), ensuring
    # that f() runs on the same node as non_local().
    cluster = ray_start_cluster

    # Disable worker caching so worker leases are not reused, and disable
    # inlining of return objects so return objects are always put into Plasma.
    cluster.add_node(
        num_cpus=1,
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 0,
        })
    # Use a custom resource for pinning tasks to a node.
    non_local_node = cluster.add_node(num_cpus=1, resources={"pin": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin": 1})
    def non_local():
        return ray.worker.global_worker.node.unique_id

    @ray.remote
    def f(x):
        return ray.worker.global_worker.node.unique_id

    # Test that task f() runs on the same node as non_local().
    assert ray.get(f.remote(non_local.remote())) == non_local_node.unique_id


def test_lease_request_leak(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            # This test uses ray.objects(), which only works with the GCS-based
            # object directory
            "ownership_based_object_directory_enabled": False,
            "object_timeout_milliseconds": 200
        })
    assert len(ray.objects()) == 0

    @ray.remote
    def f(x):
        time.sleep(0.1)
        return

    # Submit pairs of tasks. Tasks in a pair can reuse the same worker leased
    # from the raylet.
    tasks = []
    for _ in range(10):
        obj_ref = ray.put(1)
        for _ in range(2):
            tasks.append(f.remote(obj_ref))
        del obj_ref
    ray.get(tasks)

    time.sleep(
        1)  # Sleep for an amount longer than the reconstruction timeout.
    assert len(ray.objects()) == 0, ray.objects()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
