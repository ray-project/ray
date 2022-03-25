# coding: utf-8
import collections
import logging
import sys
import time

import pytest

import ray
import ray.util.accelerators
import ray.cluster_utils

from ray._private.test_utils import Semaphore

logger = logging.getLogger(__name__)


def attempt_to_load_balance(
    remote_function, args, total_tasks, num_nodes, minimum_count, num_attempts=100
):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get([remote_function.remote(*args) for _ in range(total_tasks)])
        counts = collections.Counter(locations)
        print(f"Counts are {counts}")
        if len(counts) == num_nodes and counts.most_common()[-1][1] >= minimum_count:
            break
        attempts += 1
    assert attempts < num_attempts


def test_load_balancing(ray_start_cluster_enabled):
    # This test ensures that tasks are being assigned to all raylets
    # in a roughly equal manner.
    cluster = ray_start_cluster_enabled
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.10)
        return ray.worker.global_worker.node.unique_id

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


def test_hybrid_policy(ray_start_cluster_enabled):

    cluster = ray_start_cluster_enabled
    num_nodes = 2
    num_cpus = 10
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus, memory=num_cpus)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # `block_task` ensures that scheduled tasks do not return until all are
    # running.
    block_task = Semaphore.remote(0)
    # `block_driver` ensures that the driver does not allow tasks to continue
    # until all are running.
    block_driver = Semaphore.remote(0)

    # Add the memory resource because the cpu will be released in the ray.get
    @ray.remote(num_cpus=1, memory=1)
    def get_node():
        ray.get(block_driver.release.remote())
        ray.get(block_task.acquire.remote())
        return ray.worker.global_worker.current_node_id

    # Below the hybrid threshold we pack on the local node first.
    refs = [get_node.remote() for _ in range(5)]
    ray.get([block_driver.acquire.remote() for _ in refs])
    ray.get([block_task.release.remote() for _ in refs])
    nodes = ray.get(refs)
    assert len(set(nodes)) == 1

    # We pack the second node to the hybrid threshold.
    refs = [get_node.remote() for _ in range(10)]
    ray.get([block_driver.acquire.remote() for _ in refs])
    ray.get([block_task.release.remote() for _ in refs])
    nodes = ray.get(refs)
    counter = collections.Counter(nodes)
    for node_id in counter:
        print(f"{node_id}: {counter[node_id]}")
        assert counter[node_id] == 5

    # Once all nodes are past the hybrid threshold we round robin.
    # TODO (Alex): Ideally we could schedule less than 20 nodes here, but the
    # policy is imperfect if a resource report interrupts the process.
    refs = [get_node.remote() for _ in range(20)]
    ray.get([block_driver.acquire.remote() for _ in refs])
    ray.get([block_task.release.remote() for _ in refs])
    nodes = ray.get(refs)
    counter = collections.Counter(nodes)
    for node_id in counter:
        print(f"{node_id}: {counter[node_id]}")
        assert counter[node_id] == 10, counter


if __name__ == "__main__":

    sys.exit(pytest.main(["-v", __file__]))
