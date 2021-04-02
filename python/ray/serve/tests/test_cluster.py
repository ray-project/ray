import os
import sys
import time

import pytest
import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield Cluster()
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def test_scale_up(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=3)

    @serve.deployment("D", version="1", num_replicas=1)
    def D(*args):
        return os.getpid()

    def get_pids(expected, timeout=30):
        pids = set()
        start = time.time()
        while len(pids) < expected:
            pids.add(requests.get("http://localhost:8000/D").text)
            if time.time() - start >= timeout:
                raise TimeoutError("Timed out waiting for pids.")
        return pids

    ray.init(head_node.address)
    serve.start(detached=True)
    client = serve.connect()

    D.deploy()
    pids1 = get_pids(1)

    goal_ref = D.options(num_replicas=3).deploy(_blocking=False)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    assert get_pids(1) == pids1

    # Add a node with another CPU, another replica should get placed.
    cluster.add_node(num_cpus=1)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    pids2 = get_pids(2)
    assert pids1.issubset(pids2)

    # Add a node with another CPU, the final replica should get placed
    # and the deploy goal should be done.
    cluster.add_node(num_cpus=1)
    assert client._wait_for_goal(goal_ref)
    pids3 = get_pids(3)
    assert pids2.issubset(pids3)


@pytest.mark.skip("Currently hangs due to max_task_retries=-1.")
def test_node_failure(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=3)
    worker_node = cluster.add_node(num_cpus=2)

    @serve.deployment("D", version="1", num_replicas=3)
    def D(*args):
        return os.getpid()

    def get_pids(expected, timeout=30):
        pids = set()
        start = time.time()
        while len(pids) < expected:
            pids.add(requests.get("http://localhost:8000/D").text)
            if time.time() - start >= timeout:
                raise TimeoutError("Timed out waiting for pids.")
        return pids

    ray.init(cluster.address)
    serve.start(detached=True)

    print("Initial deploy.")
    D.deploy()
    pids1 = get_pids(3)

    # Remove the node. There should still be one replica running.
    print("Kill node.")
    cluster.remove_node(worker_node)
    pids2 = get_pids(1)
    assert pids2.issubset(pids1)

    # Add a worker node back. One replica should get placed.
    print("Add back first node.")
    cluster.add_node(num_cpus=1)
    pids3 = get_pids(2)
    assert pids2.issubset(pids3)

    # Add another worker node. One more replica should get placed.
    print("Add back second node.")
    cluster.add_node(num_cpus=1)
    pids4 = get_pids(3)
    assert pids3.issubset(pids4)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
