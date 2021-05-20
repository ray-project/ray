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
    cluster.add_node(num_cpus=1)
    cluster.connect(namespace="serve")
    # By default, Serve controller and proxy actors use 0 CPUs,
    # so initially there should only be room for 1 replica.

    @serve.deployment(version="1", num_replicas=1)
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

    serve.start(detached=True)
    client = serve.connect()

    D.deploy()
    pids1 = get_pids(1)

    goal_ref = D.options(num_replicas=3).deploy(_blocking=False)

    # Check that a new replica has not started in 1.0 seconds.  This
    # doesn't guarantee that a new replica won't ever be started, but
    # 1.0 seconds is a reasonable upper bound on replica startup time.
    assert not client._wait_for_goal(goal_ref, timeout=1.0)
    assert get_pids(1) == pids1

    # Add a node with another CPU, another replica should get placed.
    cluster.add_node(num_cpus=1)
    assert not client._wait_for_goal(goal_ref, timeout=1.0)
    pids2 = get_pids(2)
    assert pids1.issubset(pids2)

    # Add a node with another CPU, the final replica should get placed
    # and the deploy goal should be done.
    cluster.add_node(num_cpus=1)
    assert client._wait_for_goal(goal_ref)
    pids3 = get_pids(3)
    assert pids2.issubset(pids3)


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_node_failure(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=3)
    cluster.connect(namespace="serve")

    worker_node = cluster.add_node(num_cpus=2)

    @serve.deployment(version="1", num_replicas=5)
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

    serve.start(detached=True)

    print("Initial deploy.")
    D.deploy()
    pids1 = get_pids(5)

    # Remove the node. There should still be three replicas running.
    print("Kill node.")
    cluster.remove_node(worker_node)
    pids2 = get_pids(3)
    assert pids2.issubset(pids1)

    # Add a worker node back. One replica should get placed.
    print("Add back first node.")
    cluster.add_node(num_cpus=1)
    pids3 = get_pids(4)
    assert pids2.issubset(pids3)

    # Add another worker node. One more replica should get placed.
    print("Add back second node.")
    cluster.add_node(num_cpus=1)
    pids4 = get_pids(5)
    assert pids3.issubset(pids4)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
