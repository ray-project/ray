import os
import sys
import time

import pytest
import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster

from ray.serve.deployment_state import ReplicaStartupStatus, ReplicaState
from ray._private.test_utils import SignalActor, wait_for_condition


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
    client = serve.api._connect()

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


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_replica_startup_status_transitions(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=1)
    cluster.connect(namespace="serve")
    serve_instance = serve.start()

    signal = SignalActor.remote()

    @serve.deployment(version="1", ray_actor_options={"num_cpus": 2})
    class D:
        def __init__(self):
            ray.get(signal.wait.remote())

    D.deploy(_blocking=False)

    def get_replicas(replica_state):
        controller = serve_instance._controller
        replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(D.name))
        return replicas.get([replica_state])

    # wait for serve to start the replica, and catch a reference to it.
    wait_for_condition(lambda: len(get_replicas(ReplicaState.STARTING)) > 0)
    replica = get_replicas(ReplicaState.STARTING)[0]

    # declare shorthands as yapf doesn't like long lambdas
    PENDING_ALLOCATION = ReplicaStartupStatus.PENDING_ALLOCATION
    PENDING_INITIALIZATION = ReplicaStartupStatus.PENDING_INITIALIZATION
    SUCCEEDED = ReplicaStartupStatus.SUCCEEDED

    # currently there are no resources to allocate the replica
    assert replica.check_started() == PENDING_ALLOCATION

    # add the necessary resources to allocate the replica
    cluster.add_node(num_cpus=4)
    wait_for_condition(
        lambda: (replica.check_started() == PENDING_INITIALIZATION))

    # send signal to complete replica intialization
    signal.send.remote()
    wait_for_condition(lambda: replica.check_started() == SUCCEEDED)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
