import os
import sys
import time
from collections import defaultdict

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.cluster_utils import Cluster
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.deployment_state import ReplicaStartupStatus, ReplicaState


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
    cluster.connect(namespace=SERVE_NAMESPACE)
    # By default, Serve controller and proxy actors use 0 CPUs,
    # so initially there should only be room for 1 replica.

    @serve.deployment(version="1", num_replicas=1, health_check_period_s=1)
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
    client = serve.context._connect()

    D.deploy()
    pids1 = get_pids(1)

    D.options(num_replicas=3).deploy(_blocking=False)

    # Check that a new replica has not started in 1.0 seconds.  This
    # doesn't guarantee that a new replica won't ever be started, but
    # 1.0 seconds is a reasonable upper bound on replica startup time.
    with pytest.raises(TimeoutError):
        client._wait_for_deployment_healthy(D.name, timeout_s=1)
    assert get_pids(1) == pids1

    # Add a node with another CPU, another replica should get placed.
    cluster.add_node(num_cpus=1)
    with pytest.raises(TimeoutError):
        client._wait_for_deployment_healthy(D.name, timeout_s=1)
    pids2 = get_pids(2)
    assert pids1.issubset(pids2)

    # Add a node with another CPU, the final replica should get placed
    # and the deploy goal should be done.
    cluster.add_node(num_cpus=1)
    client._wait_for_deployment_healthy(D.name)
    pids3 = get_pids(3)
    assert pids2.issubset(pids3)


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_node_failure(ray_cluster):
    cluster = ray_cluster

    cluster.add_node(num_cpus=3)
    cluster.connect(namespace=SERVE_NAMESPACE)

    # NOTE(edoakes): we need to start serve before adding the worker node to
    # guarantee that the controller is placed on the head node (we should be
    # able to tolerate being placed on workers, but there's currently a bug).
    # We should add an explicit test for that in the future when it's fixed.
    serve.start(detached=True)

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
            time.sleep(1)
        return pids

    print("Initial deploy.")
    serve.run(D.bind())
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
    cluster.connect(namespace=SERVE_NAMESPACE)
    serve_instance = serve.start()

    signal = SignalActor.remote()

    @serve.deployment(version="1", ray_actor_options={"num_cpus": 2})
    class E:
        async def __init__(self):
            await signal.wait.remote()

    E.deploy(_blocking=False)

    def get_replicas(replica_state):
        controller = serve_instance._controller
        replicas = ray.get(controller._dump_replica_states_for_testing.remote(E.name))
        return replicas.get([replica_state])

    # wait for serve to start the replica, and catch a reference to it.
    wait_for_condition(lambda: len(get_replicas(ReplicaState.STARTING)) > 0)
    replica = get_replicas(ReplicaState.STARTING)[0]

    # currently there are no resources to allocate the replica
    assert replica.check_started() == ReplicaStartupStatus.PENDING_ALLOCATION

    # add the necessary resources to allocate the replica
    cluster.add_node(num_cpus=4)
    wait_for_condition(lambda: (ray.cluster_resources().get("CPU", 0) >= 4))
    wait_for_condition(lambda: (ray.available_resources().get("CPU", 0) >= 2))

    def is_replica_pending_initialization():
        status = replica.check_started()
        print(status)
        return status == ReplicaStartupStatus.PENDING_INITIALIZATION

    wait_for_condition(is_replica_pending_initialization, timeout=25)

    # send signal to complete replica intialization
    signal.send.remote()
    wait_for_condition(
        lambda: replica.check_started() == ReplicaStartupStatus.SUCCEEDED
    )


def test_intelligent_scale_down(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)
    serve.start()

    @serve.deployment(version="1")
    def f():
        pass

    def get_actor_distributions():
        actors = ray._private.state.actors()
        node_to_actors = defaultdict(list)
        for actor in actors.values():
            if "ServeReplica" not in actor["ActorClassName"]:
                continue
            if actor["State"] != "ALIVE":
                continue
            node_to_actors[actor["Address"]["NodeID"]].append(actor)

        return set(map(len, node_to_actors.values()))

    f.options(num_replicas=3).deploy()
    assert get_actor_distributions() == {2, 1}

    f.options(num_replicas=2).deploy()
    assert get_actor_distributions() == {2}


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_replica_spread(ray_cluster):
    cluster = ray_cluster

    cluster.add_node(num_cpus=2)

    # NOTE(edoakes): we need to start serve before adding the worker node to
    # guarantee that the controller is placed on the head node (we should be
    # able to tolerate being placed on workers, but there's currently a bug).
    # We should add an explicit test for that in the future when it's fixed.
    cluster.connect(namespace=SERVE_NAMESPACE)
    serve.start(detached=True)

    worker_node = cluster.add_node(num_cpus=2)

    @serve.deployment(num_replicas=2)
    def get_node_id():
        return os.getpid(), ray.get_runtime_context().node_id.hex()

    h = serve.run(get_node_id.bind())

    def get_num_nodes():
        pids = set()
        node_ids = set()
        while len(pids) < 2:
            pid, node = ray.get(h.remote())
            pids.add(pid)
            node_ids.add(node)

        return len(node_ids)

    # Check that the two replicas are spread across the two nodes.
    wait_for_condition(lambda: get_num_nodes() == 2)

    # Kill the worker node. The second replica should get rescheduled on
    # the head node.
    cluster.remove_node(worker_node)

    # Check that the replica on the dead node can be rescheduled.
    wait_for_condition(lambda: get_num_nodes() == 1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
