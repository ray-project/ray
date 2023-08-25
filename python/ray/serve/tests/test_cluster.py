import os
import sys
import time
from collections import defaultdict
import requests

import pytest

import ray
from ray._private.test_utils import SignalActor, wait_for_condition

from ray import serve
from ray.cluster_utils import Cluster
from ray.serve.context import get_global_client
from ray.serve.handle import RayServeHandle
from ray.serve._private.constants import SERVE_NAMESPACE, RAY_SERVE_ENABLE_NEW_ROUTING
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.deployment_state import ReplicaStartupStatus
from ray.serve._private.utils import get_head_node_id


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield cluster
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def get_pids(expected, deployment_name="D", app_name="default", timeout=30):
    handle = serve.get_deployment_handle(deployment_name, app_name)
    refs = []
    pids = set()
    start = time.time()
    while len(pids) < expected:
        if len(refs) == 0:
            refs = [handle.remote()._to_object_ref_sync() for _ in range(10)]

        done, pending = ray.wait(refs)
        pids = pids.union(set(ray.get(done)))
        refs = list(pending)
        if time.time() - start >= timeout:
            raise TimeoutError("Timed out waiting for pids.")
    return pids


def test_scale_up(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=1)
    cluster.connect(namespace=SERVE_NAMESPACE)
    # By default, Serve controller and proxy actors use 0 CPUs,
    # so initially there should only be room for 1 replica.

    @serve.deployment(
        version="1", num_replicas=1, health_check_period_s=1, max_concurrent_queries=1
    )
    def D(*args):
        time.sleep(0.1)
        return os.getpid()

    serve.start(detached=True)
    client = serve.context._connect()

    D.deploy()
    pids1 = get_pids(1, app_name="")

    D.options(num_replicas=3).deploy(_blocking=False)

    # Check that a new replica has not started in 1.0 seconds.  This
    # doesn't guarantee that a new replica won't ever be started, but
    # 1.0 seconds is a reasonable upper bound on replica startup time.
    with pytest.raises(TimeoutError):
        client._wait_for_deployment_healthy(D.name, timeout_s=1)
    assert get_pids(1, app_name="") == pids1

    # Add a node with another CPU, another replica should get placed.
    cluster.add_node(num_cpus=1)
    with pytest.raises(TimeoutError):
        client._wait_for_deployment_healthy(D.name, timeout_s=1)
    pids2 = get_pids(2, app_name="")
    assert pids1.issubset(pids2)

    # Add a node with another CPU, the final replica should get placed
    # and the deploy goal should be done.
    cluster.add_node(num_cpus=1)
    client._wait_for_deployment_healthy(D.name)
    pids3 = get_pids(3, app_name="")
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

    @serve.deployment(
        version="1", num_replicas=5, health_check_period_s=1, max_concurrent_queries=1
    )
    def D(*args):
        time.sleep(0.1)
        return os.getpid()

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
        replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(DeploymentID(E.name, ""))
        )
        return replicas.get([replica_state])

    # wait for serve to start the replica, and catch a reference to it.
    wait_for_condition(lambda: len(get_replicas(ReplicaState.STARTING)) > 0)
    replica = get_replicas(ReplicaState.STARTING)[0]

    # currently there are no resources to allocate the replica
    assert replica.check_started()[0] == ReplicaStartupStatus.PENDING_ALLOCATION

    # add the necessary resources to allocate the replica
    cluster.add_node(num_cpus=4)
    wait_for_condition(lambda: (ray.cluster_resources().get("CPU", 0) >= 4))
    wait_for_condition(lambda: (ray.available_resources().get("CPU", 0) >= 2))

    def is_replica_pending_initialization():
        status, _ = replica.check_started()
        print(status)
        return status == ReplicaStartupStatus.PENDING_INITIALIZATION

    wait_for_condition(is_replica_pending_initialization, timeout=25)

    # send signal to complete replica intialization
    signal.send.remote()
    wait_for_condition(
        lambda: replica.check_started()[0] == ReplicaStartupStatus.SUCCEEDED
    )


def test_intelligent_scale_down(ray_cluster):
    cluster = ray_cluster
    # Head node
    cluster.add_node(num_cpus=0)
    cluster.connect(namespace=SERVE_NAMESPACE)
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
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

    @serve.deployment(
        num_replicas=2,
        health_check_period_s=1,
    )
    def D():
        return "hi"

    serve.run(D.bind())

    def get_num_nodes():
        client = get_global_client()
        details = client.get_serve_details()
        dep = details["applications"]["default"]["deployments"]["D"]
        nodes = {r["node_id"] for r in dep["replicas"]}
        print("replica nodes", nodes)
        return len(nodes)

    # Check that the two replicas are spread across the two nodes.
    wait_for_condition(lambda: get_num_nodes() == 2)

    # Kill the worker node. The second replica should get rescheduled on
    # the head node.
    print("Removing worker node. Replica should be rescheduled.")
    cluster.remove_node(worker_node)

    # Check that the replica on the dead node can be rescheduled.
    wait_for_condition(lambda: get_num_nodes() == 1)


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING, reason="Routing FF must be enabled."
)
def test_handle_prefers_replicas_on_same_node(ray_cluster):
    """Verify that handle calls prefer replicas on the same node when possible.

    If all replicas on the same node are occupied (at `max_concurrent_queries` limit),
    requests should spill to other nodes.
    """

    cluster = ray_cluster
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    signal = SignalActor.remote()

    @serve.deployment(num_replicas=2, max_concurrent_queries=1)
    def inner(block_on_signal):
        if block_on_signal:
            ray.get(signal.wait.remote())

        return ray.get_runtime_context().get_node_id()

    @serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0})
    class Outer:
        def __init__(self, inner_handle: RayServeHandle):
            self._h = inner_handle.options(_prefer_local_routing=True)

        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

        async def call_inner(self, block_on_signal: bool = False) -> str:
            return await (await self._h.remote(block_on_signal))

    # The inner deployment's two replicas will be spread across the two nodes and
    # the outer deployment's single replica will be placed on one of them.
    h = serve.run(Outer.bind(inner.bind()))

    # When sending requests sequentially, all requests to the inner deployment should
    # go to the replica on the same node as the outer deployment replica.
    outer_node_id = ray.get(h.get_node_id.remote())
    for _ in range(10):
        assert ray.get(h.call_inner.remote()) == outer_node_id

    # Make a blocking request to the inner deployment replica on the same node.
    blocked_ref = h.call_inner.remote(block_on_signal=True)
    with pytest.raises(TimeoutError):
        ray.get(blocked_ref, timeout=1)

    # Because there's a blocking request and `max_concurrent_queries` is set to 1, all
    # requests should now spill to the other node.
    for _ in range(10):
        assert ray.get(h.call_inner.remote()) != outer_node_id

    ray.get(signal.send.remote())
    assert ray.get(blocked_ref) == outer_node_id


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING, reason="Routing FF must be enabled."
)
@pytest.mark.parametrize("set_flag", [True, False])
def test_proxy_prefers_replicas_on_same_node(ray_cluster: Cluster, set_flag):
    """When the feature flag is turned on via env var, verify that http proxy routes to
    replicas on the same node when possible. Otherwise if env var is not set, http proxy
    should route to all replicas equally.
    """

    if set_flag:
        os.environ["RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING"] = "1"

    cluster = ray_cluster
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    # Only start one HTTP proxy on the head node.
    serve.start(http_options={"location": "HeadOnly"})
    head_node_id = get_head_node_id()

    @serve.deployment(num_replicas=2, max_concurrent_queries=1)
    def f():
        return ray.get_runtime_context().get_node_id()

    # The deployment's two replicas will be spread across the two nodes
    serve.run(f.bind())

    # Since they're sent sequentially, all requests should be routed to
    # the replica on the head node
    responses = [requests.post("http://localhost:8000").text for _ in range(10)]
    if set_flag:
        assert all(resp == head_node_id for resp in responses)
    else:
        assert len(set(responses)) == 2

    if "RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING" in os.environ:
        del os.environ["RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
