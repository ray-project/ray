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
from ray.exceptions import RayActorError
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.deployment_state import ReplicaStartupStatus
from ray.serve._private.utils import calculate_remaining_timeout, get_head_node_id
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import ServeDeploySchema


def get_pids(expected, deployment_name="D", app_name="default", timeout=30):
    handle = serve.get_deployment_handle(deployment_name, app_name)
    pids = set()
    start = time.time()
    while len(pids) < expected:
        for r in [handle.remote() for _ in range(10)]:
            try:
                pids.add(
                    r.result(
                        timeout_s=calculate_remaining_timeout(
                            timeout_s=timeout,
                            start_time_s=start,
                            curr_time_s=time.time(),
                        )
                    )
                )
            except RayActorError:
                # Handle sent request to dead actor before running replicas were updated
                # This can happen because health check period = 1s
                pass

        if time.time() - start >= timeout:
            raise TimeoutError("Timed out waiting for pids.")

    return pids


@serve.deployment(health_check_period_s=1, max_concurrent_queries=1)
def pid():
    time.sleep(0.1)
    return os.getpid()


pid_app = pid.bind()


def test_scale_up(ray_cluster):
    cluster = ray_cluster
    cluster.add_node(num_cpus=1)
    cluster.connect(namespace=SERVE_NAMESPACE)
    # By default, Serve controller and proxy actors use 0 CPUs,
    # so initially there should only be room for 1 replica.

    app_config = {
        "name": "default",
        "import_path": "ray.serve.tests.test_cluster.pid_app",
        "deployments": [{"name": "pid", "num_replicas": 1}],
    }
    serve.start()
    client = serve.context._connect()
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    client._wait_for_application_running("default")
    pids1 = get_pids(1, deployment_name="pid", app_name="default")

    app_config["deployments"][0]["num_replicas"] = 3
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    # Check that a new replica has not started in 1.0 seconds.  This
    # doesn't guarantee that a new replica won't ever be started, but
    # 1.0 seconds is a reasonable upper bound on replica startup time.
    with pytest.raises(TimeoutError):
        client._wait_for_application_running("default", timeout_s=1)
    assert get_pids(1, deployment_name="pid", app_name="default") == pids1

    # Add a node with another CPU, another replica should get placed.
    cluster.add_node(num_cpus=1)
    with pytest.raises(TimeoutError):
        client._wait_for_application_running("default", timeout_s=1)
    pids2 = get_pids(2, deployment_name="pid", app_name="default")
    assert pids1.issubset(pids2)

    # Add a node with another CPU, the final replica should get placed
    # and the deploy goal should be done.
    cluster.add_node(num_cpus=1)
    client._wait_for_application_running("default")
    pids3 = get_pids(3, deployment_name="pid", app_name="default")
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
    serve.start()

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
    serve.start()
    client = _get_global_client()

    signal = SignalActor.remote()

    @serve.deployment(version="1", ray_actor_options={"num_cpus": 2})
    class E:
        async def __init__(self):
            await signal.wait.remote()

    serve._run(E.bind(), _blocking=False)

    def get_replicas(replica_state):
        controller = client._controller
        replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(
                DeploymentID(E.name, "default")
            )
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


@serve.deployment
def f():
    pass


f_app = f.bind()


def test_intelligent_scale_down(ray_cluster):
    cluster = ray_cluster
    # Head node
    cluster.add_node(num_cpus=0)
    cluster.connect(namespace=SERVE_NAMESPACE)
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    serve.start()
    client = _get_global_client()

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

    def check_app_running_with_replicas(num_replicas):
        status = serve.status().applications["default"]
        assert status.status == "RUNNING"
        assert status.deployments["f"].replica_states["RUNNING"] == num_replicas
        return True

    app_config = {
        "name": "default",
        "import_path": "ray.serve.tests.test_cluster.f_app",
        "deployments": [{"name": "f", "num_replicas": 3}],
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(check_app_running_with_replicas, num_replicas=3)
    assert get_actor_distributions() == {2, 1}

    app_config["deployments"][0]["num_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(check_app_running_with_replicas, num_replicas=2)
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
    serve.start()

    worker_node = cluster.add_node(num_cpus=2)

    @serve.deployment(
        num_replicas=2,
        health_check_period_s=1,
    )
    def D():
        return "hi"

    serve.run(D.bind())

    def get_num_nodes():
        client = _get_global_client()
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
        def __init__(self, inner_handle: DeploymentHandle):
            self._h = inner_handle.options(_prefer_local_routing=True)

        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

        async def call_inner(self, block_on_signal: bool = False) -> str:
            return await self._h.remote(block_on_signal)

    # The inner deployment's two replicas will be spread across the two nodes and
    # the outer deployment's single replica will be placed on one of them.
    h = serve.run(Outer.bind(inner.bind()))

    # When sending requests sequentially, all requests to the inner deployment should
    # go to the replica on the same node as the outer deployment replica.
    outer_node_id = h.get_node_id.remote().result()
    for _ in range(10):
        assert h.call_inner.remote().result() == outer_node_id

    # Make a blocking request to the inner deployment replica on the same node.
    blocked_response = h.call_inner.remote(block_on_signal=True)
    with pytest.raises(TimeoutError):
        blocked_response.result(timeout_s=1)

    # Because there's a blocking request and `max_concurrent_queries` is set to 1, all
    # requests should now spill to the other node.
    for _ in range(10):
        assert h.call_inner.remote().result() != outer_node_id

    ray.get(signal.send.remote())
    assert blocked_response.result() == outer_node_id


@pytest.mark.parametrize("set_flag", [True, False])
def test_proxy_prefers_replicas_on_same_node(ray_cluster: Cluster, set_flag):
    """When the feature flag is turned on via env var, verify that http proxy routes to
    replicas on the same node when possible. Otherwise if env var is not set, http proxy
    should route to all replicas equally.
    """

    if not set_flag:
        os.environ["RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING"] = "0"

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
