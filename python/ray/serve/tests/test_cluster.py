import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.cluster_utils import Cluster
from ray.exceptions import RayActorError
from ray.serve._private.common import DeploymentID, DeploymentStatus, ReplicaState
from ray.serve._private.constants import (
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.deployment_state import ReplicaStartupStatus
from ray.serve._private.test_utils import (
    check_deployment_status,
    expected_proxy_actors,
    skip_if_haproxy,
)
from ray.serve._private.utils import calculate_remaining_timeout, get_head_node_id
from ray.serve.config import GangSchedulingConfig
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import ServeDeploySchema
from ray.util.state import list_actors

# Windows CI runs these on a shared, heavily loaded shard where multi-node
# cluster convergence routinely overruns wait_for_condition's 10s default.
WAIT_TIMEOUT_S = 60


def get_pids(expected, deployment_name="D", app_name="default", timeout=WAIT_TIMEOUT_S):
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


@serve.deployment(health_check_period_s=1, max_ongoing_requests=1)
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

    client._wait_for_application_running("default", timeout_s=WAIT_TIMEOUT_S)
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
    client._wait_for_application_running("default", timeout_s=WAIT_TIMEOUT_S)
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

    @serve.deployment(num_replicas=5, health_check_period_s=1, max_ongoing_requests=1)
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

    @serve.deployment(ray_actor_options={"num_cpus": 2})
    class E:
        async def __init__(self):
            await signal.wait.remote()

    serve._run(E.bind(), _blocking=False)

    def get_replicas(replica_state):
        controller = client._controller
        replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(
                DeploymentID(name=E.name)
            )
        )
        return replicas.get([replica_state])

    # wait for serve to start the replica
    wait_for_condition(
        lambda: len(get_replicas(ReplicaState.STARTING)) > 0, timeout=WAIT_TIMEOUT_S
    )

    # currently there are no resources to allocate the replica
    def get_starting_replica():
        replicas = get_replicas(ReplicaState.STARTING)
        return replicas[0] if replicas else None

    def is_pending_allocation():
        replica = get_starting_replica()
        if replica is None:
            return False
        return replica.check_started()[0] == ReplicaStartupStatus.PENDING_ALLOCATION

    wait_for_condition(is_pending_allocation, timeout=WAIT_TIMEOUT_S)

    # add the necessary resources to allocate the replica
    cluster.add_node(num_cpus=4)
    wait_for_condition(
        lambda: (ray.cluster_resources().get("CPU", 0) >= 4), timeout=WAIT_TIMEOUT_S
    )
    wait_for_condition(
        lambda: (ray.available_resources().get("CPU", 0) >= 2), timeout=WAIT_TIMEOUT_S
    )

    def is_replica_pending_initialization():
        replica = get_starting_replica()
        if replica is None:
            return False
        status, _ = replica.check_started()
        return status == ReplicaStartupStatus.PENDING_INITIALIZATION

    wait_for_condition(is_replica_pending_initialization, timeout=WAIT_TIMEOUT_S)

    # send signal to complete replica initialization
    ray.get(signal.send.remote())

    def check_succeeded():
        # After initialization succeeds, replica transitions to RUNNING state
        # So check both STARTING and RUNNING states
        replica = get_starting_replica()
        if replica:
            status, _ = replica.check_started()
            if status == ReplicaStartupStatus.SUCCEEDED:
                return True

        # Check if replica has moved to RUNNING state (which means it succeeded)
        running_replicas = get_replicas(ReplicaState.RUNNING)
        if running_replicas and len(running_replicas) > 0:
            return True

        return False

    wait_for_condition(check_succeeded, timeout=WAIT_TIMEOUT_S)


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_gang_replica_startup_status_transitions(ray_cluster):
    cluster = ray_cluster
    # Start with only 1 CPU — not enough for a gang of 2 replicas each needing 0.75 CPUs.
    cluster.add_node(num_cpus=1)
    cluster.connect(namespace=SERVE_NAMESPACE)
    serve.start()
    client = _get_global_client()

    signal = SignalActor.remote()

    @serve.deployment(
        ray_actor_options={"num_cpus": 0.75},
        num_replicas=2,
        gang_scheduling_config=GangSchedulingConfig(gang_size=2),
    )
    class GangDeployment:
        async def __init__(self):
            await signal.wait.remote()

    serve._run(GangDeployment.bind(), _blocking=False)

    def get_replicas(replica_state):
        controller = client._controller
        replicas = ray.get(
            controller._dump_replica_states_for_testing.remote(
                DeploymentID(name="GangDeployment")
            )
        )
        return replicas.get([replica_state])

    # Wait for replicas to be created in STARTING state.
    wait_for_condition(
        lambda: len(get_replicas(ReplicaState.STARTING)) > 0, timeout=WAIT_TIMEOUT_S
    )

    # With only 1 CPU available and each replica needing 0.75, replicas should
    # be stuck in PENDING_ALLOCATION.
    def is_pending_allocation():
        replicas = get_replicas(ReplicaState.STARTING)
        if not replicas:
            return False
        return all(
            r.check_started()[0] == ReplicaStartupStatus.PENDING_ALLOCATION
            for r in replicas
        )

    wait_for_condition(is_pending_allocation, timeout=WAIT_TIMEOUT_S)

    # Add enough resources for the gang
    cluster.add_node(num_cpus=1)
    wait_for_condition(
        lambda: ray.cluster_resources().get("CPU", 0) == 2, timeout=WAIT_TIMEOUT_S
    )

    # Replicas should transition to PENDING_INITIALIZATION
    def is_pending_initialization():
        replicas = get_replicas(ReplicaState.STARTING)
        if not replicas:
            return False
        return all(
            r.check_started()[0] == ReplicaStartupStatus.PENDING_INITIALIZATION
            for r in replicas
        )

    wait_for_condition(is_pending_initialization, timeout=WAIT_TIMEOUT_S)

    # Complete initialization
    ray.get(signal.send.remote())

    # Replicas should transition to RUNNING
    def check_running():
        running_replicas = get_replicas(ReplicaState.RUNNING)
        return len(running_replicas) == 2

    wait_for_condition(check_running, timeout=WAIT_TIMEOUT_S)


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
        node_to_actors = defaultdict(list)
        for actor in list_actors(
            address=cluster.address, filters=[("STATE", "=", "ALIVE")]
        ):
            if "ServeReplica" not in actor.class_name:
                continue
            node_to_actors[actor.node_id].append(actor)

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
    wait_for_condition(
        check_app_running_with_replicas, num_replicas=3, timeout=WAIT_TIMEOUT_S
    )
    assert get_actor_distributions() == {2, 1}

    app_config["deployments"][0]["num_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        check_app_running_with_replicas, num_replicas=2, timeout=WAIT_TIMEOUT_S
    )
    assert get_actor_distributions() == {2}


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@pytest.mark.skipif(
    RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY, reason="Needs spread strategy."
)
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
    wait_for_condition(lambda: get_num_nodes() == 2, timeout=WAIT_TIMEOUT_S)

    # Kill the worker node. The second replica should get rescheduled on
    # the head node.
    print("Removing worker node. Replica should be rescheduled.")
    cluster.remove_node(worker_node)

    # Check that the replica on the dead node can be rescheduled.
    wait_for_condition(lambda: get_num_nodes() == 1, timeout=WAIT_TIMEOUT_S)


def test_autoscale_upscaling_stuck_then_healthy(ray_cluster):
    """Test that deployment stuck in upscaling (due to insufficient cluster resources)
    recovers to healthy when ongoing requests drop to zero.

    Setup: Head with 0 CPUs + 1 worker with 1 CPU. 1 replica using 1 CPU,
    target_ongoing_requests=1. Send 2 requests via handle -> autoscaler wants 2
    replicas but can't add one (no CPU). Deployment stuck in UPSCALING.
    Release requests -> deployment HEALTHY.
    """
    cluster = ray_cluster
    cluster.add_node(num_cpus=0)  # Head node (controller/proxy use 0 CPU)
    cluster.connect(namespace=SERVE_NAMESPACE)
    serve.start()  # Start before adding worker so controller goes on head
    cluster.add_node(num_cpus=1)  # Worker with 1 CPU for replica
    cluster.wait_for_nodes()

    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "min_replicas": 1,
            "max_replicas": 2,
            "target_ongoing_requests": 1,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 0.5,
            "upscale_delay_s": 0,
            # If delay is large then the test will be stuck in UPSCALING state.
            "downscale_delay_s": 1,
        },
        max_ongoing_requests=1,
        ray_actor_options={"num_cpus": 1},
        graceful_shutdown_timeout_s=2,
    )
    def blocking_replica():
        ray.get(signal.wait.remote())
        return "ok"

    handle = serve.run(blocking_replica.bind())
    wait_for_condition(
        check_deployment_status,
        name="blocking_replica",
        expected_status=DeploymentStatus.HEALTHY,
        timeout=WAIT_TIMEOUT_S,
    )

    # Send 2 requests - first occupies the replica, second queues. With
    # target_ongoing_requests=1 and 1 replica, 2 requests triggers scale to 2.
    responses = [handle.remote() for _ in range(2)]

    # Deployment should get stuck in UPSCALING: autoscaler wants 2 replicas
    # but cluster only has 1 CPU (replica uses it all).
    wait_for_condition(
        check_deployment_status,
        name="blocking_replica",
        expected_status=DeploymentStatus.UPSCALING,
        timeout=WAIT_TIMEOUT_S,
    )

    # Release the signal so running requests complete and go to zero.
    ray.get(signal.send.remote())
    for r in responses:
        assert r.result() == "ok"

    # Deployment should recover to HEALTHY as load drops (may go through
    # DOWNSCALING first if a second replica was briefly added).
    wait_for_condition(
        check_deployment_status,
        name="blocking_replica",
        expected_status=DeploymentStatus.HEALTHY,
        timeout=WAIT_TIMEOUT_S,
    )


def test_handle_prefers_replicas_on_same_node(ray_cluster):
    """Verify that handle calls prefer replicas on the same node when possible.

    If all replicas on the same node are occupied (at `max_ongoing_requests` limit),
    requests should spill to other nodes.
    """

    cluster = ray_cluster
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    signal = SignalActor.remote()

    @serve.deployment(num_replicas=2, max_ongoing_requests=1)
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

    # Because there's a blocking request and `max_ongoing_requests` is set to 1, all
    # requests should now spill to the other node.
    for _ in range(10):
        assert h.call_inner.remote().result() != outer_node_id

    ray.get(signal.send.remote())
    assert blocked_response.result() == outer_node_id


# TODO: HAProxy's default ingress balances across all replicas with no
# node-local preference. prefer-local routing could be wired under HAProxy via
# the ingress_request_router use-server delegation, then this skip dropped.
@skip_if_haproxy("balances across replicas without node-local preference")
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

    @serve.deployment(num_replicas=2, max_ongoing_requests=1)
    def f():
        return ray.get_runtime_context().get_node_id()

    # The deployment's two replicas will be spread across the two nodes
    serve.run(f.bind())

    # Since they're sent sequentially, all requests should be routed to
    # the replica on the head node
    responses = [httpx.post("http://localhost:8000").text for _ in range(10)]
    if set_flag:
        assert all(resp == head_node_id for resp in responses)
    else:
        assert len(set(responses)) == 2

    if "RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING" in os.environ:
        del os.environ["RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING"]


def _replica_id() -> str:
    return (
        f"{ray.get_runtime_context().get_node_id()}:"
        f"{serve.get_replica_context().replica_tag}"
    )


@serve.deployment
class LocalityDownstream:
    async def __call__(self) -> str:
        return _replica_id()


@serve.deployment
class LocalityUpstream:
    def __init__(self, downstream: DeploymentHandle):
        self._downstream = downstream

    async def __call__(self, request) -> str:
        return f"{_replica_id()}|{await self._downstream.remote()}"


locality_app = LocalityUpstream.bind(LocalityDownstream.bind())


def _locality_app_config(node_local_routing: bool) -> dict:
    return {
        "name": SERVE_DEFAULT_APP_NAME,
        "import_path": "ray.serve.tests.test_cluster.locality_app",
        "deployments": [
            {
                "name": "LocalityUpstream",
                "num_replicas": 2,
                "max_replicas_per_node": 1,
                "max_ongoing_requests": 1000,
                # Off so the single head-node proxy spreads the burst across
                # both upstream replicas instead of pinning it to its own node.
                "prefer_local_node_routing": False,
                "prefer_local_az_routing": False,
            },
            {
                "name": "LocalityDownstream",
                "num_replicas": 2,
                "max_replicas_per_node": 1,
                "max_ongoing_requests": 1000,
                "prefer_local_node_routing": node_local_routing,
                "prefer_local_az_routing": False,
            },
        ],
    }


def _send_locality_burst(url: str, num_requests: int = 100) -> List[Tuple[str, str]]:
    """Send `num_requests` concurrently; return (upstream, downstream) id pairs."""
    with ThreadPoolExecutor(max_workers=num_requests) as pool:
        futures = [
            pool.submit(httpx.get, url, timeout=WAIT_TIMEOUT_S)
            for _ in range(num_requests)
        ]
        pairs = []
        for fut in futures:
            response = fut.result()
            assert response.status_code == 200, response.text
            upstream, downstream = response.text.split("|")
            pairs.append((upstream, downstream))
    return pairs


def _assert_node_local_routing(pairs: List[Tuple[str, str]]) -> None:
    """Every request stayed on-node, and each d1 replica mapped to a unique d2 replica."""
    mapping: Dict[str, set] = defaultdict(set)
    for upstream, downstream in pairs:
        assert (
            upstream.split(":", 1)[0] == downstream.split(":", 1)[0]
        ), f"Cross-node hop with node-local routing enabled: {upstream} -> {downstream}"
        mapping[upstream].add(downstream)

    assert len(mapping) == 2, f"Expected both upstream replicas, got {mapping.keys()}"
    assert all(
        len(downstreams) == 1 for downstreams in mapping.values()
    ), f"An upstream replica reached multiple downstream replicas: {dict(mapping)}"
    downstreams_reached = {next(iter(ds)) for ds in mapping.values()}
    assert (
        len(downstreams_reached) == 2
    ), f"Both upstream replicas reached the same downstream replica: {dict(mapping)}"


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@skip_if_haproxy("balances across replicas without node-local preference")
def test_node_local_routing_between_deployments(ray_cluster: Cluster):
    """Replica-to-replica traffic stays on-node when prefer_local_node_routing is on.

    Two nodes, two deployments (d1 -> d2), two replicas each, pinned one-per-node:
    n1 hosts d1r1 and d2r1; n2 hosts d1r2 and d2r2. max_ongoing_requests=1000 so
    nothing spills for capacity. d1's flag is off so the head-node proxy spreads
    across both d1 replicas; d2's flag is the one under test.
    """
    cluster = ray_cluster
    # Start Serve on the head node before adding the worker so the controller
    # and the single HeadOnly proxy land on n1.
    cluster.add_node(num_cpus=2)
    cluster.connect(namespace=SERVE_NAMESPACE)
    serve.start(http_options={"location": "HeadOnly"})
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    client = serve.context._connect()
    client.deploy_apps(
        ServeDeploySchema(**{"applications": [_locality_app_config(True)]})
    )
    client._wait_for_application_running(
        SERVE_DEFAULT_APP_NAME, timeout_s=WAIT_TIMEOUT_S
    )

    def replicas_spread_across_nodes() -> bool:
        details = client.get_serve_details()
        deployments = details["applications"][SERVE_DEFAULT_APP_NAME]["deployments"]
        for name in ("LocalityUpstream", "LocalityDownstream"):
            replicas = deployments[name]["replicas"]
            nodes = {replica["node_id"] for replica in replicas}
            assert len(replicas) == 2, replicas
            assert len(nodes) == 2, nodes
        return True

    wait_for_condition(replicas_spread_across_nodes, timeout=WAIT_TIMEOUT_S)

    url = "http://localhost:8000"
    warmed_up: List[Tuple[str, str]] = []

    def both_upstream_replicas_warmed_up() -> bool:
        # Sequential warmup so each replica's router is initialized before
        # the burst. A cold concurrent burst can time out queue-length probes
        # and spill off-node even with locality routing enabled.
        response = httpx.get(url, timeout=WAIT_TIMEOUT_S)
        assert response.status_code == 200, response.text
        upstream, downstream = response.text.split("|")
        warmed_up.append((upstream, downstream))
        return len({u for u, _ in warmed_up}) == 2

    wait_for_condition(
        both_upstream_replicas_warmed_up,
        timeout=WAIT_TIMEOUT_S,
        retry_interval_ms=100,
    )

    _assert_node_local_routing(_send_locality_burst(url))

    # Lightweight config update: replicas stay up, routers pick up the new
    # flag over long poll. There is no status signal for that, so retry the
    # burst until cross-node hops appear.
    client.deploy_apps(
        ServeDeploySchema(**{"applications": [_locality_app_config(False)]})
    )

    def routing_is_spread() -> bool:
        pairs = _send_locality_burst(url)
        crossed = sum(
            1
            for upstream, downstream in pairs
            if upstream.split(":", 1)[0] != downstream.split(":", 1)[0]
        )
        downstreams = {downstream for _, downstream in pairs}
        return crossed > 10 and len(downstreams) == 2

    wait_for_condition(
        routing_is_spread,
        timeout=WAIT_TIMEOUT_S,
        retry_interval_ms=1000,
    )


class TestHealthzAndRoutes:
    def test_head_node_proxy_healthy(self, ray_cluster: Cluster):
        """When a new cluster is started with no replicas, head node proxy should
        respond with 200 at /-/healthz and /-/routes"""

        cluster = ray_cluster
        cluster.add_node(num_cpus=0)  # Head node
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start(http_options={"location": "EveryNode"})

        @serve.deployment(ray_actor_options={"num_cpus": 0})
        class Dummy:
            pass

        serve.run(Dummy.bind())

        # Head node proxy /-/healthz and /-/routes should return 200
        r = httpx.post("http://localhost:8000/-/healthz")
        assert r.status_code == 200
        r = httpx.post("http://localhost:8000/-/routes")
        assert r.status_code == 200

    def test_head_and_worker_nodes_no_replicas(self, ray_cluster: Cluster):
        """Test `/-/healthz` and `/-/routes` return the correct responses for head and
        worker nodes.

        When there are replicas on all nodes, `/-/healthz` and `/-/routes` on all nodes
        should return 200. When there are no replicas on any nodes, `/-/healthz` and
        `/-/routes` on the head node should continue to return 200. `/-/healthz` and
        `/-/routes` on the worker node should start to return 503
        """
        # Setup worker http proxy to be pointing to port 8001. Head node http proxy will
        # continue to be pointing to the default port 8000.
        cluster = ray_cluster
        cluster.add_node(num_cpus=0)
        cluster.add_node(
            num_cpus=2, env_vars={"RAY_SERVE_WORKER_PROXY_HTTP_PORT": "8001"}
        )
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        serve.start(http_options={"location": "EveryNode"})

        # Deploy 2 replicas, both should be on the worker node.
        @serve.deployment(num_replicas=2)
        class HelloModel:
            def __call__(self):
                return "hello"

        model = HelloModel.bind()
        serve.run(target=model)

        # Ensure worker node has both replicas.
        def check_replicas_on_worker_nodes():
            return (
                len(
                    {
                        a.node_id
                        for a in list_actors(
                            address=cluster.address,
                            filters=[("STATE", "=", "ALIVE")],
                        )
                        if a.class_name.startswith("ServeReplica")
                    }
                )
                == 1
            )

        wait_for_condition(check_replicas_on_worker_nodes, timeout=WAIT_TIMEOUT_S)

        # Total alive actors: EveryNode proxies on both nodes + 1 controller +
        # 2 replicas. Under HAProxy each proxy node runs an HAProxyManager and
        # the head node adds a fallback ProxyActor.
        expected_num_actors = (
            sum(expected_proxy_actors(num_proxy_nodes=2).values()) + 1 + 2
        )
        wait_for_condition(
            lambda: len(
                list_actors(address=cluster.address, filters=[("STATE", "=", "ALIVE")])
            )
            == expected_num_actors,
            timeout=WAIT_TIMEOUT_S,
        )
        assert len(ray.nodes()) == 2

        # Ensure `/-/healthz` and `/-/routes` return 200 and expected responses
        # on both nodes.
        def check_request(url: str, expected_code: int, expected_text: str):
            req = httpx.get(url)
            assert req.status_code == expected_code
            assert req.text == expected_text
            return True

        wait_for_condition(
            condition_predictor=check_request,
            url="http://127.0.0.1:8000/-/healthz",
            expected_code=200,
            expected_text="success",
            timeout=WAIT_TIMEOUT_S,
        )
        assert httpx.get("http://127.0.0.1:8000/-/routes").status_code == 200
        assert httpx.get("http://127.0.0.1:8000/-/routes").text == '{"/":"default"}'
        wait_for_condition(
            condition_predictor=check_request,
            url="http://127.0.0.1:8001/-/healthz",
            expected_code=200,
            expected_text="success",
            timeout=WAIT_TIMEOUT_S,
        )
        assert httpx.get("http://127.0.0.1:8001/-/routes").status_code == 200
        assert httpx.get("http://127.0.0.1:8001/-/routes").text == '{"/":"default"}'

        # Deleting the deployment drops the replicas on all nodes. The proxies and
        # controller stay alive (the worker proxy drains), so the count is the
        # pre-delete total minus the 2 replicas.
        serve.delete(name=SERVE_DEFAULT_APP_NAME)

        expected_num_actors_after_delete = (
            sum(expected_proxy_actors(num_proxy_nodes=2).values()) + 1
        )
        wait_for_condition(
            lambda: len(
                list_actors(address=cluster.address, filters=[("STATE", "=", "ALIVE")])
            )
            == expected_num_actors_after_delete,
            timeout=WAIT_TIMEOUT_S,
        )

        # Ensure head node `/-/healthz` and `/-/routes` continue to
        # return 200 and expected responses. Also, the worker node
        # `/-/healthz` and `/-/routes` should return 503 and unavailable
        # responses.
        wait_for_condition(
            condition_predictor=check_request,
            url="http://127.0.0.1:8000/-/healthz",
            expected_code=200,
            expected_text="success",
            timeout=WAIT_TIMEOUT_S,
        )
        wait_for_condition(
            condition_predictor=check_request,
            url="http://127.0.0.1:8000/-/routes",
            expected_code=200,
            expected_text="{}",
            timeout=WAIT_TIMEOUT_S,
        )
        wait_for_condition(
            condition_predictor=check_request,
            url="http://127.0.0.1:8001/-/healthz",
            expected_code=503,
            expected_text="This node is being drained.",
            timeout=WAIT_TIMEOUT_S,
        )
        wait_for_condition(
            condition_predictor=check_request,
            url="http://127.0.0.1:8001/-/routes",
            expected_code=503,
            expected_text="This node is being drained.",
            timeout=WAIT_TIMEOUT_S,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
