import asyncio
import logging
import os
import sys
import tempfile
import time
import zipfile
from typing import Dict, Iterable, List
from unittest import mock

import pytest
import requests

import ray
import ray.util.state as state_api
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusTrigger,
    ReplicaID,
    ReplicaState,
)
from ray.serve._private.constants import (
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.controller import ServeController
from ray.serve._private.test_utils import (
    check_deployment_status,
    check_num_replicas_eq,
    check_num_replicas_gte,
    check_num_replicas_lte,
    get_num_alive_replicas,
)
from ray.serve.config import AutoscalingConfig
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import ServeDeploySchema
from ray.util.state import list_actors


def get_running_replica_ids(name: str, controller: ServeController) -> List[ReplicaID]:
    """Get the replica tags of running replicas for given deployment"""
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(DeploymentID(name=name))
    )
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return [replica.replica_id for replica in running_replicas]


def get_deployment_start_time(controller: ServeController, name: str):
    """Return start time for given deployment"""
    deployments = ray.get(controller.list_deployments_internal.remote())
    deployment_info, _ = deployments[DeploymentID(name=name)]
    return deployment_info.start_time_ms


def assert_no_replicas_deprovisioned(
    replica_ids_1: Iterable[ReplicaID], replica_ids_2: Iterable[ReplicaID]
) -> None:
    """
    Checks whether any replica ids from replica_ids_1 are absent from
    replica_ids_2. Assumes that this indicates replicas were de-provisioned.

    replica_ids_1: Replica ids of running replicas at the first timestep
    replica_ids_2: Replica ids of running replicas at the second timestep
    """

    replica_ids_1, replica_ids_2 = set(replica_ids_1), set(replica_ids_2)
    num_matching_replicas = len(replica_ids_1.intersection(replica_ids_2))

    print(
        f"{num_matching_replicas} replica(s) stayed provisioned between "
        f"both deployments. All {len(replica_ids_1)} replica(s) were "
        f"expected to stay provisioned. "
        f"{len(replica_ids_1) - num_matching_replicas} replica(s) were "
        f"de-provisioned."
    )

    assert len(replica_ids_1) == num_matching_replicas


def test_assert_no_replicas_deprovisioned():
    deployment_id = DeploymentID(name="hi")
    replica_ids_1 = [
        ReplicaID("a", deployment_id=deployment_id),
        ReplicaID("b", deployment_id=deployment_id),
        ReplicaID("c", deployment_id=deployment_id),
    ]

    replica_ids_2 = [
        ReplicaID("a", deployment_id=deployment_id),
        ReplicaID("b", deployment_id=deployment_id),
        ReplicaID("c", deployment_id=deployment_id),
        ReplicaID("d", deployment_id=deployment_id),
        ReplicaID("e", deployment_id=deployment_id),
    ]

    assert_no_replicas_deprovisioned(replica_ids_1, replica_ids_2)
    with pytest.raises(AssertionError):
        assert_no_replicas_deprovisioned(replica_ids_2, replica_ids_1)


def get_num_requests(client, dep_id: DeploymentID):
    ref = client._controller._dump_autoscaling_metrics_for_testing.remote()
    total_num_requests = ray.get(ref)[dep_id]
    print("total num requests", total_num_requests)
    return total_num_requests


def check_num_requests_eq(client, id: DeploymentID, expected: int):
    assert get_num_requests(client, id) == expected
    return True


def check_num_requests_ge(client, id: DeploymentID, expected: int):
    assert get_num_requests(client, id) >= expected
    return True


class TestAutoscalingMetrics:
    def test_basic(self, serve_instance):
        """Test that request metrics are sent correctly to the controller."""

        client = serve_instance
        signal = SignalActor.remote()

        @serve.deployment(
            autoscaling_config={
                "metrics_interval_s": 0.1,
                "min_replicas": 1,
                "max_replicas": 10,
                "target_ongoing_requests": 10,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "look_back_period_s": 1,
            },
            # We will send many requests. This will make sure replicas are
            # killed quickly during cleanup.
            graceful_shutdown_timeout_s=1,
            max_ongoing_requests=25,
            version="v1",
        )
        class A:
            async def __call__(self):
                await signal.wait.remote()

        handle = serve.run(A.bind())
        dep_id = DeploymentID(name="A")
        [handle.remote() for _ in range(50)]

        # Wait for metrics to propagate
        wait_for_condition(check_num_requests_ge, client=client, id=dep_id, expected=1)
        print("Autoscaling metrics started recording on controller.")

        # Many queries should be inflight.
        wait_for_condition(check_num_requests_ge, client=client, id=dep_id, expected=45)
        print("Confirmed many queries are inflight.")

        wait_for_condition(check_num_replicas_eq, name="A", target=5)
        print("Confirmed deployment scaled to 5 replicas.")
        print("Releasing signal.")
        signal.send.remote()

        # After traffic stops, num replica should drop to 1
        wait_for_condition(check_num_replicas_eq, name="A", target=1, timeout=15)
        print("Num replicas dropped to 1.")

        # Request metrics should drop to 0
        wait_for_condition(check_num_requests_eq, client=client, id=dep_id, expected=0)
        print("Queued and ongoing requests dropped to 0.")

    @pytest.mark.parametrize("use_generator", [True, False])
    def test_replicas_die(self, serve_instance_with_signal, use_generator):
        """If replicas die while requests are still executing, that
        should be tracked correctly."""

        client, signal = serve_instance_with_signal

        config = {
            "autoscaling_config": {
                "target_ongoing_requests": 10,
                "metrics_interval_s": 0.1,
                "min_replicas": 1,
                "max_replicas": 10,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "look_back_period_s": 1,
            },
            "graceful_shutdown_timeout_s": 0.1,
            "max_ongoing_requests": 25,
        }

        if use_generator:

            @serve.deployment(**config)
            class A:
                async def __call__(self):
                    await signal.wait.remote()
                    async for i in range(3):
                        yield i

        else:

            @serve.deployment(**config)
            class A:
                async def __call__(self):
                    await signal.wait.remote()

        handle = serve.run(A.bind(), name="app1").options(stream=use_generator)
        dep_id = DeploymentID(name="A", app_name="app1")
        [handle.remote() for _ in range(50)]

        # Many queries should be inflight.
        wait_for_condition(check_num_requests_ge, client=client, id=dep_id, expected=45)
        print("Confirmed many queries are inflight.")

        wait_for_condition(check_num_replicas_eq, name="A", target=5, app_name="app1")
        print("Confirmed deployment scaled to 5 replicas.")

        # Wait for all requests to be scheduled to replicas so they'll be failed
        # when the replicas are removed.
        wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 50)

        # Remove all replicas before they can finish the requests.
        serve.delete("app1")

        # Num requests should still drop to 0 despite all requests failing.
        def check_handle_metrics(handle):
            num_requests = handle._router._metrics_manager.num_requests_sent_to_replicas
            for replica_id, num in num_requests.items():
                assert (
                    num == 0
                ), f"Replica {replica_id} still has {num} ongoing requests"

            return True

        wait_for_condition(check_handle_metrics, handle=handle)

    @pytest.mark.skipif(
        not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
        reason="Needs metric collection at handle.",
    )
    @pytest.mark.parametrize("use_get_handle_api", [True, False])
    def test_handle_deleted_on_crashed_replica(
        self, serve_instance_with_signal, use_get_handle_api
    ):
        """If a Serve replica crashes, the metrics from handles living on that replica
        should be dropped.
        """

        client, signal = serve_instance_with_signal
        dep_id = DeploymentID(name="A")

        @serve.deployment(
            autoscaling_config={
                "target_ongoing_requests": 4,
                "metrics_interval_s": 0.1,
                "min_replicas": 0,
                "max_replicas": 10,
                "upscale_delay_s": 1,
                "downscale_delay_s": 1,
                "look_back_period_s": 10,
            },
            graceful_shutdown_timeout_s=0.1,
            health_check_period_s=1,
            max_ongoing_requests=10,
        )
        class A:
            async def __call__(self):
                await signal.wait.remote()
                return "sup"

        @serve.deployment(graceful_shutdown_timeout_s=1, max_ongoing_requests=50)
        class Router:
            def __init__(self, handle: DeploymentHandle):
                if use_get_handle_api:
                    self._handle = serve.get_deployment_handle("A")
                else:
                    self._handle = handle

            async def __call__(self):
                return await self._handle.remote()

        app = Router.bind(A.bind())
        handle = serve.run(app)
        [handle.remote() for _ in range(20)]

        # Wait for deployment A to scale up
        wait_for_condition(check_num_requests_eq, client=client, id=dep_id, expected=20)
        wait_for_condition(check_num_replicas_eq, name="A", target=5)
        print("Confirmed deployment scaled to 5 replicas.")

        router_info = [
            actor
            for actor in list_actors(filters=[("state", "=", "ALIVE")])
            if actor["class_name"] == "ServeReplica:default:Router"
        ][0]
        router = ray.get_actor(router_info["name"], namespace=SERVE_NAMESPACE)

        # Kill Router replica
        print(f"Killing Router ({router_info['actor_id']}) at", time.time())
        ray.kill(router)

        wait_for_condition(check_num_replicas_eq, name="A", target=0)
        wait_for_condition(check_num_requests_eq, client=client, id=dep_id, expected=0)

        # Wait for new Router replica to start, so we avoid potential
        # race conditions during test shutdown.
        # (Ex: controller starts a new Router replica, before the replica
        # initializes the test shutdown procedure deletes the Router
        # deployment, replica initializes and tries to get deployment
        # handle to `A` and fails.)
        wait_for_condition(check_num_replicas_eq, name="Router", target=1)

    @pytest.mark.skipif(
        not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
        reason="Needs metric collection at handle.",
    )
    def test_handle_deleted_on_non_serve_actor(self, serve_instance_with_signal):
        """If handles are deleted while requests are still inflight, the
        metrics should be invalidated after a certain time so the info
        doesn't become stale. This is the fallback for handles that don't
        live on serve actors.
        """

        client, signal = serve_instance_with_signal
        dep_id = DeploymentID(name="A")

        @serve.deployment(
            autoscaling_config={
                "target_ongoing_requests": 4,
                "metrics_interval_s": 0.1,
                "min_replicas": 0,
                "max_replicas": 10,
                "upscale_delay_s": 1,
                "downscale_delay_s": 1,
                "look_back_period_s": 10,
            },
            graceful_shutdown_timeout_s=0.1,
            health_check_period_s=1,
            max_ongoing_requests=10,
        )
        class A:
            async def __call__(self):
                await signal.wait.remote()
                return "sup"

        @ray.remote
        class CallActor:
            def __init__(self):
                self._handle = DeploymentHandle("A", "default")

            async def call(self):
                return await self._handle.remote()

        serve.run(A.bind())
        caller = CallActor.options(name="caller", namespace="abc").remote()
        [caller.call.remote() for _ in range(20)]

        # Wait for deployment A to scale up
        wait_for_condition(check_num_requests_eq, client=client, id=dep_id, expected=20)
        wait_for_condition(check_num_replicas_eq, name="A", target=5)
        print("Confirmed deployment scaled to 5 replicas.")

        # Kill CallerActor
        print("Killing CallerActor at", time.time())
        ray.kill(ray.get_actor("caller", namespace="abc"))

        wait_for_condition(check_num_replicas_eq, name="A", target=0, timeout=20)
        wait_for_condition(
            check_num_requests_eq, client=client, id=dep_id, expected=0, timeout=20
        )


@pytest.mark.parametrize("min_replicas", [1, 2])
def test_e2e_scale_up_down_basic(min_replicas, serve_instance_with_signal):
    """Send 100 requests and check that we autoscale up, and then back down."""

    client, signal = serve_instance_with_signal

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": min_replicas,
            "max_replicas": 3,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.5,
            "upscale_delay_s": 0,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_ongoing_requests=1000,
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(client._controller, "A")

    [handle.remote() for _ in range(100)]

    # scale up one more replica from min_replicas
    wait_for_condition(check_num_replicas_gte, name="A", target=min_replicas + 1)
    # check_deployment_status(controller, "A", DeploymentStatus.UPSCALING)
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(check_num_replicas_lte, name="A", target=min_replicas)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(client._controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("scaling_factor", [1, 0.2])
@pytest.mark.parametrize("use_upscale_downscale_config", [True, False])
@mock.patch("ray.serve._private.router.HANDLE_METRIC_PUSH_INTERVAL_S", 1)
def test_e2e_scale_up_down_with_0_replica(
    serve_instance_with_signal,
    scaling_factor,
    use_upscale_downscale_config,
):
    """Send 100 requests and check that we autoscale up, and then back down."""

    client, signal = serve_instance_with_signal
    controller = client._controller

    autoscaling_config = {
        "metrics_interval_s": 0.1,
        "min_replicas": 0,
        "max_replicas": 2,
        "look_back_period_s": 0.2,
        "downscale_delay_s": 0.5,
        "upscale_delay_s": 0,
    }
    if use_upscale_downscale_config:
        autoscaling_config["upscaling_factor"] = scaling_factor
        autoscaling_config["downscaling_factor"] = scaling_factor
    else:
        autoscaling_config["smoothing_factor"] = scaling_factor

    @serve.deployment(
        autoscaling_config=autoscaling_config,
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_ongoing_requests=1000,
        version="v1",
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    results = [handle.remote() for _ in range(100)]

    # After the blocking requests are sent, the number of replicas
    # should increase.
    wait_for_condition(check_num_replicas_gte, name="A", target=1)
    # Release the signal, which should unblock all requests.
    print("Number of replicas reached at least 1, releasing signal.")
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(check_num_replicas_eq, name="A", target=0)
    # Make sure no requests were dropped.
    # If the deployment (unexpectedly) scaled down before the
    # blocking signal was released, chances are some requests failed b/c
    # they were assigned to a replica that died. Therefore, this for
    # loop is intended to help make sure that didn't happen.
    for res in results:
        res.result()

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@mock.patch.object(ServeController, "run_control_loop")
def test_initial_num_replicas(mock, serve_instance):
    """assert that the inital amount of replicas a deployment is launched with
    respects the bounds set by autoscaling_config.

    For this test we mock out the run event loop, make sure the number of
    replicas is set correctly before we hit the autoscaling procedure.
    """

    @serve.deployment(
        autoscaling_config={
            "min_replicas": 2,
            "max_replicas": 4,
        },
    )
    class A:
        def __call__(self):
            return "ok!"

    serve.run(A.bind())
    check_num_replicas_eq("A", 2)


def test_cold_start_time(serve_instance):
    """Test a request is served quickly by a deployment that's scaled to zero"""

    @serve.deployment(
        autoscaling_config={
            "min_replicas": 0,
            "max_replicas": 1,
            "look_back_period_s": 0.2,
        },
    )
    class A:
        def __call__(self):
            return "hello"

    handle = serve.run(A.bind())

    def check_running():
        assert serve.status().applications["default"].status == "RUNNING"
        return True

    wait_for_condition(check_running)

    assert requests.post("http://localhost:8000/-/healthz").status_code == 200
    assert requests.post("http://localhost:8000/-/routes").status_code == 200

    start = time.time()
    result = handle.remote().result()
    cold_start_time = time.time() - start
    if sys.platform == "win32":
        timeout = 5  # Windows has a longer tail.
    else:
        timeout = 3
    assert cold_start_time < timeout
    print(
        "Time taken for deployment at 0 replicas to serve first request:",
        cold_start_time,
    )
    assert result == "hello"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_bursty(serve_instance_with_signal):
    """
    Sends 100 requests in bursts. Uses delays for smooth provisioning.
    """

    client, signal = serve_instance_with_signal
    controller = client._controller

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 2,
            "look_back_period_s": 0.5,
            "downscale_delay_s": 0.5,
            "upscale_delay_s": 0.5,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_ongoing_requests=1000,
        version="v1",
    )
    class A:
        def __init__(self):
            logging.getLogger("ray.serve").setLevel(logging.ERROR)

        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    [handle.remote() for _ in range(100)]

    wait_for_condition(check_num_replicas_gte, name="A", target=2)

    num_replicas = get_num_alive_replicas("A")
    signal.send.remote()

    # Execute a bursty workload that issues 100 requests every 0.05 seconds
    # The SignalActor allows all requests in a burst to be queued before they
    # are all executed, which increases the
    # target_in_flight_requests_per_replica. Then the send method will bring
    # it back to 0. This bursty behavior should be smoothed by the delay
    # parameters.
    for _ in range(5):
        ray.get(signal.send.remote(clear=True))
        check_num_replicas_eq("A", num_replicas)
        responses = [handle.remote() for _ in range(100)]
        signal.send.remote()
        [r.result() for r in responses]
        time.sleep(0.05)

    # As the queue is drained, we should scale back down.
    wait_for_condition(check_num_replicas_lte, name="A", target=1)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@mock.patch("ray.serve._private.router.HANDLE_METRIC_PUSH_INTERVAL_S", 1)
def test_e2e_intermediate_downscaling(serve_instance_with_signal):
    """
    Scales up, then down, and up again.
    """

    client, signal = serve_instance_with_signal
    controller = client._controller

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 0,
            "max_replicas": 20,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.2,
            "upscale_delay_s": 0.2,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_ongoing_requests=1000,
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    [handle.remote() for _ in range(50)]

    wait_for_condition(check_num_replicas_gte, name="A", target=20, timeout=30)
    signal.send.remote()

    wait_for_condition(check_num_replicas_lte, name="A", target=1, timeout=30)
    signal.send.remote(clear=True)

    [handle.remote() for _ in range(50)]
    wait_for_condition(check_num_replicas_gte, name="A", target=20, timeout=30)

    signal.send.remote()
    # As the queue is drained, we should scale back down.
    wait_for_condition(check_num_replicas_eq, name="A", target=0, timeout=30)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.parametrize("initial_replicas", [2, 3])
@pytest.mark.parametrize("use_deprecated_smoothing_factor", [True, False])
def test_downscaling_with_fractional_scaling_factor(
    serve_instance_with_signal,
    initial_replicas: int,
    use_deprecated_smoothing_factor: bool,
):
    client, signal = serve_instance_with_signal
    signal.send.remote(clear=True)

    app_config = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "autoscaling_config": {
                    "metrics_interval_s": 0.1,
                    "min_replicas": 0,
                    "max_replicas": 5,
                    "initial_replicas": initial_replicas,
                    "look_back_period_s": 0.2,
                    "downscale_delay_s": 5,
                },
                "graceful_shutdown_timeout_s": 1,
                "max_ongoing_requests": 1000,
            }
        ],
    }
    if use_deprecated_smoothing_factor:
        app_config["deployments"][0]["autoscaling_config"][
            "downscale_smoothing_factor"
        ] = 0.5
    else:
        app_config["deployments"][0]["autoscaling_config"]["downscaling_factor"] = 0.5

    # Deploy with initial replicas = 2+, smoothing factor = 0.5
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )

    # Send a blocked request to one of two replicas.
    # Deployment should still have the initial number of replicas since
    # downscale delay = 5
    h = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    h.remote()
    check_num_replicas_eq("A", initial_replicas)

    # There is 1 ongoing (blocked) request and 2+ replicas. The
    # deployment should autoscale down to 1 replica despite the
    # smoothing factor
    current_num_replicas = initial_replicas
    while current_num_replicas > 1:
        wait_for_condition(
            check_num_replicas_eq, name="A", target=current_num_replicas - 1
        )
        current_num_replicas -= 1
        print(f"Deployment has downscaled to {current_num_replicas} replicas.")

    # Release signal so we don't get an ugly error message from the
    # replica when the signal actor goes out of scope and gets killed
    ray.get(signal.send.remote())


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.skip(reason="Currently failing with undefined behavior")
def test_e2e_update_autoscaling_deployment(serve_instance_with_signal):
    # See https://github.com/ray-project/ray/issues/21017 for details

    client, signal = serve_instance_with_signal
    controller = client._controller

    app_config = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "autoscaling_config": {
                    "metrics_interval_s": 0.1,
                    "min_replicas": 0,
                    "max_replicas": 10,
                    "look_back_period_s": 0.2,
                    "downscale_delay_s": 0.2,
                    "upscale_delay_s": 0.2,
                },
                "graceful_shutdown_timeout_s": 1,
                "max_ongoing_requests": 1000,
            }
        ],
    }

    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Deployed A with min_replicas 1 and max_replicas 10.")
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    handle = serve.get_deployment_handle("A", "default")
    start_time = get_deployment_start_time(controller, "A")

    check_num_replicas_eq("A", 0)
    [handle.remote() for _ in range(400)]
    print("Issued 400 requests.")

    wait_for_condition(check_num_replicas_gte, name="A", target=10)
    print("Scaled to 10 replicas.")
    first_deployment_replicas = get_running_replica_ids("A", controller)

    check_num_replicas_lte("A", 20)

    [handle.remote() for _ in range(458)]
    time.sleep(3)
    print("Issued 458 requests. Request routing in-progress.")

    app_config["deployments"][0]["autoscaling_config"]["min_replicas"] = 2
    app_config["deployments"][0]["autoscaling_config"]["max_replicas"] = 20
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Redeployed A.")

    wait_for_condition(check_num_replicas_gte, name="A", target=20)
    print("Scaled up to 20 requests.")
    second_deployment_replicas = get_running_replica_ids("A", controller)

    # Confirm that none of the original replicas were de-provisioned
    assert_no_replicas_deprovisioned(
        first_deployment_replicas, second_deployment_replicas
    )

    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(check_num_replicas_lte, name="A", target=2)
    check_num_replicas_gte("A", 2)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time

    # scale down to 0
    app_config["deployments"][0]["autoscaling_config"]["min_replicas"] = 0
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Redeployed A.")
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )

    wait_for_condition(check_num_replicas_eq, name="A", target=0)
    check_num_replicas_eq("A", 0)

    # scale up
    [handle.remote() for _ in range(400)]
    wait_for_condition(check_num_replicas_gte, name="A", target=0)
    signal.send.remote()
    wait_for_condition(check_num_replicas_eq, name="A", target=0)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_raise_min_replicas(serve_instance_with_signal):
    """Raise min replicas from 0 to 2."""

    client, signal = serve_instance_with_signal
    controller = client._controller

    app_config = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "autoscaling_config": {
                    "metrics_interval_s": 0.1,
                    "min_replicas": 0,
                    "max_replicas": 10,
                    "look_back_period_s": 0.2,
                    "downscale_delay_s": 0.2,
                    "upscale_delay_s": 0.2,
                },
                "graceful_shutdown_timeout_s": 1,
                "max_ongoing_requests": 1000,
            }
        ],
    }

    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Deployed A.")
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    check_num_replicas_eq("A", 0)

    handle = serve.get_deployment_handle("A", "default")
    handle.remote()
    print("Issued one request.")

    wait_for_condition(check_num_replicas_eq, name="A", target=1, timeout=2)
    print("Scaled up to 1 replica.")

    first_deployment_replicas = get_running_replica_ids("A", controller)

    app_config["deployments"][0]["autoscaling_config"]["min_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Redeployed A with min_replicas set to 2.")
    wait_for_condition(
        check_deployment_status, name="A", expected_status=DeploymentStatus.HEALTHY
    )

    # Confirm that autoscaler doesn't scale above 2 even after waiting
    with pytest.raises(RuntimeError, match="timeout"):
        wait_for_condition(check_num_replicas_gte, name="A", target=3, timeout=5)
    print("Autoscaled to 2 without issuing any new requests.")

    second_deployment_replicas = get_running_replica_ids("A", controller)

    # Confirm that none of the original replicas were de-provisioned
    assert_no_replicas_deprovisioned(
        first_deployment_replicas, second_deployment_replicas
    )

    signal.send.remote()
    time.sleep(1)
    print("Completed request.")

    # As the queue is drained, we should scale back down.
    wait_for_condition(check_num_replicas_lte, name="A", target=2)
    check_num_replicas_gte("A", 2)
    print("Stayed at 2 replicas.")

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_initial_replicas(serve_instance):
    @serve.deployment(
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            initial_replicas=2,
            max_replicas=5,
            downscale_delay_s=3,
        ),
    )
    def f():
        return os.getpid()

    serve.run(f.bind())
    check_num_replicas_eq("f", target=2)

    # f should scale down to min_replicas (1) deployments
    wait_for_condition(check_num_replicas_eq, name="f", target=1, timeout=20)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_preserve_prev_replicas(serve_instance_with_signal):
    _, signal = serve_instance_with_signal

    @serve.deployment(
        max_ongoing_requests=5,
        # The config makes the deployment scale up really quickly and then
        # wait nearly forever to downscale.
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            max_replicas=2,
            downscale_delay_s=600,
            upscale_delay_s=0,
            metrics_interval_s=1,
            look_back_period_s=1,
        ),
    )
    def scaler():
        ray.get(signal.wait.remote())
        time.sleep(0.2)
        return os.getpid()

    handle = serve.run(scaler.bind())
    dep_id = DeploymentID(name="scaler")
    responses = [handle.remote() for _ in range(10)]

    wait_for_condition(
        check_num_replicas_eq,
        name="scaler",
        target=2,
        retry_interval_ms=1000,
        timeout=20,
    )

    ray.get(signal.send.remote())

    pids = {r.result() for r in responses}
    assert len(pids) == 2

    # Now re-deploy the application, make sure it is still 2 replicas and it shouldn't
    # be scaled down.
    handle = serve.run(scaler.bind())
    responses = [handle.remote() for _ in range(10)]
    pids = {r.result() for r in responses}
    assert len(pids) == 2

    def check_num_replicas(live: int, dead: int):
        live_actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "ALIVE"),
            ]
        )
        dead_actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "DEAD"),
            ]
        )

        return len(live_actors) == live and len(dead_actors) == dead

    wait_for_condition(
        check_num_replicas, retry_interval_ms=1000, timeout=20, live=2, dead=2
    )
    ray.get(signal.send.remote())

    # re-deploy the application with initial_replicas. This should override the
    # previous number of replicas.
    scaler = scaler.options(
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            initial_replicas=3,
            max_replicas=5,
            downscale_delay_s=600,
            upscale_delay_s=600,
            metrics_interval_s=1,
            look_back_period_s=1,
        )
    )
    handle = serve.run(scaler.bind())
    responses = [handle.remote() for _ in range(15)]
    pids = {r.result() for r in responses}
    assert len(pids) == 3

    wait_for_condition(
        check_num_replicas, retry_interval_ms=1000, timeout=20, live=3, dead=4
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_preserve_prev_replicas_rest_api(serve_instance_with_signal):
    client, signal = serve_instance_with_signal

    # Step 1: Prepare the script in a zip file so it can be submitted via REST API.
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_path:
        with zipfile.ZipFile(tmp_path, "w") as zip_obj:
            with zip_obj.open("app.py", "w") as f:
                f.write(
                    """
from ray import serve
import ray
import os

@serve.deployment
def g():
    signal = ray.get_actor("signal123")
    ray.get(signal.wait.remote())
    return os.getpid()


app = g.bind()
""".encode()
                )

    # Step 2: Deploy it with max_replicas=1
    app_config = {
        "import_path": "app:app",
        "runtime_env": {"working_dir": f"file://{tmp_path.name}"},
        "deployments": [
            {
                "name": "g",
                "autoscaling_config": {
                    "min_replicas": 0,
                    "max_replicas": 1,
                    "downscale_delay_s": 600,
                    "upscale_delay_s": 0,
                    "metrics_interval_s": 1,
                    "look_back_period_s": 1,
                },
            }
        ],
    }

    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status == "RUNNING"
    )

    # Step 3: Verify that it can scale from 0 to 1.
    @ray.remote
    def send_request():
        return requests.get("http://localhost:8000/").text

    ref = send_request.remote()

    wait_for_condition(
        check_num_replicas_eq, name="g", target=1, retry_interval_ms=1000, timeout=20
    )

    signal.send.remote()
    existing_pid = int(ray.get(ref))

    # Step 4: Change the max replicas to 2
    app_config["deployments"][0]["autoscaling_config"]["max_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status == "RUNNING"
    )
    wait_for_condition(
        check_num_replicas_eq, name="g", target=1, retry_interval_ms=1000, timeout=20
    )

    # Step 5: Make sure it is the same replica (lightweight change).
    for _ in range(10):
        other_pid = int(ray.get(send_request.remote()))
        assert other_pid == existing_pid

    # Step 6: Make sure initial_replicas overrides previous replicas
    app_config["deployments"][0]["autoscaling_config"]["max_replicas"] = 5
    app_config["deployments"][0]["autoscaling_config"]["initial_replicas"] = 3
    app_config["deployments"][0]["autoscaling_config"]["upscale_delay"] = 600
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status == "RUNNING"
    )
    wait_for_condition(
        check_num_replicas_eq, name="g", target=3, retry_interval_ms=1000, timeout=20
    )

    # Step 7: Make sure original replica is still running (lightweight change)
    pids = {int(pid) for pid in ray.get([send_request.remote() for _ in range(20)])}
    assert existing_pid in pids


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.skipif(
    not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    reason="Only works when collecting request metrics at handle.",
)
def test_max_ongoing_requests_set_to_one(serve_instance_with_signal):
    assert RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE
    _, signal = serve_instance_with_signal

    @serve.deployment(
        autoscaling_config=AutoscalingConfig(
            target_ongoing_requests=1.0,
            min_replicas=1,
            max_replicas=3,
            upscale_delay_s=0.5,
            downscale_delay_s=0.5,
            metrics_interval_s=0.5,
            look_back_period_s=2,
        ),
        max_ongoing_requests=1,
        graceful_shutdown_timeout_s=1,
        ray_actor_options={"num_cpus": 0},
    )
    async def f():
        await signal.wait.remote()
        return os.getpid()

    h = serve.run(f.bind())

    check_num_replicas_eq("f", 1)

    # Repeatedly (5 times):
    # 1. Send a new request.
    # 2. Wait for the number of waiters on signal to increase by 1.
    # 3. Assert the number of replicas has increased by 1.
    refs = []
    for i in range(3):
        refs.append(h.remote())

        def check_num_waiters(target: int):
            num_waiters = ray.get(signal.cur_num_waiters.remote())
            assert num_waiters == target
            return True

        wait_for_condition(check_num_waiters, target=i + 1)
        print(time.time(), f"Number of waiters on signal reached {i+1}.")
        check_num_replicas_eq("f", i + 1)
        print(time.time(), f"Confirmed number of replicas are at {i+1}.")

    print(time.time(), "Releasing signal.")
    signal.send.remote()

    # Check that pids returned are unique
    # This implies that each replica only served one request, so the
    # number of "running" requests per replica was at most 1 at any time;
    # meaning the "queued" requests were taken into consideration for
    # autoscaling.
    pids = [ref.result() for ref in refs]
    assert len(pids) == len(set(pids)), f"Pids {pids} are not unique."
    print("Confirmed each replica only served one request.")


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_autoscaling_status_changes(serve_instance):
    """Test status changes when autoscaling deployments are deployed.

    This test runs an autoscaling deployment and an actor called the
    EventManager. During initialization, each replica creates an asyncio.Event
    in the EventManager, and it waits on the event. Once the event is set, the
    replica can finish initializing. The test uses this EventManager to control
    the number of replicas that should be running at a given time.

    The test does the following:

    1.  Starts an EventManager.
    2.  Deploys an autoscaling deployment with min_replicas 3.
    3.  Releases 2 replicas via the EventManager.
    4.  Checks that the deployment remains in the UPDATING status.
    5.  Redeploys the deployment with min_replicas 4.
    6.  Releases 1 more replica via the EventManager.
    7.  Checks that the deployment remains in the UPDATING status.
    8.  Releases 1 more replica.
    9.  Checks that the deployment enters HEALTHY status.
    10. Redeploys the deployment with min_replicas 5.
    11. Checks that the deployment re-enters and remains in the UPDATING status.
    12. Releases 1 more replica.
    13  Checks that the deployment enters HEALTHY status.
    """

    @ray.remote
    class EventManager:
        """Manages events for each deployment replica.

        This actor uses a goal-state architecture. The test sets a max number
        of replicas to run. Whenever this manager creates or removes an event,
        it checks how many replicas are running and attempts to match the goal
        state.
        """

        def __init__(self):
            self._max_replicas_to_run = 0

            # This dictionary maps replica names -> asyncio.Event.
            self._events: Dict[str, asyncio.Event] = dict()

        def get_num_running_replicas(self):
            running_replicas = [
                actor_name
                for actor_name, event in self._events.items()
                if event.is_set()
            ]
            return len(running_replicas)

        def release_replicas(self):
            """Releases replicas until self._max_replicas_to_run are released."""

            num_replicas_released = 0
            for _, event in self._events.items():
                if self.get_num_running_replicas() < self._max_replicas_to_run:
                    if not event.is_set():
                        event.set()
                        num_replicas_released += 1
                else:
                    break

            if num_replicas_released > 0:
                print(
                    f"Started running {num_replicas_released} replicas. "
                    f"{self.get_waiter_statuses()}"
                )

        async def wait(self, actor_name):
            print(f"Replica {actor_name} started waiting...")
            event = asyncio.Event()
            self._events[actor_name] = event
            self.release_replicas()
            await event.wait()
            print(f"Replica {actor_name} finished waiting.")

        async def set_max_replicas_to_run(self, max_num_replicas: int = 1):
            print(f"Setting _max_replicas_to_run to {max_num_replicas}.")
            self._max_replicas_to_run = max_num_replicas
            self.release_replicas()

        async def get_max_replicas_to_run(self) -> int:
            return self._max_replicas_to_run

        async def num_active_replicas(self) -> int:
            """The number of replicas that are waiting or running."""

            return len(self._events)

        def get_waiter_statuses(self) -> Dict[str, bool]:
            return {
                actor_name: event.is_set() for actor_name, event in self._events.items()
            }

        async def clear_dead_replicas(self):
            """Clears dead replicas from internal _events dictionary."""

            actor_names = list(self._events.keys())
            for name in actor_names:
                try:
                    ray.get_actor(name=name, namespace=SERVE_NAMESPACE)
                except ValueError:
                    print(f"Actor {name} has died. Removing event.")
                    self._events.pop(name)

            self.release_replicas()

    print("Starting EventManager actor...")

    event_manager_actor_name = "event_manager_actor"
    event_manager = EventManager.options(
        name=event_manager_actor_name, namespace=SERVE_NAMESPACE
    ).remote()

    print("Starting Serve app...")

    deployment_name = "autoscaling_app"
    min_replicas = 3
    max_replicas = 15

    @serve.deployment(
        name=deployment_name,
        autoscaling_config=AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        ),
        ray_actor_options=dict(num_cpus=0),
        graceful_shutdown_timeout_s=0,
    )
    class AutoscalingDeployment:
        """Deployment that autoscales."""

        async def __init__(self):
            self.name = ray.get_runtime_context().get_actor_name()
            print(f"Replica {self.name} initializing...")
            event_manager = ray.get_actor(
                name=event_manager_actor_name, namespace=SERVE_NAMESPACE
            )
            await event_manager.wait.remote(self.name)
            print(f"Replica {self.name} has initialized.")

    app_name = "autoscaling_app"
    app = AutoscalingDeployment.bind()

    # Start the AutoscalingDeployment.
    serve._run(app, name=app_name, _blocking=False)

    # Active replicas are replicas that are waiting or running.
    expected_num_active_replicas: int = min_replicas

    def check_num_active_replicas(expected: int) -> bool:
        ray.get(event_manager.clear_dead_replicas.remote())
        assert ray.get(event_manager.num_active_replicas.remote()) == expected
        return True

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)
    print("Replicas have started waiting. Releasing some replicas...")

    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas - 1))

    # Wait for replicas to start.
    print("Waiting for replicas to run.")

    def replicas_running(expected_num_running_replicas: int) -> bool:
        ray.get(event_manager.clear_dead_replicas.remote())
        status = serve.status()
        app_status = status.applications[app_name]
        deployment_status = app_status.deployments[deployment_name]
        num_running_replicas = deployment_status.replica_states.get(
            ReplicaState.RUNNING, 0
        )
        assert num_running_replicas == expected_num_running_replicas, (
            f"{app_status}, {ray.available_resources()}, "
            f"{ray.get(event_manager.get_waiter_statuses.remote())}, "
            f"{ray.get(event_manager.get_max_replicas_to_run.remote())}"
        )
        return True

    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=(min_replicas - 1),
        timeout=15,
    )

    def check_expected_statuses(
        expected_app_status: ApplicationStatus,
        expected_deployment_status: DeploymentStatus,
        expected_deployment_status_trigger: DeploymentStatusTrigger,
    ) -> bool:
        status = serve.status()

        app_status = status.applications[app_name]
        assert app_status.status == expected_app_status, f"{app_status}"

        deployment_status = app_status.deployments[deployment_name]
        assert (
            deployment_status.status == expected_deployment_status
        ), f"{deployment_status}"
        assert (
            deployment_status.status_trigger == expected_deployment_status_trigger
        ), f"{deployment_status}"

        return True

    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    # Check that these statuses don't change over time.
    print("Statuses are as expected. Sleeping briefly and checking again...")
    time.sleep(1.5)
    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print("Statuses are still as expected. Redeploying...")

    # Check the status after redeploying the deployment.
    min_replicas += 1
    app = AutoscalingDeployment.options(
        autoscaling_config=AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        )
    ).bind()
    serve._run(app, name=app_name, _blocking=False)
    expected_num_active_replicas = min_replicas

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)
    print("Replicas have started waiting. Releasing some replicas...")

    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas - 1))
    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=(min_replicas - 1),
        timeout=20,
    )

    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print("Statuses are as expected. Sleeping briefly and checking again...")
    time.sleep(1.5)
    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print(
        "Statuses are still as expected. "
        "Releasing some replicas and checking again..."
    )

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)

    # Release enough replicas for deployment to enter autoscaling bounds.
    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas))
    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=min_replicas,
        timeout=20,
    )

    check_expected_statuses(
        ApplicationStatus.RUNNING,
        DeploymentStatus.HEALTHY,
        DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED,
    )

    print("Statuses are as expected. Redeploying with higher min_replicas...")
    min_replicas += 1
    app = AutoscalingDeployment.options(
        autoscaling_config=AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        )
    ).bind()
    serve._run(app, name=app_name, _blocking=False)
    expected_num_active_replicas = min_replicas

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)
    print("Replicas have started waiting. Checking statuses...")

    # DeploymentStatus should return to UPDATING because the
    # autoscaling_config changed.
    wait_for_condition(
        check_expected_statuses,
        expected_app_status=ApplicationStatus.DEPLOYING,
        expected_deployment_status=DeploymentStatus.UPDATING,
        expected_deployment_status_trigger=(
            DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        ),
    )

    print("Statuses are as expected. Sleeping briefly and checking again...")
    time.sleep(1.5)
    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print(
        "Statuses are still as expected. Releasing some replicas and checking again..."
    )

    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas))
    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=min_replicas,
        timeout=20,
    )

    check_expected_statuses(
        ApplicationStatus.RUNNING,
        DeploymentStatus.HEALTHY,
        DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED,
    )

    print("Statuses are as expected.")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
