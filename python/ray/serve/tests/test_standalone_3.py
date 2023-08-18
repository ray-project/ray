import os
import subprocess
import sys
from tempfile import NamedTemporaryFile
from contextlib import contextmanager

import pytest
import requests

import ray
import ray.actor
import ray._private.state

from ray import serve
from ray._private.test_utils import (
    wait_for_condition,
    SignalActor,
)
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.exceptions import RayActorError
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve.context import get_global_client
from ray.serve.schema import ServeInstanceDetails
from ray.serve._private.utils import get_head_node_id
from ray.serve._private.common import HTTPProxyStatus
from ray.tests.conftest import call_ray_stop_only  # noqa: F401


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


@contextmanager
def start_and_shutdown_ray_cli():
    subprocess.check_output(
        ["ray", "start", "--head"],
    )
    yield
    subprocess.check_output(
        ["ray", "stop", "--force"],
    )


@pytest.fixture(scope="function")
def start_and_shutdown_ray_cli_function():
    with start_and_shutdown_ray_cli():
        yield


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_LOWER_BOUND": "1",
            "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_UPPER_BOUND": "2",
        },
    ],
    indirect=True,
)
def test_long_poll_timeout_with_max_concurrent_queries(ray_instance):
    """Test max_concurrent_queries can be honored with long poll timeout

    issue: https://github.com/ray-project/ray/issues/32652
    """

    signal_actor = SignalActor.remote()

    @serve.deployment(max_concurrent_queries=1)
    async def f():
        await signal_actor.wait.remote()
        return "hello"

    handle = serve.run(f.bind())
    first_ref = handle.remote()

    # Clear all the internal longpoll client objects within handle
    # long poll client will receive new updates from long poll host,
    # this is to simulate the longpoll timeout
    object_snapshots1 = handle._router.long_poll_client.object_snapshots
    handle._router.long_poll_client._reset()
    wait_for_condition(
        lambda: len(handle._router.long_poll_client.object_snapshots) > 0, timeout=10
    )
    object_snapshots2 = handle._router.long_poll_client.object_snapshots

    # Check object snapshots between timeout interval
    assert object_snapshots1.keys() == object_snapshots2.keys()
    assert len(object_snapshots1.keys()) == 1
    key = list(object_snapshots1.keys())[0]
    assert (
        object_snapshots1[key][0].actor_handle != object_snapshots2[key][0].actor_handle
    )
    assert (
        object_snapshots1[key][0].actor_handle._actor_id
        == object_snapshots2[key][0].actor_handle._actor_id
    )

    # Make sure the first request is still ongoing.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(first_ref, timeout=1)

    # Unblock the first request.
    signal_actor.send.remote()
    assert ray.get(first_ref) == "hello"
    serve.shutdown()


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1",
            "RAY_SERVE_HTTP_REQUEST_MAX_RETRIES": "5",
        },
    ],
    indirect=True,
)
@pytest.mark.parametrize("crash", [True, False])
@pytest.mark.skipif(
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    reason="No retries w/ new behavior.",
)
def test_http_request_number_of_retries(ray_instance, crash):
    """Test HTTP proxy retry requests."""

    signal_actor = SignalActor.remote()

    @serve.deployment
    class Model:
        async def __call__(self):
            if crash:
                # Trigger Actor Error
                os._exit(0)
            await signal_actor.wait.remote()
            return "hello"

    serve.run(Model.bind())
    assert requests.get("http://127.0.0.1:8000/").status_code == 500

    def verify_metrics():
        resp = requests.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        # Make sure http proxy retry 5 times
        verfied = False
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_num_router_requests" in metrics:
                assert "6.0" in metrics
                verfied = True
        return verfied

    wait_for_condition(verify_metrics, timeout=60, retry_interval_ms=500)
    signal_actor.send.remote()
    serve.shutdown()


@pytest.mark.parametrize(
    "ray_instance",
    [],
    indirect=True,
)
def test_replica_health_metric(ray_instance):
    """Test replica health metrics"""

    @serve.deployment(num_replicas=2)
    def f():
        return "hello"

    serve.run(f.bind())

    def count_live_replica_metrics():
        resp = requests.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        count = 0
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_deployment_replica_healthy" in metrics:
                if "1.0" in metrics:
                    count += 1
        return count

    wait_for_condition(
        lambda: count_live_replica_metrics() == 2, timeout=120, retry_interval_ms=500
    )

    # Add more replicas
    serve.run(f.options(num_replicas=10).bind())
    wait_for_condition(
        lambda: count_live_replica_metrics() == 10, timeout=120, retry_interval_ms=500
    )

    # delete the application
    serve.delete(SERVE_DEFAULT_APP_NAME)
    wait_for_condition(
        lambda: count_live_replica_metrics() == 0, timeout=120, retry_interval_ms=500
    )
    serve.shutdown()


def test_shutdown_remote(start_and_shutdown_ray_cli_function):
    """Check that serve.shutdown() works on a remote Ray cluster."""

    deploy_serve_script = (
        "import ray\n"
        "from ray import serve\n"
        "\n"
        'ray.init(address="auto", namespace="x")\n'
        "serve.start(detached=True)\n"
        "\n"
        "@serve.deployment\n"
        "def f(*args):\n"
        '   return "got f"\n'
        "\n"
        "serve.run(f.bind())\n"
    )

    shutdown_serve_script = (
        "import ray\n"
        "from ray import serve\n"
        "\n"
        'ray.init(address="auto", namespace="x")\n'
        "serve.shutdown()\n"
    )

    # Cannot use context manager due to tmp file's delete flag issue in Windows
    # https://stackoverflow.com/a/15590253
    deploy_file = NamedTemporaryFile(mode="w+", delete=False, suffix=".py")
    shutdown_file = NamedTemporaryFile(mode="w+", delete=False, suffix=".py")

    try:
        deploy_file.write(deploy_serve_script)
        deploy_file.close()

        shutdown_file.write(shutdown_serve_script)
        shutdown_file.close()

        # Ensure Serve can be restarted and shutdown with for loop
        for _ in range(2):
            subprocess.check_output(["python", deploy_file.name])
            assert requests.get("http://localhost:8000/f").text == "got f"
            subprocess.check_output(["python", shutdown_file.name])
            with pytest.raises(requests.exceptions.ConnectionError):
                requests.get("http://localhost:8000/f")
    finally:
        os.unlink(deploy_file.name)
        os.unlink(shutdown_file.name)


def test_handle_early_detect_failure(shutdown_ray):
    """Check that handle can be notified about replicas failure.

    It should detect replica raises ActorError and take them out of the replicas set.
    """
    ray.init()
    serve.start(detached=True)

    @serve.deployment(num_replicas=2, max_concurrent_queries=1)
    def f(do_crash: bool = False):
        if do_crash:
            os._exit(1)
        return os.getpid()

    handle = serve.run(f.bind())
    pids = ray.get([handle.remote() for _ in range(2)])
    assert len(set(pids)) == 2

    client = get_global_client()
    # Kill the controller so that the replicas membership won't be updated
    # through controller health check + long polling.
    ray.kill(client._controller, no_restart=True)

    with pytest.raises(RayActorError):
        ray.get(handle.remote(do_crash=True))

    pids = ray.get([handle.remote() for _ in range(10)])
    assert len(set(pids)) == 1

    # Restart the controller, and then clean up all the replicas
    serve.start(detached=True)
    serve.shutdown()


def test_autoscaler_shutdown_node_http_everynode(
    monkeypatch, shutdown_ray, call_ray_stop_only  # noqa: F811
):
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "1")
    cluster = AutoscalingCluster(
        head_resources={"CPU": 4},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 4,
                    "IS_WORKER": 100,
                },
                "node_config": {},
                "max_workers": 1,
            },
        },
        idle_timeout_minutes=0.05,
    )
    cluster.start()
    ray.init(address="auto")

    serve.start(http_options={"location": "EveryNode"})

    @serve.deployment(
        num_replicas=2,
        ray_actor_options={"num_cpus": 3},
    )
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind(), name="app_f")

    # 2 proxies, 1 controller, 2 replicas.
    wait_for_condition(lambda: len(ray._private.state.actors()) == 5)
    assert len(ray.nodes()) == 2

    # Stop all deployment replicas.
    serve.delete("app_f")

    # The http proxy on worker node should exit as well.
    wait_for_condition(
        lambda: len(
            list(
                filter(
                    lambda a: a["State"] == "ALIVE",
                    ray._private.state.actors().values(),
                )
            )
        )
        == 2
    )
    client = get_global_client()
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    assert len(serve_details.http_proxies) == 1
    assert (
        serve_details.http_proxies[get_head_node_id()].status == HTTPProxyStatus.HEALTHY
    )

    # Only head node should exist now.
    wait_for_condition(
        lambda: len(list(filter(lambda n: n["Alive"], ray.nodes()))) == 1
    )

    # Clean up serve.
    serve.shutdown()


def test_drain_and_undrain_http_proxy_actors(
    monkeypatch, shutdown_ray, call_ray_stop_only  # noqa: F811
):
    """Test the state transtion of the proxy actor between
    HEALTHY, DRAINING and DRAINED
    """
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "10")

    cluster = Cluster()
    head_node = cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(address=head_node.address)
    serve.start(http_options={"location": "EveryNode"})

    @serve.deployment
    class HelloModel:
        def __call__(self):
            return "hello"

    serve.run(HelloModel.options(num_replicas=2).bind())

    # 3 proxies, 1 controller, 2 replicas.
    wait_for_condition(lambda: len(ray._private.state.actors()) == 6)
    assert len(ray.nodes()) == 3

    client = get_global_client()
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    proxy_actor_ids = {
        proxy.actor_id for _, proxy in serve_details.http_proxies.items()
    }
    assert len(proxy_actor_ids) == 3

    serve.run(HelloModel.options(num_replicas=1).bind())
    # 1 proxy should be draining

    def check_proxy_status(proxy_status_to_count):
        serve_details = ServeInstanceDetails(
            **ray.get(client._controller.get_serve_instance_details.remote())
        )
        proxy_status_list = [
            proxy.status for _, proxy in serve_details.http_proxies.items()
        ]
        return {
            status: proxy_status_list.count(status) for status in proxy_status_list
        } == proxy_status_to_count

    wait_for_condition(
        condition_predictor=check_proxy_status,
        proxy_status_to_count={HTTPProxyStatus.HEALTHY: 2, HTTPProxyStatus.DRAINING: 1},
    )

    serve.run(HelloModel.options(num_replicas=2).bind())
    # The draining proxy should become healthy.
    wait_for_condition(
        condition_predictor=check_proxy_status,
        proxy_status_to_count={HTTPProxyStatus.HEALTHY: 3},
    )
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    {
        proxy.actor_id for _, proxy in serve_details.http_proxies.items()
    } == proxy_actor_ids

    serve.run(HelloModel.options(num_replicas=1).bind())
    # 1 proxy should be draining and eventually be drained.
    wait_for_condition(
        condition_predictor=check_proxy_status,
        timeout=40,
        proxy_status_to_count={HTTPProxyStatus.HEALTHY: 2},
    )

    # Clean up serve.
    serve.shutdown()


def test_healthz_and_routes_on_head_and_worker_nodes(
    shutdown_ray, call_ray_stop_only  # noqa: F811
):
    """Test `/-/healthz` and `/-/routes` return the correct responses for head and
    worker nodes.

    When there are replicas on all nodes, `/-/routes` and `/-/routes` on all nodes
    should return 200. When there are no replicas on any nodes, `/-/routes` and
    `/-/routes` on the head node should continue to return 200. `/-/routes` and
    `/-/routes` on the worker node should start to return 503
    """
    # Setup worker http proxy to be pointing to port 8001. Head node http proxy will
    # continue to be pointing to the default port 8000.
    os.environ["TEST_WORKER_NODE_PORT"] = "8001"

    # Setup a cluster with 2 nodes
    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
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
        _actors = ray._private.state.actors().values()
        replica_nodes = [
            a["Address"]["NodeID"]
            for a in _actors
            if a["ActorClassName"].startswith("ServeReplica")
        ]
        return len(set(replica_nodes)) == 1

    wait_for_condition(check_replicas_on_worker_nodes)

    # Ensure total actors of 2 proxies, 1 controller, and 2 replicas, and 2 nodes exist.
    wait_for_condition(lambda: len(ray._private.state.actors()) == 5)
    assert len(ray.nodes()) == 2

    # Ensure `/-/healthz` and `/-/routes` return 200 and expected responses
    # on both nodes.
    def check_request(url: str, expected_code: int, expected_text: str):
        req = requests.get(url)
        return req.status_code == expected_code and req.text == expected_text

    wait_for_condition(
        condition_predictor=check_request,
        url="http://127.0.0.1:8000/-/healthz",
        expected_code=200,
        expected_text="success",
    )
    assert requests.get("http://127.0.0.1:8000/-/routes").status_code == 200
    assert requests.get("http://127.0.0.1:8000/-/routes").text == '{"/":"default"}'
    wait_for_condition(
        condition_predictor=check_request,
        url="http://127.0.0.1:8001/-/healthz",
        expected_code=200,
        expected_text="success",
    )
    assert requests.get("http://127.0.0.1:8001/-/routes").status_code == 200
    assert requests.get("http://127.0.0.1:8001/-/routes").text == '{"/":"default"}'

    # Delete the deployment should bring the active actors down to 3 and drop
    # replicas on all nodes.
    serve.delete(name=SERVE_DEFAULT_APP_NAME)

    def _check():
        _actors = ray._private.state.actors().values()
        return (
            len(
                list(
                    filter(
                        lambda a: a["State"] == "ALIVE",
                        _actors,
                    )
                )
            )
            == 3
        )

    wait_for_condition(_check)

    # Ensure head node `/-/healthz` and `/-/routes` continue to return 200 and expected
    # responses. Also, the worker node `/-/healthz` and `/-/routes` should return 503
    # and unavailable responses.
    wait_for_condition(
        condition_predictor=check_request,
        url="http://127.0.0.1:8000/-/healthz",
        expected_code=200,
        expected_text="success",
    )
    assert requests.get("http://127.0.0.1:8000/-/routes").status_code == 200
    assert requests.get("http://127.0.0.1:8000/-/routes").text == "{}"
    wait_for_condition(
        condition_predictor=check_request,
        url="http://127.0.0.1:8001/-/healthz",
        expected_code=503,
        expected_text="This node is being drained.",
    )
    assert requests.get("http://127.0.0.1:8001/-/routes").status_code == 503
    assert (
        requests.get("http://127.0.0.1:8001/-/routes").text
        == "This node is being drained."
    )

    # Clean up serve.
    serve.shutdown()


@pytest.mark.parametrize("wait_for_controller_shutdown", (True, False))
def test_controller_shutdown_gracefully(
    shutdown_ray, call_ray_stop_only, wait_for_controller_shutdown  # noqa: F811
):
    """Test controller shutdown gracefully when calling `graceful_shutdown()`.

    Called `graceful_shutdown()` on the controller, so it will start shutdown and
    eventually all actors will be in DEAD state. Test both cases whether to wait for
    the controller shutdown or not should both resolve graceful shutdown.
    """
    # Setup a cluster with 2 nodes
    cluster = Cluster()
    cluster.add_node()
    cluster.add_node()
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Deploy 2 replicas
    @serve.deployment(num_replicas=2)
    class HelloModel:
        def __call__(self):
            return "hello"

    model = HelloModel.bind()
    serve.run(target=model)

    # Ensure total actors of 2 proxies, 1 controller, and 2 replicas
    wait_for_condition(lambda: len(ray._private.state.actors()) == 5)
    assert len(ray.nodes()) == 2

    # Call `graceful_shutdown()` on the controller, so it will start shutdown.
    client = get_global_client()
    if wait_for_controller_shutdown:
        # Waiting for controller shutdown will throw RayActorError when the controller
        # killed itself.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(client._controller.graceful_shutdown.remote(True))
    else:
        ray.get(client._controller.graceful_shutdown.remote(False))

    # Ensure the all resources are shutdown.
    wait_for_condition(
        lambda: all(
            [actor["State"] == "DEAD" for actor in ray._private.state.actors().values()]
        )
    )

    # Clean up serve.
    serve.shutdown()


def test_client_shutdown_gracefully_when_timeout(
    shutdown_ray, call_ray_stop_only, caplog  # noqa: F811
):
    """Test client shutdown gracefully when timeout.

    When the controller is taking longer than the timeout to shutdown, the client will
    log timeout message and exit the process. The controller will continue to shutdown
    everything gracefully.
    """
    # Setup a cluster with 2 nodes
    cluster = Cluster()
    cluster.add_node()
    cluster.add_node()
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Deploy 2 replicas
    @serve.deployment(num_replicas=2)
    class HelloModel:
        def __call__(self):
            return "hello"

    model = HelloModel.bind()
    serve.run(target=model)

    # Ensure total actors of 2 proxies, 1 controller, and 2 replicas
    wait_for_condition(lambda: len(ray._private.state.actors()) == 5)
    assert len(ray.nodes()) == 2

    # Ensure client times out if the controller does not shutdown within timeout.
    timeout_s = 0.0
    client = get_global_client()
    client.shutdown(timeout_s=timeout_s)
    assert (
        f"Controller failed to shut down within {timeout_s}s. "
        f"Check controller logs for more details." in caplog.text
    )

    # Ensure the all resources are shutdown gracefully.
    wait_for_condition(
        lambda: all(
            [actor["State"] == "DEAD" for actor in ray._private.state.actors().values()]
        ),
    )

    # Clean up serve.
    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
