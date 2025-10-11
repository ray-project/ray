import logging
import os
import subprocess
import sys
from contextlib import contextmanager

import httpx
import pytest

import ray
import ray.actor
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.exceptions import RayActorError
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_LOGGER_NAME
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve._private.utils import get_head_node_id
from ray.serve.context import _get_global_client
from ray.serve.schema import ProxyStatus, ServeInstanceDetails
from ray.tests.conftest import call_ray_stop_only  # noqa: F401
from ray.util.state import list_actors


# Some tests are not possible to run if proxy is not available on every node.
# We skip them if proxy is not available.
def is_proxy_on_every_node() -> bool:
    client = _get_global_client()
    return client._http_config.location == "EveryNode"


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
            "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_LOWER_BOUND": "0.1",
            "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_UPPER_BOUND": "0.2",
        },
    ],
    indirect=True,
)
def test_long_poll_timeout_with_max_ongoing_requests(ray_instance):
    """Test that max_ongoing_requests is respected when there are long poll timeouts.

    Previously, when a long poll update occurred (e.g., a timeout or new replicas
    added), ongoing requests would no longer be counted against
    `max_ongoing_requests`.

    Issue: https://github.com/ray-project/ray/issues/32652
    """

    @ray.remote(num_cpus=0)
    class CounterActor:
        def __init__(self):
            self._count = 0

        def inc(self):
            self._count += 1

        def get(self):
            return self._count

    signal_actor = SignalActor.remote()
    counter_actor = CounterActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    async def f():
        await counter_actor.inc.remote()
        await signal_actor.wait.remote()
        return "hello"

    # Issue a blocking request which should occupy the only slot due to
    # `max_ongoing_requests=1`.
    serve.run(f.bind())

    @ray.remote
    def do_req():
        return httpx.get("http://localhost:8000").text

    # The request should be hanging waiting on the `SignalActor`.
    first_ref = do_req.remote()

    def check_request_started(num_expected_requests: int) -> bool:
        with pytest.raises(TimeoutError):
            ray.get(first_ref, timeout=0.1)
        assert ray.get(counter_actor.get.remote()) == num_expected_requests
        return True

    wait_for_condition(check_request_started, timeout=5, num_expected_requests=1)

    # Now issue 10 more requests and wait for significantly longer than the long poll
    # timeout. They should all be queued in the handle due to `max_ongoing_requests`
    # enforcement (verified via the counter).
    new_refs = [do_req.remote() for _ in range(10)]
    ready, _ = ray.wait(new_refs, timeout=1)
    assert len(ready) == 0
    assert ray.get(counter_actor.get.remote()) == 1

    # Unblock the first request. Now everything should get executed.
    ray.get(signal_actor.send.remote())
    assert ray.get(first_ref) == "hello"
    assert ray.get(new_refs) == ["hello"] * 10
    assert ray.get(counter_actor.get.remote()) == 11

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
        resp = httpx.get("http://127.0.0.1:9999").text
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


def test_shutdown_remote(start_and_shutdown_ray_cli_function, tmp_path):
    """Check that serve.shutdown() works on a remote Ray cluster."""

    deploy_serve_script = (
        "import ray\n"
        "from ray import serve\n"
        "\n"
        'ray.init(address="auto", namespace="x")\n'
        "serve.start()\n"
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

    deploy_file = tmp_path / "deploy.py"
    shutdown_file = tmp_path / "shutdown.py"

    deploy_file.write_text(deploy_serve_script)

    shutdown_file.write_text(shutdown_serve_script)

    # Ensure Serve can be restarted and shutdown with for loop
    for _ in range(2):
        subprocess.check_output([sys.executable, str(deploy_file)])
        assert httpx.get("http://localhost:8000/f").text == "got f"
        subprocess.check_output([sys.executable, str(shutdown_file)])
        with pytest.raises(httpx.ConnectError):
            httpx.get("http://localhost:8000/f")


def test_handle_early_detect_failure(shutdown_ray):
    """Check that handle can be notified about replicas failure.

    It should detect replica raises ActorError and take them out of the replicas set.
    """

    try:

        @serve.deployment(num_replicas=2, max_ongoing_requests=1)
        def f(do_crash: bool = False):
            if do_crash:
                os._exit(1)
            return os.getpid()

        handle = serve.run(f.bind())
        responses = [handle.remote() for _ in range(10)]
        assert len({r.result() for r in responses}) == 2

        client = _get_global_client()
        # Kill the controller so that the replicas membership won't be updated
        # through controller health check + long polling.
        ray.kill(client._controller, no_restart=True)

        with pytest.raises(RayActorError):
            handle.remote(do_crash=True).result()

        responses = [handle.remote() for _ in range(10)]
        assert len({r.result() for r in responses}) == 1
    finally:
        # Restart the controller, and then clean up all the replicas.
        serve.shutdown()


@pytest.mark.parametrize(
    "autoscaler_v2",
    [False, True],
    ids=["v1", "v2"],
)
def test_autoscaler_shutdown_node_http_everynode(
    autoscaler_v2, monkeypatch, shutdown_ray, call_ray_stop_only  # noqa: F811
):
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "1")
    # Faster health check interval to speed up the test.
    monkeypatch.setenv("RAY_health_check_failure_threshold", "1")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "2000")
    monkeypatch.setenv("RAY_health_check_period_ms", "3000")

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
        autoscaler_v2=autoscaler_v2,
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

    # If proxy is on every node, total actors are 2 proxies, 1 controller, 2 replicas.
    # Otherwise, total actors are 1 proxy, 1 controller, 2 replicas.
    expected_actors = 5 if is_proxy_on_every_node() else 4
    wait_for_condition(lambda: len(list_actors()) == expected_actors)
    assert len(ray.nodes()) == 2

    # Stop all deployment replicas.
    serve.delete("app_f")

    # The http proxy on worker node should exit as well.
    wait_for_condition(
        lambda: len(list_actors(filters=[("STATE", "=", "ALIVE")])) == 2,
    )

    client = _get_global_client()

    def serve_details_proxy_count():
        serve_details = ServeInstanceDetails(
            **ray.get(client._controller.get_serve_instance_details.remote())
        )
        return len(serve_details.proxies)

    wait_for_condition(lambda: serve_details_proxy_count() == 1)

    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    assert serve_details.proxies[get_head_node_id()].status == ProxyStatus.HEALTHY

    # Only head node should exist now.
    wait_for_condition(
        lambda: len(list(filter(lambda n: n["Alive"], ray.nodes()))) == 1
    )

    # Clean up serve.
    serve.shutdown()
    cluster.shutdown()
    ray.shutdown()


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
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # On Windows, wait for resources to be available before adding second node
    # to avoid timeout errors when cluster has zero CPU resources
    if sys.platform == "win32":
        wait_for_condition(
            lambda: ray.cluster_resources().get("CPU", 0) > 0,
            timeout=30,
            retry_interval_ms=1000,
        )

    cluster.add_node()
    cluster.wait_for_nodes()

    # Deploy 2 replicas
    @serve.deployment(num_replicas=2)
    class HelloModel:
        def __call__(self):
            return "hello"

    model = HelloModel.bind()
    serve.run(target=model)

    # If proxy is on every node, total actors are 2 proxies, 1 controller, and 2 replicas
    # Otherwise, total actors are 1 proxy, 1 controller, and 2 replicas
    expected_actors = 5 if is_proxy_on_every_node() else 4
    wait_for_condition(lambda: len(list_actors()) == expected_actors)
    assert len(ray.nodes()) == 2

    # Call `graceful_shutdown()` on the controller, so it will start shutdown.
    client = _get_global_client()
    if wait_for_controller_shutdown:
        # Waiting for controller shutdown will throw RayActorError when the controller
        # killed itself.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(client._controller.graceful_shutdown.remote(True))
    else:
        ray.get(client._controller.graceful_shutdown.remote(False))

    # Ensure the all resources are shutdown.
    wait_for_condition(
        lambda: len(list_actors(filters=[("STATE", "=", "ALIVE")])) == 0,
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
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    caplog.set_level(logging.WARNING, logger=SERVE_LOGGER_NAME)

    warning_msg = []

    class WarningHandler(logging.Handler):
        def emit(self, record):
            warning_msg.append(self.format(record))

    logger.addHandler(WarningHandler())

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

    # Check expected actors based on mode
    # If proxy is on every node, total actors are 2 proxies, 1 controller, and 2 replicas
    # Otherwise, total actors are 1 proxy, 1 controller, and 2 replicas
    expected_actors = 5 if is_proxy_on_every_node() else 4
    wait_for_condition(lambda: len(list_actors()) == expected_actors)
    assert len(ray.nodes()) == 2

    # Ensure client times out if the controller does not shutdown within timeout.
    timeout_s = 0.0
    client = _get_global_client()
    client.shutdown(timeout_s=timeout_s)
    assert (
        f"Controller failed to shut down within {timeout_s}s. "
        f"Check controller logs for more details." in warning_msg
    )

    # Ensure the all resources are shutdown gracefully.
    wait_for_condition(
        lambda: len(list_actors(filters=[("STATE", "=", "ALIVE")])) == 0,
    )

    # Clean up serve.
    serve.shutdown()


def test_serve_shut_down_without_duplicated_logs(
    shutdown_ray, call_ray_stop_only  # noqa: F811
):
    """Test Serve shut down without duplicated logs.

    When Serve shutdown is called and executing the shutdown process, the controller
    log should not be spamming controller shutdown and deleting app messages.
    """
    cluster = Cluster()
    cluster.add_node()
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @serve.deployment
    class HelloModel:
        def __call__(self):
            return "hello"

    model = HelloModel.bind()
    serve.run(target=model)
    serve.shutdown()

    # Ensure the all resources are shutdown gracefully.
    wait_for_condition(
        lambda: len(list_actors(filters=[("STATE", "=", "ALIVE")])) == 0,
    )

    all_serve_logs = ""
    for filename in os.listdir(get_serve_logs_dir()):
        file_path = os.path.join(get_serve_logs_dir(), filename)
        if os.path.isfile(file_path):
            with open(file_path, "r") as f:
                all_serve_logs += f.read()
    assert all_serve_logs.count("Controller shutdown started") == 1
    assert all_serve_logs.count("Deleting app 'default'") == 1


def test_job_runtime_env_not_leaked(shutdown_ray):  # noqa: F811
    """https://github.com/ray-project/ray/issues/49074"""

    @serve.deployment
    class D:
        async def __call__(self) -> str:
            return os.environ["KEY"]

    app = D.bind()

    # Initialize Ray with a runtime_env, should get picked up by the app.
    ray.init(runtime_env={"env_vars": {"KEY": "VAL1"}})
    h = serve.run(app)
    assert h.remote().result() == "VAL1"
    serve.shutdown()
    ray.shutdown()

    # Re-initialize Ray with a different runtime_env, check that the updated one
    # is picked up by the app.
    ray.init(runtime_env={"env_vars": {"KEY": "VAL2"}})
    h = serve.run(app)
    assert h.remote().result() == "VAL2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
