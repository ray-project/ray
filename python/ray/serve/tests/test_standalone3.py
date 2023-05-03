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
    run_string_as_driver,
    wait_for_condition,
    SignalActor,
)
from ray._private.ray_constants import gcs_actor_scheduling_enabled
from ray.cluster_utils import AutoscalingCluster
from ray.exceptions import RayActorError
from ray.serve._private.constants import (
    SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve.context import get_global_client
from ray.tests.conftest import call_ray_stop_only  # noqa: F401


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture()
def ray_instance(request):
    """Starts and stops a Ray instance for this test.

    Args:
        request: request.param should contain a dictionary of env vars and
            their values. The Ray instance will be started with these env vars.
    """

    original_env_vars = os.environ.copy()

    try:
        requested_env_vars = request.param
    except AttributeError:
        requested_env_vars = {}

    os.environ.update(requested_env_vars)

    yield ray.init(
        _metrics_export_port=9999,
        _system_config={
            "metrics_report_interval_ms": 1000,
            "task_retry_delay_ms": 50,
        },
    )

    ray.shutdown()

    os.environ.clear()
    os.environ.update(original_env_vars)


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
    """Test max_concurrent_queries can be honorded with long poll timeout

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
    object_snapshots1 = handle.router.long_poll_client.object_snapshots
    handle.router.long_poll_client._reset()
    wait_for_condition(
        lambda: len(handle.router.long_poll_client.object_snapshots) > 0, timeout=10
    )
    object_snapshots2 = handle.router.long_poll_client.object_snapshots

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

    # Make sure the inflight queries still one
    assert len(handle.router._replica_set.in_flight_queries) == 1
    key = list(handle.router._replica_set.in_flight_queries.keys())[0]
    assert len(handle.router._replica_set.in_flight_queries[key]) == 1

    # Make sure the first request is being run.
    replicas = list(handle.router._replica_set.in_flight_queries.keys())
    assert len(handle.router._replica_set.in_flight_queries[replicas[0]]) == 1
    # First ref should be still ongoing
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
    assert len(handle.router._replica_set.in_flight_queries.keys()) == 2

    client = get_global_client()
    # Kill the controller so that the replicas membership won't be updated
    # through controller health check + long polling.
    ray.kill(client._controller, no_restart=True)

    with pytest.raises(RayActorError):
        ray.get(handle.remote(do_crash=True))

    pids = ray.get([handle.remote() for _ in range(10)])
    assert len(set(pids)) == 1
    assert len(handle.router._replica_set.in_flight_queries.keys()) == 1

    # Restart the controller, and then clean up all the replicas
    serve.start(detached=True)
    serve.shutdown()


@pytest.mark.skipif(
    gcs_actor_scheduling_enabled(),
    reason="Raylet-based scheduler favors (http proxy) actors' owner "
    + "nodes (the head one), so the `EveryNode` option is actually not "
    + "enforced. Besides, the second http proxy does not die with the "
    + "placeholder (happens to both schedulers), so gcs-based scheduler (which "
    + "may collocate the second http proxy and the place holder) "
    + "can not shutdown the worker node.",
)
def test_autoscaler_shutdown_node_http_everynode(
    shutdown_ray, call_ray_stop_only  # noqa: F811
):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2},
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

    @ray.remote
    class Placeholder:
        def ready(self):
            return 1

    a = Placeholder.options(resources={"IS_WORKER": 1}).remote()
    assert ray.get(a.ready.remote()) == 1

    # 2 proxies, 1 controller, and one placeholder.
    wait_for_condition(lambda: len(ray._private.state.actors()) == 4)
    assert len(ray.nodes()) == 2

    # Now make sure the placeholder actor exits.
    ray.kill(a)
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
    # Only head node should exist now.
    wait_for_condition(
        lambda: len(list(filter(lambda n: n["Alive"], ray.nodes()))) == 1
    )


def test_legacy_sync_handle_env_var(call_ray_stop_only):  # noqa: F811
    script = """
from ray import serve
from ray.serve.dag import InputNode
from ray.serve.drivers import DAGDriver
import ray

@serve.deployment
class A:
    def predict(self, inp):
        return inp

@serve.deployment
class Dispatch:
    def __init__(self, handle):
        self.handle = handle

    def predict(self, inp):
        ref = self.handle.predict.remote(inp)
        assert isinstance(ref, ray.ObjectRef), ref
        return ray.get(ref)

with InputNode() as inp:
    a = A.bind()
    d = Dispatch.bind(a)
    dag = d.predict.bind(inp)

handle = serve.run(DAGDriver.bind(dag))
assert ray.get(handle.predict.remote(1)) == 1
    """

    run_string_as_driver(
        script, dict(os.environ, **{SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY: "1"})
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
