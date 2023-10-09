import os
import random
import subprocess
import tempfile

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray._private.usage import usage_lib
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.serve.context import _get_global_client
from ray.serve.tests.common.utils import TELEMETRY_ROUTE_PREFIX, check_ray_stopped
from ray.tests.conftest import propagate_logs, pytest_runtest_makereport  # noqa

# https://tools.ietf.org/html/rfc6335#section-6
MIN_DYNAMIC_PORT = 49152
MAX_DYNAMIC_PORT = 65535

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False) == 1:
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture
def ray_shutdown():
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield Cluster()
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_autoscaling_cluster(request):
    cluster = AutoscalingCluster(**request.param)
    cluster.start()
    yield
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start(scope="module"):
    port = random.randint(MIN_DYNAMIC_PORT, MAX_DYNAMIC_PORT)
    subprocess.check_output(
        [
            "ray",
            "start",
            "--head",
            "--num-cpus",
            "16",
            "--ray-client-server-port",
            f"{port}",
        ]
    )
    try:
        yield f"localhost:{port}"
    finally:
        subprocess.check_output(["ray", "stop", "--force"])


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield tmp_dir
        os.chdir(old_dir)


@pytest.fixture(scope="session")
def _shared_serve_instance():
    # Note(simon):
    # This line should be not turned on on master because it leads to very
    # spammy and not useful log in case of a failure in CI.
    # To run locally, please use this instead.
    # SERVE_DEBUG_LOG=1 pytest -v -s test_api.py
    # os.environ["SERVE_DEBUG_LOG"] = "1" <- Do not uncomment this.

    # Overriding task_retry_delay_ms to relaunch actors more quickly
    ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    serve.start(http_options={"host": "0.0.0.0"})
    yield _get_global_client()


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    # Clear all state for 2.x applications and deployments.
    _shared_serve_instance.delete_all_apps()
    # Clear all state for 1.x deployments.
    _shared_serve_instance.delete_deployments(serve.list_deployments().keys())
    # Clear the ServeHandle cache between tests to avoid them piling up.
    _shared_serve_instance.shutdown_cached_handles()


def check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


@pytest.fixture(scope="function")
def ray_start_stop():
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )
    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(
        lambda: requests.get("http://localhost:52365/api/ray/version").status_code
        == 200,
        timeout=15,
    )
    yield
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )


@pytest.fixture
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


@pytest.fixture
def manage_ray_with_telemetry(monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv(
            "RAY_USAGE_STATS_REPORT_URL",
            f"http://127.0.0.1:8000{TELEMETRY_ROUTE_PREFIX}",
        )
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(check_ray_stopped, timeout=5)
        yield

        # Call Python API shutdown() methods to clear global variable state
        serve.shutdown()
        ray.shutdown()

        # Reset global state (any keys that may have been set and cached while the
        # workload was running).
        usage_lib.reset_global_state()

        # Shut down Ray cluster with CLI
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(check_ray_stopped, timeout=5)
