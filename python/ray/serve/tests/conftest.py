import os
import random
import subprocess
import tempfile
from contextlib import contextmanager
from copy import deepcopy

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._common.utils import reset_ray_address
from ray._private.usage import usage_lib
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.serve._private.test_utils import (
    TELEMETRY_ROUTE_PREFIX,
    check_ray_started,
    check_ray_stopped,
    start_telemetry_app,
)
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.context import _get_global_client
from ray.tests.conftest import propagate_logs, pytest_runtest_makereport  # noqa

# https://tools.ietf.org/html/rfc6335#section-6
MIN_DYNAMIC_PORT = 49152
MAX_DYNAMIC_PORT = 65535
TEST_METRICS_EXPORT_PORT = 9999

TEST_GRPC_SERVICER_FUNCTIONS = [
    "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
    "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
]

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False) == 1:
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture
def ray_shutdown():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield cluster
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_autoscaling_cluster(request):
    # NOTE(zcin): We have to make a deepcopy here because AutoscalingCluster
    # modifies the dictionary that's passed in.
    params = deepcopy(request.param)
    cluster = AutoscalingCluster(**params)
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


def _check_ray_stop():
    try:
        httpx.get("http://localhost:8265/api/ray/version")
        return False
    except Exception:
        return True


@contextmanager
def start_and_shutdown_ray_cli():
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(_check_ray_stop, timeout=15)
    subprocess.check_output(["ray", "start", "--head"])

    yield

    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(_check_ray_stop, timeout=15)


@pytest.fixture(scope="module")
def start_and_shutdown_ray_cli_module():
    with start_and_shutdown_ray_cli():
        yield


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
    serve.start(
        http_options={"host": "0.0.0.0"},
        grpc_options={
            "port": 9000,
            "grpc_servicer_functions": TEST_GRPC_SERVICER_FUNCTIONS,
        },
    )
    yield _get_global_client()


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    # Clear all state for 2.x applications and deployments.
    _shared_serve_instance.delete_all_apps()
    # Clear the ServeHandle cache between tests to avoid them piling up.
    _shared_serve_instance.shutdown_cached_handles()


@pytest.fixture
def serve_instance_with_signal(serve_instance):
    client = serve_instance

    signal = SignalActor.options(name="signal123").remote()
    yield client, signal

    # Delete signal actor so there is no conflict between tests
    ray.kill(signal)


def check_ray_stop():
    try:
        httpx.get("http://localhost:8265/api/ray/version")
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
        lambda: httpx.get("http://localhost:8265/api/ray/version").status_code == 200,
        timeout=15,
    )
    yield
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )


@pytest.fixture(scope="function")
def ray_start_stop_in_specific_directory(request):
    original_working_dir = os.getcwd()

    # Change working directory so Ray will start in the requested directory.
    new_working_dir = request.param
    os.chdir(new_working_dir)
    print(f"\nChanged working directory to {new_working_dir}\n")

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(
        lambda: httpx.get("http://localhost:8265/api/ray/version").status_code == 200,
        timeout=15,
    )
    try:
        yield
    finally:
        # Change the directory back to the original one.
        os.chdir(original_working_dir)
        print(f"\nChanged working directory back to {original_working_dir}\n")

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

        subprocess.check_output(["ray", "start", "--head"])
        wait_for_condition(check_ray_started, timeout=5)

        storage = start_telemetry_app()
        wait_for_condition(
            lambda: ray.get(storage.get_reports_received.remote()) > 1, timeout=15
        )

        yield storage

        # Call Python API shutdown() methods to clear global variable state
        serve.shutdown()
        ray.shutdown()

        # Reset global state (any keys that may have been set and cached while the
        # workload was running).
        usage_lib.reset_global_state()

        # Shut down Ray cluster with CLI
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(check_ray_stopped, timeout=5)


@pytest.fixture
def metrics_start_shutdown(request):
    param = request.param if hasattr(request, "param") else None
    request_timeout_s = param if param else None
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    ray.init(
        _metrics_export_port=TEST_METRICS_EXPORT_PORT,
        _system_config={
            "metrics_report_interval_ms": 100,
            "task_retry_delay_ms": 50,
        },
    )
    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]
    yield serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
            request_timeout_s=request_timeout_s,
        ),
        http_options=HTTPOptions(
            host="0.0.0.0",
            request_timeout_s=request_timeout_s,
        ),
    )
    serve.shutdown()
    ray.shutdown()
    reset_ray_address()
