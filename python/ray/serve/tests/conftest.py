import os

import pytest
import tempfile
import subprocess
import random

import requests
import ray
from ray import serve

from ray._private.test_utils import wait_for_condition
from ray.tests.conftest import pytest_runtest_makereport  # noqa

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
    yield serve.start(detached=True, http_options={"host": "0.0.0.0"})


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    # Clear all state between tests to avoid naming collisions.
    _shared_serve_instance.delete_deployments(serve.list_deployments().keys())
    # Clear the ServeHandle cache between tests to avoid them piling up.
    _shared_serve_instance.handle_cache.clear()


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
