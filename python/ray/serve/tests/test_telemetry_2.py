import sys
import pytest
import subprocess
import time

import ray
from ray._private.test_utils import wait_for_condition

from ray import serve
from ray.serve.tests.utils import (
    check_ray_started,
    start_telemetry_app,
    check_telemetry_recorded,
    check_telemetry_not_recorded,
)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_status_api_detected(manage_ray, location):
    """Check that serve.status is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    # Check telemetry is not recorded before test starts
    check_telemetry_not_recorded(storage_handle, "serve_status_api_used")

    @serve.deployment
    class Model:
        async def __call__(self):
            return serve.status()

    if location:
        if location == "deployment":
            handle = serve.run(Model.bind(), route_prefix="/model")
            handle.remote()
        elif location == "driver":
            serve.status()

        wait_for_condition(
            check_telemetry_recorded,
            storage_handle=storage_handle,
            key="serve_status_api_used",
            expected_value="1",
        )
    else:
        for _ in range(3):
            check_telemetry_not_recorded(storage_handle, "serve_status_api_used")
            time.sleep(1)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_get_app_handle_api_detected(manage_ray, location):
    """Check that serve.get_app_handle is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    # Check telemetry is not recorded before test starts
    check_telemetry_not_recorded(storage_handle, "serve_get_app_handle_api_used")

    @serve.deployment
    class Model:
        async def __call__(self):
            serve.get_app_handle("telemetry")

    if location:
        if location == "deployment":
            handle = serve.run(Model.bind(), route_prefix="/model")
            handle.remote()
        elif location == "driver":
            serve.get_app_handle("telemetry")

        wait_for_condition(
            check_telemetry_recorded,
            storage_handle=storage_handle,
            key="serve_get_app_handle_api_used",
            expected_value="1",
        )
    else:
        for _ in range(3):
            check_telemetry_not_recorded(
                storage_handle, "serve_get_app_handle_api_used"
            )
            time.sleep(1)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_get_deployment_handle_api_detected(manage_ray, location):
    """Check that serve.get_deployment_handle is detected correctly by telemetry."""

    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(check_ray_started, timeout=5)

    storage_handle = start_telemetry_app()
    wait_for_condition(
        lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
    )
    # Check telemetry is not recorded before test starts
    check_telemetry_not_recorded(storage_handle, "serve_get_deployment_handle_api_used")

    @serve.deployment
    class Model:
        async def __call__(self):
            serve.get_deployment_handle("TelemetryReceiver", "telemetry")

    if location:
        if location == "deployment":
            handle = serve.run(Model.bind(), route_prefix="/model")
            handle.remote()
        elif location == "driver":
            serve.get_deployment_handle("TelemetryReceiver", "telemetry")

        wait_for_condition(
            check_telemetry_recorded,
            storage_handle=storage_handle,
            key="serve_get_deployment_handle_api_used",
            expected_value="1",
        )
    else:
        for _ in range(3):
            check_telemetry_not_recorded(
                storage_handle, "serve_get_deployment_handle_api_used"
            )
            time.sleep(1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
