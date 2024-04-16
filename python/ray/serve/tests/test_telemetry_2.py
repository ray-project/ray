import sys
import time

import pytest

from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.test_utils import check_telemetry
from ray.serve._private.usage import ServeUsageTag


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_status_api_detected(manage_ray_with_telemetry, location):
    """Check that serve.status is detected correctly by telemetry."""

    # Check telemetry is not recorded before test starts
    check_telemetry(ServeUsageTag.SERVE_STATUS_API_USED, expected=None)

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
            check_telemetry, tag=ServeUsageTag.SERVE_STATUS_API_USED, expected="1"
        )
    else:
        for _ in range(3):
            check_telemetry(ServeUsageTag.SERVE_STATUS_API_USED, expected=None)
            time.sleep(1)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_get_app_handle_api_detected(manage_ray_with_telemetry, location):
    """Check that serve.get_app_handle is detected correctly by telemetry."""

    # Check telemetry is not recorded before test starts
    check_telemetry(ServeUsageTag.SERVE_GET_APP_HANDLE_API_USED, expected=None)

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
            check_telemetry,
            tag=ServeUsageTag.SERVE_GET_APP_HANDLE_API_USED,
            expected="1",
        )
    else:
        for _ in range(3):
            check_telemetry(ServeUsageTag.SERVE_GET_APP_HANDLE_API_USED, expected=None)
            time.sleep(1)


@pytest.mark.parametrize("location", ["driver", "deployment", None])
def test_get_deployment_handle_api_detected(manage_ray_with_telemetry, location):
    """Check that serve.get_deployment_handle is detected correctly by telemetry."""

    check_telemetry(ServeUsageTag.SERVE_GET_DEPLOYMENT_HANDLE_API_USED, expected=None)

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
            check_telemetry,
            tag=ServeUsageTag.SERVE_GET_DEPLOYMENT_HANDLE_API_USED,
            expected="1",
        )
    else:
        for _ in range(3):
            check_telemetry(
                ServeUsageTag.SERVE_GET_DEPLOYMENT_HANDLE_API_USED, expected=None
            )
            time.sleep(1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
