import time
import pytest

import ray
from ray._private.test_utils import wait_for_condition

from ray import serve
from ray.serve._private.common import DeploymentInfo
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)
from ray.serve.schema import ServeDeploySchema
from ray.serve._private.common import ApplicationStatus


def get_deployment_name(name: str):
    return f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}{name}"


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = serve.context._global_client._controller

    @serve.deployment
    def test(_):
        return "1"

    serve.run(test.bind())
    deployment_name = get_deployment_name("test")
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote(deployment_name))
    )
    deployment_info_1 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote(deployment_name))
    )
    deployment_info_2 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


def test_deploy_app_custom_exception(serve_instance):
    """Check that controller doesn't deserialize an exception from deploy_app."""

    client = serve_instance
    controller = serve.context.get_global_client()._controller

    config = {
        "applications": [
            {
                "name": "broken_app",
                "route_prefix": "/broken",
                "import_path": "ray.serve.tests.test_config_files.broken_app:app",
            }
        ]
    }

    ray.get(controller.deploy_apps.remote(config=ServeDeploySchema.parse_obj(config)))

    def check_custom_exception() -> bool:
        status = client.get_serve_status(name="broken_app")
        assert status.app_status.status == ApplicationStatus.DEPLOY_FAILED
        assert "custom exception info" in status.app_status.message
        return True

    wait_for_condition(check_custom_exception, timeout=10)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
