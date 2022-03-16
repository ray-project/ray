import pytest
import time

import ray
from ray import serve
from ray.serve.common import DeploymentInfo
from ray.serve.generated.serve_pb2 import DeploymentRoute


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = serve.api._global_client._controller

    @serve.deployment
    def test(_):
        return "1"

    test.deploy()
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test"))
    )
    deployment_info_1 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    test.deploy()
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test"))
    )
    deployment_info_2 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
