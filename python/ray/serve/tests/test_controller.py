import pytest
import time
import requests

import ray
from ray._private.test_utils import wait_for_condition

from ray import serve
from ray.serve.common import DeploymentInfo
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve.schema import ServeApplicationSchema


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = serve.context._global_client._controller

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


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
class Waiter:
    def __init__(self):
        time.sleep(5)

    def __call__(self, *args):
        return "May I take your order?"


WaiterNode = Waiter.bind()


def test_run_graph_task_uses_zero_cpus():
    """Check that the run_graph() task uses zero CPUs."""

    ray.init(num_cpus=2)
    client = serve.start(detached=True)

    config = {"import_path": "ray.serve.tests.test_controller.WaiterNode"}
    config = ServeApplicationSchema.parse_obj(config)
    client.deploy_app(config)

    with pytest.raises(RuntimeError):
        wait_for_condition(lambda: ray.available_resources()["CPU"] < 1.9, timeout=5)

    wait_for_condition(
        lambda: requests.get("http://localhost:8000/Waiter").text
        == "May I take your order?"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
