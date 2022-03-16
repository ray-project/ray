import pytest
import time

import ray
from ray import serve
from ray.serve.api import internal_get_global_client


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = serve.api._global_client._controller

    @serve.deployment
    def test(_):
        return "1"

    test.deploy()
    deployment_info_1, route_1 = ray.get(controller.get_deployment_info.remote("test"))
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    test.deploy()
    deployment_info_2, route_2 = ray.get(controller.get_deployment_info.remote("test"))
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


@pytest.mark.parametrize("detached", [True, False])
def test_override_namespace(detached):
    """Test the _override_controller_namespace flag in serve.start()."""

    if ray.is_initialized():
        ray.shutdown()

    ray_namespace = "ray_namespace"
    controller_namespace = "controller_namespace"

    ray.init(namespace=ray_namespace)
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

    controller_name = internal_get_global_client()._controller_name
    ray.get_actor(controller_name, namespace=controller_namespace)

    serve.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
