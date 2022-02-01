import pytest
import time

import ray
from ray import serve


def test_controller_inflight_requests_clear(serve_instance):
    controller = serve.api._global_client._controller
    initial_number_reqs = ray.get(controller._num_pending_goals.remote())

    @serve.deployment
    def test(_):
        return "hello"

    test.deploy()

    assert ray.get(controller._num_pending_goals.remote()) - initial_number_reqs == 0


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
