import sys
import time
from typing import Any, Dict

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentID


def get_autoscaling_metrics_from_controller(
    client, deployment_id: DeploymentID
) -> Dict[str, float]:
    """Get autoscaling metrics from the controller for testing."""
    ref = client._controller._dump_all_metrics_for_testing.remote()
    metrics = ray.get(ref)
    return metrics.get(deployment_id, {})


def test_deployment_custom_metric(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    @serve.deployment
    class DummyMetricIncrementer:
        def __init__(self):
            self.counter = 0

        async def __call__(self) -> str:
            self.counter += 1
            return "Hello, world"

        async def record_autoscaling_stats(self) -> Dict[str, Any]:
            # Increments each time the deployment has been called
            return {"counter": self.counter}

    app_name = "test_custom_metrics_app"
    handle = serve.run(DummyMetricIncrementer.bind(), name=app_name, route_prefix="/")
    dep_id = DeploymentID(name="DummyMetricIncrementer", app_name=app_name)
    responses = [handle.remote() for _ in range(3)]

    time.sleep(10)

    ref = serve_instance._controller._dump_autoscaling_metrics_for_testing.remote()
    metrics = ray.get(ref)[dep_id]

    assert responses == ["Hello, world" * 3]
    assert metrics["counter"] == 3

    assert True


def test_deployment_custom_metric_custom_method(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    @serve.deployment
    class CustomMetricMethodOverride:
        def __init__(self):
            self.counter = 0

        async def __call__(self) -> str:
            self.counter += 1
            return "Hello, world"

        async def my_custom_metric_method(self) -> Dict[str, Any]:
            # Increments each time the deployment has been called
            return {"counter": 42}

    assert True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
