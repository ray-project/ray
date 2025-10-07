import asyncio
import sys
from typing import Dict

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID


def get_autoscaling_metrics_from_controller(
    client, deployment_id: DeploymentID
) -> Dict[str, float]:
    """Get autoscaling metrics from the controller for testing."""
    ref = client._controller._dump_all_autoscaling_metrics_for_testing.remote()
    metrics = ray.get(ref)
    return metrics.get(deployment_id, {})


class TestCustomServeMetrics:
    """Check that redeploying a deployment doesn't reset its start time."""

    def test_custom_serve_metrics(self, serve_instance):
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 2,
                "downscale_delay_s": 10,
                "metrics_interval_s": 1,
            }
        )
        class DummyMetricIncrementer:
            def __init__(self):
                self.counter = 0

            async def __call__(self) -> str:
                self.counter += 1
                return "Hello, world"

            def record_autoscaling_stats(self) -> Dict[str, int]:
                # Increments each time the deployment has been called
                return {"counter": self.counter}

        app_name = "test_custom_metrics_app"
        handle = serve.run(
            DummyMetricIncrementer.bind(), name=app_name, route_prefix="/"
        )
        dep_id = DeploymentID(name="DummyMetricIncrementer", app_name=app_name)

        # Call deployment 3 times
        [handle.remote() for _ in range(3)]

        def check_counter_value():
            metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)
            return "counter" in metrics and metrics["counter"][-1][0].value == 3

        # The final counter value recorded by the controller should be 3
        wait_for_condition(
            check_counter_value,
            timeout=15,
        )

    def test_custom_serve_timeout(self, serve_instance):
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 2,
                "downscale_delay_s": 10,
                "metrics_interval_s": 1,
            }
        )
        class DummyMetricTimeout:
            def __init__(self):
                self.counter = 0

            async def __call__(self) -> str:
                self.counter += 1
                return "Hello, world"

            async def record_autoscaling_stats(self) -> Dict[str, int]:
                # Block here until it is forced to cancel due to timeout beyond RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S
                await asyncio.sleep(1000)

        app_name = "test_custom_metrics_app"
        handle = serve.run(DummyMetricTimeout.bind(), name=app_name, route_prefix="/")
        dep_id = DeploymentID(name="DummyMetricTimeout", app_name=app_name)
        # Call deployment 3 times
        [handle.remote() for _ in range(3)]
        # There should be no counter metric because asyncio timeout would have stopped the method execution
        metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)
        assert metrics.get("counter", None) is None

    def test_custom_serve_invalid_metric_type(self, serve_instance):
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 2,
                "downscale_delay_s": 10,
                "metrics_interval_s": 1,
            }
        )
        class DummyInvalidMetric:
            def __init__(self):
                self.counter = 0

            async def __call__(self) -> str:
                self.counter += 1
                return "Hello, world"

            def record_autoscaling_stats(self) -> Dict[str, str]:
                # Return an invalid metric dict whose valuse are neither int nor float
                return {"counter": "not_an_int"}

        app_name = "test_custom_metrics_app"
        handle = serve.run(DummyInvalidMetric.bind(), name=app_name, route_prefix="/")
        dep_id = DeploymentID(name="DummyInvalidMetric", app_name=app_name)
        # Call deployment 3 times
        [handle.remote() for _ in range(3)]
        # There should be no counter metric because it failed validation, must be int or float
        metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)
        assert metrics.get("counter", None) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
