import os
import sys
import time
from typing import Any, Dict

import pytest

os.environ["RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE"] = "0"
os.environ["RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S"] = "3"

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
                "target_num_ongoing_requests_per_replica": 2,
                "upscale_delay_s": 2,
                "downscale_delay_s": 10,
            }
        )
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
        handle = serve.run(
            DummyMetricIncrementer.bind(), name=app_name, route_prefix="/"
        )
        dep_id = DeploymentID(name="DummyMetricIncrementer", app_name=app_name)

        # Call deployment 3 times
        [handle.remote() for _ in range(3)]

        # Wait for controller to receive new metrics
        wait_for_condition(
            lambda _: "counter"
            in get_autoscaling_metrics_from_controller(serve_instance, dep_id),
            timeout=15,
        )
        metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)

        # The final counter value recorded by the controller should be 3
        assert metrics["counter"][-1][0].value == 3

    def test_custom_serve_timeout(self, serve_instance):
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "target_num_ongoing_requests_per_replica": 2,
                "upscale_delay_s": 2,
                "downscale_delay_s": 10,
            }
        )
        class DummyMetricTimeout:
            def __init__(self):
                self.counter = 0

            async def __call__(self) -> str:
                self.counter += 1
                return "Hello, world"

            async def record_autoscaling_stats(self) -> Dict[str, Any]:
                # Sleep beyond RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S
                time.sleep(12)
                return {"counter": self.counter}

        app_name = "test_custom_metrics_app"
        handle = serve.run(DummyMetricTimeout.bind(), name=app_name, route_prefix="/")
        dep_id = DeploymentID(name="DummyMetricTimeout", app_name=app_name)

        # Call deployment 3 times
        [handle.remote() for _ in range(3)]

        # Wait for controller to receive new metrics
        wait_for_condition(
            lambda: "counter"
            in get_autoscaling_metrics_from_controller(serve_instance, dep_id),
            timeout=10,
        )

        metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)

        # The should have no counter metric because asyncio timeout would have stopped the method execution
        assert metrics["counter"] == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
