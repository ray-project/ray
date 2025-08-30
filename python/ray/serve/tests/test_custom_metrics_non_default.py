import os
import sys
import time
from typing import Any, Dict

import pytest

os.environ["RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE"] = "0"
os.environ["RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S"] = "3"
os.environ[
    "RAY_SERVE_AUTOSCALING_STATS_METHOD"
] = "record_autoscaling_stats_non_default_method"

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


class TestCustomServeMetricsNonDefault:
    def test_custom_serve_metrics_non_default(self, serve_instance):
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "target_num_ongoing_requests_per_replica": 2,
                "upscale_delay_s": 2,
                "downscale_delay_s": 10,
            }
        )
        class DummyMetricDeploymentNonDefault:
            def __init__(self):
                self.counter = 0

            async def __call__(self) -> str:
                self.counter += 1
                return "Hello, world"

            async def record_autoscaling_stats(self) -> Dict[str, Any]:
                # Increments each time the deployment has been called
                return {"counter": self.counter}

            async def record_autoscaling_stats_non_default_method(
                self,
            ) -> Dict[str, Any]:
                # Enabled only by setting RAY_SERVE_AUTOSCALING_STATS_METHOD env var
                return {"non_default_method_autoscaling_metric": 42}

        app_name = "test_custom_metrics_app_non_default"
        handle = serve.run(
            DummyMetricDeploymentNonDefault.bind(), name=app_name, route_prefix="/"
        )
        dep_id = DeploymentID(name="DummyMetricDeploymentNonDefault", app_name=app_name)

        # Call deployment 3 times
        [handle.remote() for _ in range(3)]

        # Wait for controller to receive new metrics
        time.sleep(10)
        metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)

        # The final counter value recorded by the controller should be 42
        assert metrics["non_default_method_autoscaling_metric"][-1][0].value == 42


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
