import asyncio
import sys
from typing import Dict

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.test_utils import check_num_replicas_eq
from ray.serve.config import AutoscalingContext, AutoscalingPolicy


def get_autoscaling_metrics_from_controller(
    client, deployment_id: DeploymentID
) -> Dict[str, float]:
    """Get autoscaling metrics from the controller for testing."""
    ref = client._controller._get_metrics_for_deployment_for_testing.remote(
        deployment_id
    )
    return ray.get(ref)


def custom_autoscaling_policy(ctx: AutoscalingContext):
    aggregated_counter = sum(
        x for x in ctx.aggregated_metrics.get("counter", {}).values()
    )
    max_counter = sum(
        [x[-1].value for x in ctx.raw_metrics.get("counter", {}).values()]
    )
    if max_counter == aggregated_counter == 10:
        return 3, {}
    else:
        return 1, {}


# Example from doc/source/serve/doc_code/autoscaling_policy.py
def max_cpu_usage_autoscaling_policy(ctx: AutoscalingContext):
    cpu_usage_metric = ctx.aggregated_metrics.get("cpu_usage", {})
    max_cpu_usage = list(cpu_usage_metric.values())[-1] if cpu_usage_metric else 0

    if max_cpu_usage > 80:
        return min(ctx.capacity_adjusted_max_replicas, ctx.current_num_replicas + 1), {}
    elif max_cpu_usage < 30:
        return max(ctx.capacity_adjusted_min_replicas, ctx.current_num_replicas - 1), {}
    else:
        return ctx.current_num_replicas, {}


class TestCustomServeMetrics:
    """Check that redeploying a deployment doesn't reset its start time."""

    def test_custom_serve_metrics(self, serve_instance):
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 0.5,
                "downscale_delay_s": 0.5,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 1,
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
                "look_back_period_s": 1,
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
                "look_back_period_s": 1,
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

    def test_policy_using_custom_metrics(self, serve_instance):
        signal = SignalActor.remote()

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 2,
                "downscale_delay_s": 1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 1,
                "target_ongoing_requests": 10,
                "policy": AutoscalingPolicy(policy_function=custom_autoscaling_policy),
            },
            max_ongoing_requests=100,
        )
        class CustomMetricsDeployment:
            def __init__(self):
                self.counter = 0

            async def __call__(self) -> str:
                self.counter += 1
                await signal.wait.remote()
                return "Hello, world"

            def record_autoscaling_stats(self) -> Dict[str, int]:
                return {"counter": self.counter}

        handle = serve.run(CustomMetricsDeployment.bind())
        [handle.remote() for _ in range(10)]
        wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 10)
        wait_for_condition(
            check_num_replicas_eq, name="CustomMetricsDeployment", target=3
        )
        signal.send.remote()

    def test_max_cpu_usage_autoscaling_policy(self, serve_instance):
        """Test autoscaling policy based on max CPU usage from documentation example."""
        signal = SignalActor.remote()

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 0.5,
                "downscale_delay_s": 0.5,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 1,
                "target_ongoing_requests": 10,
                "policy": AutoscalingPolicy(
                    policy_function=max_cpu_usage_autoscaling_policy
                ),
            },
            max_ongoing_requests=100,
        )
        class MaxCpuUsageDeployment:
            def __init__(self):
                self.cpu_usage = 0

            async def __call__(self) -> str:
                self.cpu_usage += 1
                await signal.wait.remote()
                return "Hello, world"

            def record_autoscaling_stats(self) -> Dict[str, int]:
                return {"cpu_usage": self.cpu_usage}

        handle = serve.run(MaxCpuUsageDeployment.bind())

        # Test scale up when CPU usage > 80
        # Set CPU usage to 90 to trigger scale up
        dep_id = DeploymentID(name="MaxCpuUsageDeployment")

        # Send requests to increase CPU usage
        [handle.remote() for _ in range(90)]
        wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 90)

        # Wait for metrics to be recorded and policy to trigger scale up
        def check_scale_up():
            metrics = get_autoscaling_metrics_from_controller(serve_instance, dep_id)
            return "cpu_usage" in metrics and metrics["cpu_usage"][-1][0].value >= 90

        wait_for_condition(check_scale_up, timeout=10)

        # Should scale up to 2 replicas due to high CPU usage
        wait_for_condition(
            check_num_replicas_eq, name="MaxCpuUsageDeployment", target=2, timeout=15
        )

        # Release signal and test scale down when CPU usage < 30
        signal.send.remote()
        wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 0)

        signal = SignalActor.remote()
        # Reset CPU usage to low value by creating new deployment instance
        # This simulates low CPU usage scenario
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "upscale_delay_s": 0.5,
                "downscale_delay_s": 0.5,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 1,
                "target_ongoing_requests": 10,
                "policy": AutoscalingPolicy(
                    policy_function=max_cpu_usage_autoscaling_policy
                ),
            },
            max_ongoing_requests=100,
        )
        class LowCpuUsageDeployment:
            def __init__(self):
                self.cpu_usage = 0

            async def __call__(self) -> str:
                self.cpu_usage += 1
                await signal.wait.remote()
                return "Hello, world"

            def record_autoscaling_stats(self) -> Dict[str, int]:
                # Return low CPU usage to trigger scale down
                return {"cpu_usage": 20}

        handle = serve.run(LowCpuUsageDeployment.bind())

        # Send a few requests to establish low CPU usage
        [handle.remote() for _ in range(5)]
        wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 5)

        # Wait for metrics to be recorded
        dep_id_low = DeploymentID(name="LowCpuUsageDeployment")

        def check_low_cpu():
            metrics = get_autoscaling_metrics_from_controller(
                serve_instance, dep_id_low
            )
            return "cpu_usage" in metrics and metrics["cpu_usage"][-1][0].value <= 30

        wait_for_condition(check_low_cpu, timeout=10)

        # Should downscale to 1 replica due to low CPU usage
        wait_for_condition(
            check_num_replicas_eq, name="LowCpuUsageDeployment", target=1, timeout=15
        )

        signal.send.remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
