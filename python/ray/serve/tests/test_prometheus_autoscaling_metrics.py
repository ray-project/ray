import asyncio
import sys
import time
from typing import Dict, List

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.replica import ReplicaMetricsManager


class MockReplicaId:
    def __init__(self, unique_id):
        self.unique_id = unique_id


class MockAutoscalingConfig:
    def __init__(self, prometheus_metrics):
        self.prometheus_metrics = prometheus_metrics
        self.metrics_interval_s = 0.1
        self.look_back_period_s = 0.1


def get_autoscaling_metrics_from_controller(
    client, deployment_id: DeploymentID
) -> Dict[str, float]:
    """Get autoscaling metrics from the controller for testing."""
    ref = client._controller._dump_all_autoscaling_metrics_for_testing.remote()
    metrics = ray.get(ref)
    return metrics.get(deployment_id, {})


def check_autoscaling_metrics_include_prometheus(
    client, deployment_id: DeploymentID, expected_metrics: List[str]
) -> bool:
    """Check that autoscaling metrics include the expected prometheus metrics."""

    try:
        metrics = get_autoscaling_metrics_from_controller(client, deployment_id)
        # The metrics should include both ongoing requests and prometheus custom metrics
        if not metrics:
            print("No metrics returned from controller!")
            return False

        # For prometheus custom metrics, we expect the keys to be present in the dict
        for expected_metric in expected_metrics:
            if expected_metric not in metrics:
                print(f"Expected metric {expected_metric} not found")
                return False
        return True
    except Exception as e:
        print(f"Error checking metrics: {e}")
        return False


class TestPrometheusCustomMetrics:
    """Test that prometheus custom metrics are properly fetched and reported."""

    def test_prometheus_metrics_integration(self, serve_instance):
        """Test that prometheus custom metrics are fetched and reported correctly."""

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 3,
                "target_ongoing_requests": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                "prometheus_metrics": [
                    "ray_serve_deployment_processing_latency_ms_bucket"
                ],
            },
            max_ongoing_requests=10,
        )
        class DeploymentA:
            def __init__(self, b):
                self.b = b

            async def __call__(self):
                print("DeploymentA called")
                response = self.b.remote()
                return await response

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 3,
                "target_ongoing_requests": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                "prometheus_metrics": [
                    "ray_serve_deployment_processing_latency_ms_bucket_only_b"
                ],
            },
            max_ongoing_requests=10,
        )
        class DeploymentB:
            def __init__(self, c):
                self.c = c

            async def __call__(self):
                print("DeploymentB called")
                response = self.c.remote()
                return await response

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 3,
                "target_ongoing_requests": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                # No prometheus custom metrics
            },
            max_ongoing_requests=10,
        )
        class DeploymentC:
            def __call__(self):
                print("DeploymentC called")
                return "Hello from C"

        # Deploy the sequential application
        c = DeploymentC.bind()
        b = DeploymentB.bind(c)
        a = DeploymentA.bind(b)

        app_name = "prometheus_test_app"
        handle = serve.run(a, name=app_name, route_prefix="/")

        # Send some requests to trigger autoscaling and metrics collection
        responses = [handle.remote() for _ in range(5)]

        # Wait for requests to be processed
        for response in responses:
            result = response.result()
            assert result == "Hello from C"

        # Wait a bit for metrics to be collected and pushed
        time.sleep(10)

        # Check that autoscaling metrics are being collected for deployments with prometheus custom metrics
        dep_a_id = DeploymentID(name="DeploymentA", app_name=app_name)
        dep_b_id = DeploymentID(name="DeploymentB", app_name=app_name)
        dep_c_id = DeploymentID(name="DeploymentC", app_name=app_name)

        # Wait for controller to receive new metrics
        wait_for_condition(
            lambda: check_autoscaling_metrics_include_prometheus(
                serve_instance,
                dep_a_id,
                ["ray_serve_deployment_processing_latency_ms_bucket"],
            ),
            timeout=15,
        )

        wait_for_condition(
            lambda: check_autoscaling_metrics_include_prometheus(
                serve_instance,
                dep_b_id,
                ["ray_serve_deployment_processing_latency_ms_bucket_only_b"],
            ),
            timeout=15,
        )

        wait_for_condition(
            lambda: not check_autoscaling_metrics_include_prometheus(
                serve_instance,
                dep_c_id,
                ["ray_serve_deployment_processing_latency_ms_bucket"],
            ),
            timeout=15,
        )

        print("All prometheus custom metrics tests passed!")

    # Dependency injection test for promQL filtering logic
    @pytest.mark.unit
    def test_promql_filtering_with_mock_prom_serve():
        # Mock prom_serve to simulate Prometheus server response
        def mock_prom_serve(host, query, **kwargs):
            # Simulate filtering by replica label in PromQL
            if (
                "ray_serve_deployment_processing_latency_ms_bucket" in query
                and 'replica="replica-123"' in query
            ):
                return {"data": {"result": [{"value": [1234567890, 42]}]}}
            return {"data": {"result": []}}

        # Create a metrics manager with the mock prom_serve injected
        metrics_manager = ReplicaMetricsManager(
            replica_id=MockReplicaId("replica-123"),
            event_loop=asyncio.get_event_loop(),
            autoscaling_config=MockAutoscalingConfig(
                ["ray_serve_deployment_processing_latency_ms_bucket"]
            ),
            ingress=True,
            prom_query_func=mock_prom_serve,
        )

        # Run the promQL filtering logic
        result = asyncio.get_event_loop().run_until_complete(
            metrics_manager._fetch_prometheus_metrics(
                ["ray_serve_deployment_processing_latency_ms_bucket"]
            )
        )
        assert result["ray_serve_deployment_processing_latency_ms_bucket"] == 42


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
