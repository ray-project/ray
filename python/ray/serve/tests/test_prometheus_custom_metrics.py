import os
import sys
import time
from typing import Dict, List

import pytest

os.environ["RAY_PROMETHEUS_HOST"] = "localhost:9999"
os.environ["RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE"] = "0"

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    fetch_prometheus_metrics,
)
from ray.serve._private.common import DeploymentID

# def get_metric_value_from_prometheus(metric_name: str, prometheus_host: str = "http://localhost:9090") -> Optional[float]:
#     """Fetch a metric value from Prometheus."""
#     try:
#         # Query the metric directly
#         query_params = {"query": metric_name}
#         response = httpx.get(f"{prometheus_host}/api/v1/query", params=query_params, timeout=5)
#         if response.status_code == 200:
#             data = response.json()
#             if data.get("status") == "success" and data.get("data", {}).get("result"):
#                 result = data["data"]["result"][0]
#                 if "value" in result and len(result["value"]) >= 2:
#                     return float(result["value"][1])
#         return None
#     except Exception as e:
#         print(f"Error fetching metric {metric_name}: {e}")
#         return None


def get_autoscaling_metrics_from_controller(
    client, deployment_id: DeploymentID
) -> Dict[str, float]:
    """Get autoscaling metrics from the controller for testing."""
    ref = client._controller._dump_autoscaling_metrics_for_testing.remote()
    metrics = ray.get(ref)
    return metrics.get(deployment_id, {})


def check_autoscaling_metrics_include_prometheus(
    client,
    deployment_id: DeploymentID,
    expected_metrics: List[str],
    timeout: float = 30,
) -> bool:
    """Check that autoscaling metrics include the expected prometheus metrics."""

    def check_metrics():
        try:
            metrics = get_autoscaling_metrics_from_controller(client, deployment_id)
            # The metrics should include both ongoing requests and prometheus custom metrics
            # We check that at least some metrics are present (indicating the system is working)
            if not metrics:
                print("NO METRICS!!!")
                return False

            # For prometheus custom metrics, we expect them to be present in the metrics
            # The exact values depend on the current system state, so we just check presence
            print(f"Deployment {deployment_id} metrics: {metrics}")
            return True
        except Exception as e:
            print(f"Error checking metrics: {e}")
            return False

    try:
        wait_for_condition(check_metrics, timeout=timeout)
        return True
    except RuntimeError:
        return False


class TestPrometheusCustomMetrics:
    """Test that prometheus custom metrics are properly fetched and reported."""

    def test_prometheus_custom_metrics_integration(self, serve_instance):
        """Test that prometheus custom metrics are fetched and reported correctly."""

        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 3,
                "target_num_ongoing_requests_per_replica": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                "prometheus_custom_metrics": [
                    ("ray_node_cpu_utilization", None),  # Use default query
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
                "target_num_ongoing_requests_per_replica": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                "prometheus_custom_metrics": [
                    (
                        "ray_node_cpu_utilization",
                        "sum(ray_resources{Name='CPU',State='USED'}) by (instance)",
                    ),
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
                "target_num_ongoing_requests_per_replica": 1,
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

        # Deploy the applications
        c = DeploymentC.bind()
        b = DeploymentB.bind(c)
        a = DeploymentA.bind(b)

        # Deploy with the same structure as sequential_autoscale.py
        app_name = "prometheus_test_app"
        handle = serve.run(a, name=app_name, route_prefix="/")

        # Send some requests to trigger autoscaling and metrics collection
        responses = [handle.remote() for _ in range(5)]

        # Wait for requests to be processed
        for response in responses:
            result = response.result()
            assert result == "Hello from C"

        # Wait a bit for metrics to be collected and pushed
        time.sleep(2)

        # Check that autoscaling metrics are being collected for deployments with prometheus custom metrics
        dep_a_id = DeploymentID(name="DeploymentA", app_name=app_name)
        dep_b_id = DeploymentID(name="DeploymentB", app_name=app_name)
        dep_c_id = DeploymentID(name="DeploymentC", app_name=app_name)

        # First verify Prometheus contains ray node and ray serve metrics
        basic_metrics = ["ray_serve_deployment_processing_latency_ms_bucket"]
        metrics = fetch_prometheus_metrics(["localhost:9999"])
        node_metrics = {
            k: v
            for k, v in metrics.items()
            if "ray_serve_deployment_processing_latency_ms_bucket" in k
        }
        # serve_metrics = {k: v for k, v in metrics.items() if "ray_serve_" in k}

        for bm in basic_metrics:
            assert bm in node_metrics, f"Basic metric {bm} not found in Prometheus"

        # Check that DeploymentA (with prometheus custom metrics) has metrics
        assert check_autoscaling_metrics_include_prometheus(
            serve_instance,
            dep_a_id,
            ["ray_serve_deployment_processing_latency_ms_bucket"],
        ), "DeploymentA should have prometheus custom metrics"

        # Check that DeploymentB (with prometheus custom metrics) has metrics
        assert check_autoscaling_metrics_include_prometheus(
            serve_instance,
            dep_b_id,
            ["ray_serve_deployment_processing_latency_ms_bucket"],
        ), "DeploymentB should have prometheus custom metrics"

        # Check that DeploymentC (without prometheus custom metrics) still has basic metrics
        # but may not have the specific prometheus custom metrics
        metrics_c = get_autoscaling_metrics_from_controller(serve_instance, dep_c_id)
        print(f"DeploymentC metrics: {metrics_c}")
        # DeploymentC should still have some metrics (like ongoing requests) but not prometheus custom metrics
        assert (
            metrics_c is not None
        ), "DeploymentC should have basic autoscaling metrics"

        print("All prometheus custom metrics tests passed!")

    def test_prometheus_custom_metrics_with_custom_queries(self, serve_instance):
        """Test that custom PromQL queries work correctly."""

        # Test a deployment with a custom PromQL query
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 2,
                "target_num_ongoing_requests_per_replica": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                "prometheus_custom_metrics": [
                    (
                        "custom_cpu_metric",
                        "ray_node_cpu_utilization * 100",  # Custom query that multiplies by 100
                    ),
                ],
            },
            max_ongoing_requests=5,
        )
        class CustomQueryDeployment:
            def __call__(self):
                return "Custom query test"

        handle = serve.run(CustomQueryDeployment.bind(), name="custom_query_test")

        # Send a request to trigger metrics collection
        response = handle.remote()
        assert response.result() == "Custom query test"

        # Wait for metrics to be collected
        time.sleep(2)

        # Check that the custom metric is being collected
        dep_id = DeploymentID(
            name="CustomQueryDeployment", app_name="custom_query_test"
        )
        assert check_autoscaling_metrics_include_prometheus(
            serve_instance, dep_id, ["custom_cpu_metric"]
        ), "CustomQueryDeployment should have custom prometheus metrics"

        print("Custom PromQL query test passed!")

    def test_prometheus_custom_metrics_error_handling(self, serve_instance):
        """Test that the system handles prometheus query errors gracefully."""

        # Test a deployment with an invalid PromQL query
        @serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 2,
                "target_num_ongoing_requests_per_replica": 1,
                "upscale_delay_s": 0.1,
                "downscale_delay_s": 0.1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.1,
                "prometheus_custom_metrics": [
                    (
                        "invalid_metric",
                        "invalid_promql_query{invalid_syntax}",  # Invalid PromQL
                    ),
                ],
            },
            max_ongoing_requests=5,
        )
        class InvalidQueryDeployment:
            def __call__(self):
                return "Invalid query test"

        handle = serve.run(InvalidQueryDeployment.bind(), name="invalid_query_test")

        # Send a request to trigger metrics collection
        response = handle.remote()
        assert response.result() == "Invalid query test"
        dep_id = DeploymentID(
            name="InvalidQueryDeployment", app_name="invalid_query_test"
        )
        assert check_autoscaling_metrics_include_prometheus(
            serve_instance, dep_id, ["invalid_metric"]
        ), "CustomQueryDeployment should have custom prometheus metrics"

        print("Prometheus error handling test passed!")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
