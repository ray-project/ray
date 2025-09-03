import json
import os
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentID


class MockPrometheusHandler(BaseHTTPRequestHandler):
    # You can customize this dict to return different results for different queries
    DUMMY_METRICS = {
        "ray_serve_deployment_processing_latency_ms_bucket": [
            {
                "metric": {
                    "deployment": "DeploymentA",
                    "app": "prometheus_test_app",
                    "actor_id": "actor1",
                },
                "value": [1234567890, 1900],
            },
            {
                "metric": {
                    "deployment": "DeploymentA",
                    "app": "prometheus_test_app",
                    "actor_id": "actor2",
                },
                "value": [1234567890, 2000],
            },
            {
                "metric": {
                    "deployment": "DeploymentB",
                    "app": "prometheus_test_app",
                    "actor_id": "actor1",
                },
                "value": [1234567890, 500],
            },
        ],
        'ray_serve_deployment_processing_latency_ms_bucket{deployment="B",application="prometheus_test_app"}': [
            {
                "metric": {
                    "deployment": "DeploymentB",
                    "app": "prometheus_test_app",
                    "actor_id": "actor1",
                },
                "value": [1234567890, 500],
            }
        ],
    }

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/v1/query":
            query_params = urllib.parse.parse_qs(parsed.query)
            promql = query_params.get("query", [""])[0]
            results = self.DUMMY_METRICS.get(promql, [])
            response = {
                "status": "success",
                "data": {"resultType": "vector", "result": results},
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Silence the default logging
        return


MOCK_PROMETHEUS_PORT = 9091
os.environ["RAY_PROMETHEUS_HOST"] = f"http://localhost:{MOCK_PROMETHEUS_PORT}"
os.environ["RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE"] = "0"


@pytest.fixture(scope="module", autouse=True)
def start_mock_prometheus_host():
    server = HTTPServer(("localhost", MOCK_PROMETHEUS_PORT), MockPrometheusHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield
    server.shutdown()
    thread.join()


def get_autoscaling_metrics_from_controller(
    client, deployment_id: DeploymentID
) -> Dict[str, float]:
    """Get autoscaling metrics from the controller for testing."""
    ref = client._controller._dump_all_metrics_for_testing.remote()
    metrics = ray.get(ref)
    return metrics.get(deployment_id, {})


def check_autoscaling_metrics_include_prometheus(
    client,
    deployment_id: DeploymentID,
    expected_metrics: List[str],
    timeout: float = 30,
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
                    (
                        "ray_serve_deployment_processing_latency_ms_bucket",
                        None,
                    ),  # PromQL query is optional
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
                        "ray_serve_deployment_processing_latency_ms_bucket_only_b",
                        'ray_serve_deployment_processing_latency_ms_bucket{deployment="B",application="prometheus_test_app"}',
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

        # Check that DeploymentA (with prometheus custom metrics) has metrics
        assert check_autoscaling_metrics_include_prometheus(
            serve_instance,
            dep_a_id,
            ["ray_serve_deployment_processing_latency_ms_bucket"],
        ), "DeploymentA should have ray_serve_deployment_processing_latency_ms_bucket"

        # Check that DeploymentB (with prometheus custom metrics) has filtered metrics
        assert check_autoscaling_metrics_include_prometheus(
            serve_instance,
            dep_b_id,
            ["ray_serve_deployment_processing_latency_ms_bucket_only_b"],
        ), "DeploymentB should have filtered prometheus metrics"

        # DeploymentC should still have ongoing request metric but not prometheus custom metrics
        assert not check_autoscaling_metrics_include_prometheus(
            serve_instance,
            dep_c_id,
            ["ray_serve_deployment_processing_latency_ms_bucket"],
        ), "DeploymentC should have no custom metrics other than queue length"

        print("All prometheus custom metrics tests passed!")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
