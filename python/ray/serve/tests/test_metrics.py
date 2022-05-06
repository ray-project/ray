import os

import requests
import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.utils import block_until_http_ready


def test_serve_metrics_for_successful_connection(serve_instance):
    @serve.deployment(name="metrics")
    async def f(request):
        return "hello"

    f.deploy()

    # send 10 concurrent requests
    url = "http://127.0.0.1:8000/metrics"
    handle = f.get_handle()
    ray.get([block_until_http_ready.remote(url) for _ in range(10)])
    ray.get([handle.remote(url) for _ in range(10)])

    def verify_metrics(do_assert=False):
        try:
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
            return False

        expected_metrics = [
            # counter
            "serve_num_router_requests",
            "serve_num_http_requests",
            "serve_deployment_queued_queries",
            "serve_deployment_request_counter",
            "serve_deployment_replica_starts",
            # histogram
            "deployment_processing_latency_ms_bucket",
            "deployment_processing_latency_ms_count",
            "deployment_processing_latency_ms_sum",
            "serve_deployment_processing_latency_ms",
            # gauge
            "serve_replica_processing_queries",
            # handle
            "serve_handle_request_counter",
        ]
        for metric in expected_metrics:
            # For the final error round
            if do_assert:
                assert metric in resp
            # For the wait_for_condition
            else:
                if metric not in resp:
                    return False
        return True

    try:
        wait_for_condition(verify_metrics, retry_interval_ms=500)
    except RuntimeError:
        verify_metrics(do_assert=True)


def test_http_metrics(serve_instance):
    expected_metrics = ["serve_num_http_requests", "serve_num_http_error_requests"]

    def verify_metrics(expected_metrics, do_assert=False):
        try:
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
            return False
        for metric in expected_metrics:
            if do_assert:
                assert metric in resp
            if metric not in resp:
                return False
        return True

    # Trigger HTTP 404 error
    requests.get("http://127.0.0.1:8000/B/")
    requests.get("http://127.0.0.1:8000/B/")
    try:
        wait_for_condition(
            verify_metrics,
            retry_interval_ms=1000,
            timeout=10,
            expected_metrics=expected_metrics,
        )
    except RuntimeError:
        verify_metrics(expected_metrics, True)

    expected_metrics.append("serve_num_deployment_http_error_requests")

    @serve.deployment(name="A")
    class A:
        async def __init__(self):
            pass

        async def __call__(self):
            # Trigger RayActorError
            os._exit(0)

    A.deploy()
    requests.get("http://127.0.0.1:8000/A/")
    requests.get("http://127.0.0.1:8000/A/")
    try:
        wait_for_condition(
            verify_metrics,
            retry_interval_ms=1000,
            timeout=10,
            expected_metrics=expected_metrics,
        )
    except RuntimeError:
        verify_metrics(expected_metrics, True)

    def verify_error_count(do_assert=False):
        resp = requests.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_num_http_error_requests" in metrics:
                # both route "/A/" and "/B/" should have error count 2
                if do_assert:
                    assert "2.0" in metrics
                if "2.0" not in metrics:
                    return False
            elif "serve_num_deployment_http_error_requests" in metrics:
                # deployment A should have error count 2
                if do_assert:
                    assert 'deployment="A"' in metrics and "2.0" in metrics
                if 'deployment="A"' not in metrics or "2.0" not in metrics:
                    return False
        return True

    # There is a latency in updating the counter
    try:
        wait_for_condition(verify_error_count, retry_interval_ms=1000, timeout=10)
    except RuntimeError:
        verify_error_count(do_assert=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
