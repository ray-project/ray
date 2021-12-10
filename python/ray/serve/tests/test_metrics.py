import io
import os
import logging
from contextlib import redirect_stderr

import requests
import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.utils import block_until_http_ready


def test_serve_metrics(serve_instance):
    @serve.deployment(name="metrics")
    async def f(request):
        return "hello"

    f.deploy()

    # send 10 concurrent requests
    url = "http://127.0.0.1:8000/metrics"
    ray.get([block_until_http_ready.remote(url) for _ in range(10)])

    def verify_metrics(do_assert=False):
        try:
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
            return False

        expected_metrics = [
            # counter
            "num_router_requests_total",
            "num_http_requests_total",
            "deployment_queued_queries_total",
            "deployment_request_counter_requests_total",
            "deployment_worker_starts_restarts_total",
            # histogram
            "deployment_processing_latency_ms_bucket",
            "deployment_processing_latency_ms_count",
            "deployment_processing_latency_ms_sum",
            # gauge
            "replica_processing_queries",
            # handle
            "serve_handle_request_counter",
            # ReplicaSet
            "deployment_queued_queries"
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
        verify_metrics()


def test_deployment_logger(serve_instance):
    # Tests that deployment tag and replica tag appear in Serve log output.
    logger = logging.getLogger("ray")

    @serve.deployment(name="counter")
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, request):
            self.count += 1
            logger.info(f"count: {self.count}")

    Counter.deploy()
    f = io.StringIO()
    with redirect_stderr(f):
        requests.get("http://127.0.0.1:8000/counter/")

        def counter_log_success():
            s = f.getvalue()
            return "deployment" in s and "replica" in s and "count" in s

        wait_for_condition(counter_log_success)


def test_http_metrics(serve_instance):
    expected_metrics = [
        "serve_num_http_requests", "serve_num_http_error_requests"
    ]

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
    try:
        wait_for_condition(
            verify_metrics,
            retry_interval_ms=1000,
            timeout=10,
            expected_metrics=expected_metrics)
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
    try:
        wait_for_condition(
            verify_metrics,
            retry_interval_ms=1000,
            timeout=10,
            expected_metrics=expected_metrics)
    except RuntimeError:
        verify_metrics(expected_metrics, True)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
