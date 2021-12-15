import io
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
            # gauge
            "replica_processing_queries",
            # handle
            "serve_handle_request_counter"
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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
