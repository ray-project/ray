import io
import logging
from contextlib import redirect_stderr

import requests
import pytest

import ray
from ray import serve
from ray.test_utils import wait_for_condition
from ray.serve.utils import block_until_http_ready


def test_serve_metrics(serve_instance):
    @serve.accept_batch
    def batcher(starlette_requests):
        return ["hello"] * len(starlette_requests)

    serve.create_backend("metrics", batcher)
    serve.create_endpoint("metrics", backend="metrics", route="/metrics")

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
            "backend_queued_queries_total",
            "backend_request_counter_requests_total",
            "backend_worker_starts_restarts_total",
            # histogram
            "backend_processing_latency_ms_bucket",
            "backend_processing_latency_ms_count",
            "backend_processing_latency_ms_sum",
            "backend_queuing_latency_ms_bucket",
            "backend_queuing_latency_ms_count",
            "backend_queuing_latency_ms_sum",
            # gauge
            "replica_processing_queries",
            "replica_queued_queries",
            # handle
            "serve_handle_request_counter",
            # ReplicaSet
            "backend_queued_queries"
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


def test_backend_logger(serve_instance):
    # Tests that backend tag and replica tag appear in Serve log output.
    logger = logging.getLogger("ray")

    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, request):
            self.count += 1
            logger.info(f"count: {self.count}")

    serve.create_backend("my_backend", Counter)
    serve.create_endpoint(
        "my_endpoint", backend="my_backend", route="/counter")
    f = io.StringIO()
    with redirect_stderr(f):
        requests.get("http://127.0.0.1:8000/counter")

        def counter_log_success():
            s = f.getvalue()
            return "backend" in s and "replica" in s and "count" in s

        wait_for_condition(counter_log_success)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
