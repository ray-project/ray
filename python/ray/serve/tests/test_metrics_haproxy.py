"""
HAProxy metrics tests for Ray Serve.

These tests verify that Ray Serve metrics work correctly when HAProxy is enabled
as a replacement for the default Serve HTTP proxy.

Key differences from the default Serve proxy:
1. When HAProxy is enabled, RAY_SERVE_ENABLE_DIRECT_INGRESS is automatically set.
2. HTTP proxy metrics (serve_num_http_requests, etc.) are emitted from replicas when
   they receive direct ingress requests from HAProxy.
3. 404 errors for non-existent routes are handled by HAProxy itself (not forwarded to
   replicas), so these won't generate Serve metrics. Tests that need to verify 404
   metrics must deploy an application that returns 404s.
4. HAProxy has its own metrics exposed on a separate port (default 9101), but these
   tests focus on Serve metrics exposed via the Ray metrics port (9999).
"""
import http
import json
import sys
from typing import Dict, Optional

import httpx
import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import PlainTextResponse

import ray
from ray import serve
from ray._common.network_utils import parse_address
from ray._common.test_utils import (
    SignalActor,
    fetch_prometheus_metrics,
    wait_for_condition,
)
from ray._common.utils import reset_ray_address
from ray.serve import HTTPOptions
from ray.serve._private.long_poll import LongPollHost, UpdatedObject
from ray.serve._private.test_utils import (
    get_application_url,
)
from ray.serve._private.utils import block_until_http_ready
from ray.serve.tests.conftest import TEST_METRICS_EXPORT_PORT
from ray.serve.tests.test_metrics import get_metric_dictionaries
from ray.util.state import list_actors


@pytest.fixture
def metrics_start_shutdown(request):
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    param = request.param if hasattr(request, "param") else None
    request_timeout_s = param if param else None
    ray.init(
        _metrics_export_port=TEST_METRICS_EXPORT_PORT,
        _system_config={
            "metrics_report_interval_ms": 100,
            "task_retry_delay_ms": 50,
        },
    )
    yield serve.start(
        http_options=HTTPOptions(
            host="0.0.0.0",
            request_timeout_s=request_timeout_s,
        ),
    )
    serve.shutdown()
    ray.shutdown()
    reset_ray_address()


def extract_tags(line: str) -> Dict[str, str]:
    """Extracts any tags from the metrics line."""

    try:
        tags_string = line.replace("{", "}").split("}")[1]
    except IndexError:
        # No tags were found in this line.
        return {}

    detected_tags = {}
    for tag_pair in tags_string.split(","):
        sanitized_pair = tag_pair.replace('"', "")
        tag, value = sanitized_pair.split("=")
        detected_tags[tag] = value

    return detected_tags


def contains_tags(line: str, expected_tags: Optional[Dict[str, str]] = None) -> bool:
    """Checks if the metrics line contains the expected tags.

    Does nothing if expected_tags is None.
    """

    if expected_tags is not None:
        detected_tags = extract_tags(line)

        # Check if expected_tags is a subset of detected_tags
        return expected_tags.items() <= detected_tags.items()
    else:
        return True


def get_metric_float(
    metric: str, expected_tags: Optional[Dict[str, str]] = None
) -> float:
    """Gets the float value of metric.

    If tags is specified, searched for metric with matching tags.

    Returns -1 if the metric isn't available.
    """

    metrics = httpx.get("http://127.0.0.1:9999").text
    metric_value = -1
    for line in metrics.split("\n"):
        if metric in line and contains_tags(line, expected_tags):
            metric_value = line.split(" ")[-1]
    return metric_value


def check_metric_float_eq(
    metric: str, expected: float, expected_tags: Optional[Dict[str, str]] = None
) -> bool:
    metric_value = get_metric_float(metric, expected_tags)
    assert float(metric_value) == expected
    return True


def check_sum_metric_eq(
    metric_name: str,
    expected: float,
    tags: Optional[Dict[str, str]] = None,
) -> bool:
    if tags is None:
        tags = {}

    metrics = fetch_prometheus_metrics(["localhost:9999"])
    metrics = {k: v for k, v in metrics.items() if "ray_serve_" in k}
    metric_samples = metrics.get(metric_name, None)
    if metric_samples is None:
        metric_sum = 0
    else:
        metric_samples = [
            sample for sample in metric_samples if tags.items() <= sample.labels.items()
        ]
        metric_sum = sum(sample.value for sample in metric_samples)

    # Check the metrics sum to the expected number
    assert float(metric_sum) == float(expected), (
        f"The following metrics don't sum to {expected}: "
        f"{json.dumps(metric_samples, indent=4)}\n."
        f"All metrics: {json.dumps(metrics, indent=4)}"
    )

    # # For debugging
    if metric_samples:
        print(f"The following sum to {expected} for '{metric_name}' and tags {tags}:")
        for sample in metric_samples:
            print(sample)

    return True


def test_serve_metrics_for_successful_connection(metrics_start_shutdown):
    @serve.deployment(name="metrics")
    async def f(request):
        return "hello"

    app_name = "app1"
    handle = serve.run(target=f.bind(), name=app_name)

    http_url = get_application_url(app_name=app_name)
    # send 10 concurrent requests
    ray.get([block_until_http_ready.remote(http_url) for _ in range(10)])
    [handle.remote(http_url) for _ in range(10)]

    def verify_metrics(do_assert=False):
        try:
            resp = httpx.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except httpx.HTTPError:
            return False

        # NOTE: These metrics should be documented at
        # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
        # Any updates to here should be reflected there too.
        expected_metrics = [
            # counter
            "ray_serve_num_router_requests",
            "ray_serve_num_http_requests",
            "ray_serve_deployment_queued_queries",
            "ray_serve_deployment_request_counter",
            "ray_serve_deployment_replica_starts",
            # histogram
            "ray_serve_deployment_processing_latency_ms_bucket",
            "ray_serve_deployment_processing_latency_ms_count",
            "ray_serve_deployment_processing_latency_ms_sum",
            "ray_serve_deployment_processing_latency_ms",
            # gauge
            "ray_serve_replica_processing_queries",
            "ray_serve_deployment_replica_healthy",
            # handle
            "ray_serve_handle_request_counter",
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


def test_http_replica_gauge_metrics(metrics_start_shutdown):
    """Test http replica gauge metrics"""
    signal = SignalActor.remote()

    @serve.deployment(graceful_shutdown_timeout_s=0.0001)
    class A:
        async def __call__(self):
            await signal.wait.remote()

    handle = serve.run(A.bind(), name="app1")
    _ = handle.remote()

    processing_requests = get_metric_dictionaries(
        "ray_serve_replica_processing_queries", timeout=5
    )
    assert len(processing_requests) == 1
    assert processing_requests[0]["deployment"] == "A"
    assert processing_requests[0]["application"] == "app1"
    print("ray_serve_replica_processing_queries exists.")

    def ensure_request_processing():
        resp = httpx.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "ray_serve_replica_processing_queries" in metrics:
                assert "1.0" in metrics
        return True

    wait_for_condition(ensure_request_processing, timeout=5)


def test_proxy_metrics_not_found(metrics_start_shutdown):
    # NOTE: When using HAProxy, 404 errors for non-existent routes are handled
    # by HAProxy itself (not forwarded to replicas), so we need to deploy an
    # application and test 404s within that application's context.
    # These metrics should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
    expected_metrics = [
        "ray_serve_num_http_requests",
        "ray_serve_num_http_error_requests_total",
        "ray_serve_num_deployment_http_error_requests",
        "ray_serve_http_request_latency_ms",
    ]

    app = FastAPI()

    @serve.deployment(name="A")
    @serve.ingress(app)
    class A:
        @app.get("/existing-path")  # Only this path is defined
        async def handler(self, request: Request):
            return {"message": "success"}

    app_name = "app"
    serve.run(A.bind(), name=app_name, route_prefix="/A")

    def verify_metrics(_expected_metrics, do_assert=False):
        try:
            resp = httpx.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except httpx.HTTPError:
            return False
        for metric in _expected_metrics:
            if do_assert:
                assert metric in resp
            if metric not in resp:
                return False
        return True

    # Trigger HTTP 404 error via the deployed application
    httpx.get("http://127.0.0.1:8000/A/nonexistent")
    httpx.get("http://127.0.0.1:8000/A/nonexistent")

    # Ensure all expected metrics are present.
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
        resp = httpx.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        http_error_count = 0
        deployment_404_count = 0

        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            # Skip health check metrics
            if "/-/healthz" in metrics:
                continue
            if (
                "ray_serve_num_http_error_requests_total" in metrics
                and 'route="/A"' in metrics
            ):
                # Accumulate error counts from route "/A"
                http_error_count += int(float(metrics.split(" ")[-1]))
            elif (
                "ray_serve_num_deployment_http_error_requests_total" in metrics
                and 'route="/A"' in metrics
                and 'error_code="404"' in metrics
            ):
                # Count deployment 404 errors
                deployment_404_count += int(float(metrics.split(" ")[-1]))

        # We expect 2 requests total, both should be 404 errors from the deployment
        if do_assert:
            assert (
                http_error_count == 2
            ), f"Expected at least 2 HTTP errors, got {http_error_count}"
            assert (
                deployment_404_count == 2
            ), f"Expected 2 deployment 404 errors, got {deployment_404_count}"

        return http_error_count >= 2 and deployment_404_count == 2

    # There is a latency in updating the counter
    try:
        wait_for_condition(verify_error_count, retry_interval_ms=1000, timeout=20)
    except RuntimeError:
        verify_error_count(do_assert=True)


def test_proxy_metrics_internal_error(metrics_start_shutdown):
    # NOTE: When using HAProxy, we need the replica to stay alive to emit metrics.
    # Instead of crashing the actor (which prevents metric emission), we return
    # a 500 error explicitly.
    # These metrics should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
    expected_metrics = [
        "ray_serve_num_http_requests",
        "ray_serve_num_http_error_requests_total",
        "ray_serve_num_deployment_http_error_requests",
        "ray_serve_http_request_latency_ms",
    ]

    def verify_metrics(_expected_metrics, do_assert=False):
        try:
            resp = httpx.get("http://127.0.0.1:9999", timeout=None).text
        # Requests will fail if we are crashing the controller
        except httpx.HTTPError:
            return False
        for metric in _expected_metrics:
            if do_assert:
                assert metric in resp
            if metric not in resp:
                return False
        return True

    @serve.deployment(name="A")
    class A:
        async def __init__(self):
            pass

        async def __call__(self, request: Request):
            # Return 500 Internal Server Error
            return PlainTextResponse("Internal Server Error", status_code=500)

    app_name = "app"
    serve.run(A.bind(), name=app_name, route_prefix="/")

    httpx.get("http://localhost:8000/", timeout=None)
    httpx.get("http://localhost:8000/", timeout=None)

    # Ensure all expected metrics are present.
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
        resp = httpx.get("http://127.0.0.1:9999", timeout=None).text
        resp = resp.split("\n")
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "ray_serve_num_http_error_requests_total" in metrics:
                # route "/" should have error count 2 (HTTP 500)
                if do_assert:
                    assert "2.0" in metrics
                if "2.0" not in metrics:
                    return False
            elif "ray_serve_num_deployment_http_error_requests" in metrics:
                # deployment A should have error count 2 (HTTP 500)
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


def test_proxy_metrics_fields_not_found(metrics_start_shutdown):
    """Tests the proxy metrics' fields' behavior for not found.

    Note: When using HAProxy, we need to deploy an application that returns 404,
    as HAProxy handles non-existent route 404s itself without forwarding to replicas.
    """
    # These metrics should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
    expected_metrics = [
        "ray_serve_num_http_requests",
        "ray_serve_num_http_error_requests_total",
        "ray_serve_num_deployment_http_error_requests",
        "ray_serve_http_request_latency_ms",
    ]

    app = FastAPI()

    @serve.deployment(name="test_app")
    @serve.ingress(app)
    class NotFoundApp:
        @app.get("/existing-path")  # Only this path is defined
        async def handler(self, request: Request):
            return {"message": "success"}

    app_name = "app"
    serve.run(NotFoundApp.bind(), name=app_name, route_prefix="/test")

    def verify_metrics(_expected_metrics, do_assert=False):
        try:
            resp = httpx.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except httpx.HTTPError:
            return False
        for metric in _expected_metrics:
            if do_assert:
                assert metric in resp
            if metric not in resp:
                return False
        return True

    # Trigger HTTP 404 error via the deployed application
    httpx.get("http://127.0.0.1:8000/test/nonexistent")
    httpx.get("http://127.0.0.1:8000/test/nonexistent")

    # Ensure all expected metrics are present.
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
        resp = httpx.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        http_error_count = 0
        deployment_404_count = 0

        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            # Skip health check metrics
            if "/-/healthz" in metrics:
                continue
            if (
                "ray_serve_num_http_error_requests_total" in metrics
                and 'route="/test"' in metrics
            ):
                # Accumulate error counts from route "/test"
                http_error_count += int(float(metrics.split(" ")[-1]))
            elif (
                "ray_serve_num_deployment_http_error_requests_total" in metrics
                and 'route="/test"' in metrics
                and 'error_code="404"' in metrics
            ):
                # Count deployment 404 errors
                deployment_404_count += int(float(metrics.split(" ")[-1]))

        # We expect 2 requests total, both should be 404 errors from the deployment
        if do_assert:
            assert (
                http_error_count == 2
            ), f"Expected at least 2 HTTP errors, got {http_error_count}"
            assert (
                deployment_404_count == 2
            ), f"Expected 2 deployment 404 errors, got {deployment_404_count}"

        return http_error_count >= 2 and deployment_404_count == 2

    # There is a latency in updating the counter
    try:
        wait_for_condition(verify_error_count, retry_interval_ms=1000, timeout=20)
    except RuntimeError:
        verify_error_count(do_assert=True)


@pytest.mark.parametrize(
    "metrics_start_shutdown",
    [
        1,
    ],
    indirect=True,
)
def test_proxy_timeout_metrics(metrics_start_shutdown):
    """Test that HTTP timeout metrics are reported correctly."""
    signal = SignalActor.remote()

    @serve.deployment
    async def return_status_code_with_timeout(request: Request):
        await signal.wait.remote()
        return

    serve.run(
        return_status_code_with_timeout.bind(),
        route_prefix="/status_code_timeout",
        name="status_code_timeout",
    )

    http_url = get_application_url("HTTP", app_name="status_code_timeout")

    r = httpx.get(http_url)
    assert r.status_code == 408
    ray.get(signal.send.remote(clear=True))

    num_errors = get_metric_dictionaries("ray_serve_num_http_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/status_code_timeout"
    assert num_errors[0]["error_code"] == "408"
    assert num_errors[0]["method"] == "GET"
    assert num_errors[0]["application"] == "status_code_timeout"


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows")
def test_proxy_disconnect_http_metrics(metrics_start_shutdown):
    """Test that HTTP disconnect metrics are reported correctly."""

    signal = SignalActor.remote()

    @serve.deployment
    class Disconnect:
        async def __call__(self, request: Request):
            await signal.wait.remote()
            return

    serve.run(
        Disconnect.bind(),
        route_prefix="/disconnect",
        name="disconnect",
    )

    # Simulate an HTTP disconnect
    http_url = get_application_url("HTTP", app_name="disconnect")
    ip_port = http_url.replace("http://", "").split("/")[0]  # remove the route prefix
    ip, port = parse_address(ip_port)
    conn = http.client.HTTPConnection(ip, int(port))
    conn.request("GET", "/disconnect")
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
    )
    conn.close()  # Forcefully close the connection
    ray.get(signal.send.remote(clear=True))

    num_errors = get_metric_dictionaries("ray_serve_num_http_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/disconnect"
    assert num_errors[0]["error_code"] == "499"
    assert num_errors[0]["method"] == "GET"
    assert num_errors[0]["application"] == "disconnect"


def test_proxy_metrics_fields_internal_error(metrics_start_shutdown):
    """Tests the proxy metrics' fields' behavior for internal error."""

    @serve.deployment()
    def f(*args):
        return 1 / 0

    real_app_name = "app"
    real_app_name2 = "app2"
    serve.run(f.bind(), name=real_app_name, route_prefix="/real_route")
    serve.run(f.bind(), name=real_app_name2, route_prefix="/real_route2")

    # Deployment should generate divide-by-zero errors
    correct_url = get_application_url("HTTP", real_app_name)
    _ = httpx.get(correct_url).text
    print("Sent requests to correct URL.")

    num_deployment_errors = get_metric_dictionaries(
        "ray_serve_num_deployment_http_error_requests_total"
    )
    assert len(num_deployment_errors) == 1
    assert num_deployment_errors[0]["deployment"] == "f"
    assert num_deployment_errors[0]["error_code"] == "500"
    assert num_deployment_errors[0]["method"] == "GET"
    assert num_deployment_errors[0]["application"] == "app"
    print("ray_serve_num_deployment_http_error_requests working as expected.")

    latency_metrics = get_metric_dictionaries("ray_serve_http_request_latency_ms_sum")
    # Filter out health check metrics - HAProxy generates health checks to /-/healthz
    latency_metrics = [m for m in latency_metrics if m["route"] != "/-/healthz"]
    assert len(latency_metrics) == 1
    assert latency_metrics[0]["method"] == "GET"
    assert latency_metrics[0]["route"] == "/real_route"
    assert latency_metrics[0]["application"] == "app"
    assert latency_metrics[0]["status_code"] == "500"
    print("ray_serve_http_request_latency_ms working as expected.")


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows")
def test_proxy_metrics_http_status_code_is_error(metrics_start_shutdown):
    """Verify that 2xx and 3xx status codes aren't errors, others are."""

    def check_request_count_metrics(
        expected_error_count: int,
        expected_success_count: int,
    ):
        resp = httpx.get("http://127.0.0.1:9999").text
        error_count = 0
        success_count = 0
        for line in resp.split("\n"):
            # Skip health check metrics
            if "/-/healthz" in line:
                continue
            if line.startswith("ray_serve_num_http_error_requests_total"):
                error_count += int(float(line.split(" ")[-1]))
            if line.startswith("ray_serve_num_http_requests_total"):
                success_count += int(float(line.split(" ")[-1]))

        assert error_count == expected_error_count
        assert success_count == expected_success_count
        return True

    @serve.deployment
    async def return_status_code(request: Request):
        code = int((await request.body()).decode("utf-8"))
        return PlainTextResponse("", status_code=code)

    serve.run(return_status_code.bind())

    http_url = get_application_url("HTTP")

    # 200 is not an error.
    r = httpx.request("GET", http_url, content=b"200")
    assert r.status_code == 200
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=1,
    )

    # 2xx is not an error.
    r = httpx.request("GET", http_url, content=b"250")
    assert r.status_code == 250
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=2,
    )

    # 3xx is not an error.
    r = httpx.request("GET", http_url, content=b"300")
    assert r.status_code == 300
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=3,
    )

    # 4xx is an error.
    r = httpx.request("GET", http_url, content=b"400")
    assert r.status_code == 400
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=1,
        expected_success_count=4,
    )

    # 5xx is an error.
    r = httpx.request("GET", http_url, content=b"500")
    assert r.status_code == 500
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=2,
        expected_success_count=5,
    )


def test_replica_metrics_fields(metrics_start_shutdown):
    """Test replica metrics fields"""

    @serve.deployment
    def f():
        return "hello"

    @serve.deployment
    def g():
        return "world"

    serve.run(f.bind(), name="app1", route_prefix="/f")
    serve.run(g.bind(), name="app2", route_prefix="/g")
    url_f = get_application_url("HTTP", "app1")
    url_g = get_application_url("HTTP", "app2")

    assert "hello" == httpx.post(url_f).text
    assert "world" == httpx.post(url_g).text

    wait_for_condition(
        lambda: len(
            get_metric_dictionaries("ray_serve_deployment_request_counter_total")
        )
        == 2,
        timeout=40,
    )

    metrics = get_metric_dictionaries("ray_serve_deployment_request_counter_total")
    assert len(metrics) == 2
    expected_output = {
        ("/f", "f", "app1"),
        ("/g", "g", "app2"),
    }
    assert {
        (
            metric["route"],
            metric["deployment"],
            metric["application"],
        )
        for metric in metrics
    } == expected_output

    start_metrics = get_metric_dictionaries("ray_serve_deployment_replica_starts_total")
    assert len(start_metrics) == 2
    expected_output = {("f", "app1"), ("g", "app2")}
    assert {
        (start_metric["deployment"], start_metric["application"])
        for start_metric in start_metrics
    } == expected_output

    # Latency metrics
    wait_for_condition(
        lambda: len(
            get_metric_dictionaries("ray_serve_deployment_processing_latency_ms_count")
        )
        == 2,
        timeout=40,
    )
    for metric_name in [
        "ray_serve_deployment_processing_latency_ms_count",
        "ray_serve_deployment_processing_latency_ms_sum",
    ]:
        latency_metrics = get_metric_dictionaries(metric_name)
        print(f"checking metric {metric_name}, {latency_metrics}")
        assert len(latency_metrics) == 2
        expected_output = {("f", "app1"), ("g", "app2")}
        assert {
            (latency_metric["deployment"], latency_metric["application"])
            for latency_metric in latency_metrics
        } == expected_output

    wait_for_condition(
        lambda: len(get_metric_dictionaries("ray_serve_replica_processing_queries"))
        == 2
    )
    processing_queries = get_metric_dictionaries("ray_serve_replica_processing_queries")
    expected_output = {("f", "app1"), ("g", "app2")}
    assert {
        (processing_query["deployment"], processing_query["application"])
        for processing_query in processing_queries
    } == expected_output

    @serve.deployment
    def h():
        return 1 / 0

    serve.run(h.bind(), name="app3", route_prefix="/h")
    url_h = get_application_url("HTTP", "app3")
    assert 500 == httpx.get(url_h).status_code
    wait_for_condition(
        lambda: len(get_metric_dictionaries("ray_serve_deployment_error_counter_total"))
        == 1,
        timeout=40,
    )
    err_requests = get_metric_dictionaries("ray_serve_deployment_error_counter_total")
    assert len(err_requests) == 1
    expected_output = ("/h", "h", "app3")
    assert (
        err_requests[0]["route"],
        err_requests[0]["deployment"],
        err_requests[0]["application"],
    ) == expected_output

    wait_for_condition(
        lambda: len(get_metric_dictionaries("ray_serve_deployment_replica_healthy"))
        == 3,
        timeout=40,
    )
    health_metrics = get_metric_dictionaries("ray_serve_deployment_replica_healthy")
    expected_output = {
        ("f", "app1"),
        ("g", "app2"),
        ("h", "app3"),
    }
    assert {
        (health_metric["deployment"], health_metric["application"])
        for health_metric in health_metrics
    } == expected_output


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows")
def test_multiplexed_metrics(metrics_start_shutdown):
    """Tests multiplexed API corresponding metrics."""

    @serve.deployment
    class Model:
        @serve.multiplexed(max_num_models_per_replica=2)
        async def get_model(self, model_id: str):
            return model_id

        async def __call__(self, model_id: str):
            await self.get_model(model_id)
            return

    handle = serve.run(Model.bind(), name="app", route_prefix="/app")
    handle.remote("model1")
    handle.remote("model2")
    # Trigger model eviction.
    handle.remote("model3")
    expected_metrics = [
        "ray_serve_multiplexed_model_load_latency_ms",
        "ray_serve_multiplexed_model_unload_latency_ms",
        "ray_serve_num_multiplexed_models",
        "ray_serve_multiplexed_models_load_counter",
        "ray_serve_multiplexed_models_unload_counter",
    ]

    def verify_metrics():
        try:
            resp = httpx.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except httpx.HTTPError:
            return False
        for metric in expected_metrics:
            assert metric in resp
        return True

    wait_for_condition(
        verify_metrics,
        timeout=40,
        retry_interval_ms=1000,
    )


def test_long_poll_host_sends_counted(serve_instance):
    """Check that the transmissions by the long_poll are counted."""

    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.01, 0.01)
    )

    # Write a value.
    ray.get(host.notify_changed.remote({"key_1": 999}))
    object_ref = host.listen_for_change.remote({"key_1": -1})

    # Check that the result's size is reported.
    result_1: Dict[str, UpdatedObject] = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "key_1"},
    )

    # Write two new values.
    ray.get(host.notify_changed.remote({"key_1": 1000}))
    ray.get(host.notify_changed.remote({"key_2": 1000}))
    object_ref = host.listen_for_change.remote(
        {"key_1": result_1["key_1"].snapshot_id, "key_2": -1}
    )

    # Check that the new objects are transmitted.
    result_2: Dict[str, UpdatedObject] = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "key_2"},
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter",
        expected=2,
        expected_tags={"namespace_or_state": "key_1"},
    )

    # Check that a timeout result is counted.
    object_ref = host.listen_for_change.remote({"key_2": result_2["key_2"].snapshot_id})
    _ = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "TIMEOUT"},
    )


def test_actor_summary(serve_instance):
    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    class_names = {actor["class_name"] for actor in actors}
    assert class_names.issuperset(
        {"ServeController", "HAProxyManager", "ServeReplica:app:f"}
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
