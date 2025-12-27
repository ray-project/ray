import asyncio
import concurrent.futures
import http
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import grpc
import httpx
import pytest
from fastapi import FastAPI, WebSocket
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

import ray
from ray import serve
from ray._common.network_utils import parse_address
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import (
    PrometheusTimeseries,
    fetch_prometheus_metric_timeseries,
)
from ray.serve._private.constants import (
    RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP,
    RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD,
)
from ray.serve._private.long_poll import LongPollClient, LongPollHost, UpdatedObject
from ray.serve._private.test_utils import (
    get_application_url,
    ping_grpc_call_method,
    ping_grpc_list_applications,
)
from ray.serve._private.utils import block_until_http_ready
from ray.serve.config import RequestRouterConfig
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.util.state import list_actors


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


def get_metric_float(
    metric: str,
    expected_tags: Optional[Dict[str, str]],
    timeseries: Optional[PrometheusTimeseries] = None,
) -> float:
    """Gets the float value of metric.

    If tags is specified, searched for metric with matching tags.

    Returns -1 if the metric isn't available.
    """
    if timeseries is None:
        timeseries = PrometheusTimeseries()
    samples = fetch_prometheus_metric_timeseries(["localhost:9999"], timeseries).get(
        metric, []
    )
    for sample in samples:
        if expected_tags.items() <= sample.labels.items():
            return sample.value
    return -1


def check_metric_float_eq(
    metric: str,
    expected: float,
    expected_tags: Optional[Dict[str, str]],
    timeseries: Optional[PrometheusTimeseries] = None,
) -> bool:
    metric_value = get_metric_float(metric, expected_tags, timeseries)
    assert float(metric_value) == expected
    return True


def check_sum_metric_eq(
    metric_name: str,
    expected: float,
    tags: Optional[Dict[str, str]] = None,
    timeseries: Optional[PrometheusTimeseries] = None,
) -> bool:
    if tags is None:
        tags = {}
    if timeseries is None:
        timeseries = PrometheusTimeseries()

    metrics = fetch_prometheus_metric_timeseries(["localhost:9999"], timeseries)
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


def get_metric_dictionaries(
    name: str, timeout: float = 20, timeseries: Optional[PrometheusTimeseries] = None
) -> List[Dict]:
    """Gets a list of metric's tags from metrics' text output.

    Return:
        Example:

        >>> get_metric_dictionaries("ray_serve_num_http_requests")
        [
            {
                'Component': 'core_worker',
                'JobId': '01000000',
                ...
                'method': 'GET',
                'route': '/hello'
            },
            {
                'Component': 'core_worker',
                ...
                'method': 'GET',
                'route': '/hello/'
            }
        ]
    """
    if timeseries is None:
        timeseries = PrometheusTimeseries()

    def metric_available() -> bool:
        assert name in fetch_prometheus_metric_timeseries(
            ["localhost:9999"], timeseries
        )
        return True

    wait_for_condition(metric_available, retry_interval_ms=1000, timeout=timeout)
    serve_samples = [
        sample
        for sample in timeseries.metric_samples.values()
        if "ray_serve_" in sample.name
    ]
    print(
        "metrics", "\n".join([f"Labels: {sample.labels}\n" for sample in serve_samples])
    )

    metric_dicts = []
    for sample in timeseries.metric_samples.values():
        if sample.name == name:
            metric_dicts.append(sample.labels)

    print(metric_dicts)
    return metric_dicts


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

    # Ping gPRC proxy
    grpc_url = "localhost:9000"
    channel = grpc.insecure_channel(grpc_url)
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=[app_name]
    )

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
            "serve_num_router_requests",
            "serve_num_http_requests",
            "serve_num_grpc_requests",
            "serve_deployment_queued_queries",
            "serve_deployment_request_counter",
            "serve_deployment_replica_starts",
            # histogram
            "serve_deployment_processing_latency_ms_bucket",
            "serve_deployment_processing_latency_ms_count",
            "serve_deployment_processing_latency_ms_sum",
            "serve_deployment_processing_latency_ms",
            # gauge
            "serve_replica_processing_queries",
            "serve_deployment_replica_healthy",
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
    print("serve_replica_processing_queries exists.")

    def ensure_request_processing():
        resp = httpx.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_replica_processing_queries" in metrics:
                assert "1.0" in metrics
        return True

    wait_for_condition(ensure_request_processing, timeout=5)


def test_proxy_metrics_not_found(metrics_start_shutdown):
    # NOTE: These metrics should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
    expected_metrics = [
        "serve_num_http_requests",
        "serve_num_grpc_requests",
        "serve_num_http_error_requests",
        "serve_num_grpc_error_requests",
        "serve_num_deployment_http_error_requests",
        "serve_http_request_latency_ms",
        "serve_num_deployment_grpc_error_requests",
        "serve_grpc_request_latency_ms",
    ]

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

    # Trigger HTTP 404 error
    httpx.get("http://127.0.0.1:8000/B/")
    httpx.get("http://127.0.0.1:8000/B/")

    # Ping gPRC proxy
    channel = grpc.insecure_channel("localhost:9000")
    ping_grpc_call_method(channel=channel, app_name="foo", test_not_found=True)

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
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_num_http_error_requests" in metrics:
                # route "/B/" should have error count 2
                if do_assert:
                    assert "2.0" in metrics
                if "2.0" not in metrics:
                    return False
            elif "serve_num_deployment_http_error_requests" in metrics:
                # deployment B should have error count 2
                if do_assert:
                    assert 'error_code="404"' in metrics and "2.0" in metrics
                if 'error_code="404"' not in metrics or "2.0" not in metrics:
                    return False
            elif "serve_num_grpc_error_requests" in metrics:
                # gRPC pinged "B" once
                if do_assert:
                    assert "1.0" in metrics
                if "1.0" not in metrics:
                    return False
            elif "serve_num_deployment_grpc_error_requests" in metrics:
                # gRPC pinged "B" once
                if do_assert:
                    assert (
                        'error_code="StatusCode.NOT_FOUND"' in metrics
                        and "1.0" in metrics
                    )
                if (
                    'error_code="StatusCode.NOT_FOUND"' not in metrics
                    or "1.0" not in metrics
                ):
                    return False
        return True

    # There is a latency in updating the counter
    try:
        wait_for_condition(verify_error_count, retry_interval_ms=1000, timeout=10)
    except RuntimeError:
        verify_error_count(do_assert=True)


def test_proxy_metrics_internal_error(metrics_start_shutdown):
    # NOTE: These metrics should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
    expected_metrics = [
        "serve_num_http_requests",
        "serve_num_grpc_requests",
        "serve_num_http_error_requests",
        "serve_num_grpc_error_requests",
        "serve_num_deployment_http_error_requests",
        "serve_http_request_latency_ms",
        "serve_num_deployment_grpc_error_requests",
        "serve_grpc_request_latency_ms",
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

        async def __call__(self, *args):
            # Trigger RayActorError
            os._exit(0)

    app_name = "app"
    serve.run(A.bind(), name=app_name)

    httpx.get("http://localhost:8000", timeout=None)
    httpx.get("http://localhost:8000", timeout=None)
    channel = grpc.insecure_channel("localhost:9000")
    with pytest.raises(grpc.RpcError):
        ping_grpc_call_method(channel=channel, app_name=app_name)

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
            if "serve_num_http_error_requests" in metrics:
                # route "/A/" should have error count 2
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
            elif "serve_num_grpc_error_requests" in metrics:
                # gRPC pinged "A" once
                if do_assert:
                    assert "1.0" in metrics
                if "1.0" not in metrics:
                    return False
            elif "serve_num_deployment_grpc_error_requests" in metrics:
                # gRPC pinged "A" once
                if do_assert:
                    assert 'deployment="A"' in metrics and "1.0" in metrics
                if 'deployment="A"' not in metrics or "1.0" not in metrics:
                    return False
        return True

    # There is a latency in updating the counter
    try:
        wait_for_condition(verify_error_count, retry_interval_ms=1000, timeout=10)
    except RuntimeError:
        verify_error_count(do_assert=True)


def test_proxy_metrics_fields_not_found(metrics_start_shutdown):
    """Tests the proxy metrics' fields' behavior for not found."""

    # Should generate 404 responses
    broken_url = "http://127.0.0.1:8000/fake_route"
    _ = httpx.get(broken_url).text
    print("Sent requests to broken URL.")

    # Ping gRPC proxy for not existing application.
    channel = grpc.insecure_channel("127.0.0.1:9000")
    fake_app_name = "fake-app"
    ping_grpc_call_method(channel=channel, app_name=fake_app_name, test_not_found=True)

    num_requests = get_metric_dictionaries("ray_serve_num_http_requests_total")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == ""
    assert num_requests[0]["method"] == "GET"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == "404"
    print("serve_num_http_requests working as expected.")

    num_requests = get_metric_dictionaries("ray_serve_num_grpc_requests_total")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == ""
    assert num_requests[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == str(grpc.StatusCode.NOT_FOUND)
    print("serve_num_grpc_requests working as expected.")

    num_errors = get_metric_dictionaries("ray_serve_num_http_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == ""
    assert num_errors[0]["error_code"] == "404"
    assert num_errors[0]["method"] == "GET"
    print("serve_num_http_error_requests working as expected.")

    num_errors = get_metric_dictionaries("ray_serve_num_grpc_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == ""
    assert num_errors[0]["error_code"] == str(grpc.StatusCode.NOT_FOUND)
    assert num_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    print("serve_num_grpc_error_requests working as expected.")


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

    # make grpc call
    channel = grpc.insecure_channel("localhost:9000")
    with pytest.raises(grpc.RpcError):
        ping_grpc_call_method(channel=channel, app_name="status_code_timeout")

    num_errors = get_metric_dictionaries("ray_serve_num_http_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/status_code_timeout"
    assert num_errors[0]["error_code"] == "408"
    assert num_errors[0]["method"] == "GET"
    assert num_errors[0]["application"] == "status_code_timeout"

    num_errors = get_metric_dictionaries("ray_serve_num_grpc_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "status_code_timeout"
    assert num_errors[0]["error_code"] == str(grpc.StatusCode.DEADLINE_EXCEEDED)
    assert num_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
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


def test_proxy_disconnect_grpc_metrics(metrics_start_shutdown):
    """Test that gRPC disconnect metrics are reported correctly."""

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

    # make grpc call
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
    metadata = (("application", "disconnect"),)

    def make_request():
        try:
            response = stub.__call__(
                request, metadata=metadata
            )  # Long-running RPC call
            print("Response received:", response)
        except grpc.RpcError as e:
            print("Client disconnected:", e.code(), e.details())

    thread = threading.Thread(target=make_request)
    thread.start()

    # Wait briefly, then forcefully close the channel
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
    )
    channel.close()  # Forcefully close the channel, simulating a client disconnect
    thread.join()
    ray.get(signal.send.remote(clear=True))

    num_errors = get_metric_dictionaries("ray_serve_num_grpc_error_requests_total")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "disconnect"
    assert num_errors[0]["error_code"] == str(grpc.StatusCode.CANCELLED)
    assert num_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
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

    # Ping gPRC proxy for broken app
    channel = grpc.insecure_channel("localhost:9000")
    with pytest.raises(grpc.RpcError):
        ping_grpc_call_method(channel=channel, app_name=real_app_name)

    num_deployment_errors = get_metric_dictionaries(
        "ray_serve_num_deployment_http_error_requests_total"
    )
    assert len(num_deployment_errors) == 1
    assert num_deployment_errors[0]["deployment"] == "f"
    assert num_deployment_errors[0]["error_code"] == "500"
    assert num_deployment_errors[0]["method"] == "GET"
    assert num_deployment_errors[0]["application"] == "app"
    print("serve_num_deployment_http_error_requests working as expected.")

    num_deployment_errors = get_metric_dictionaries(
        "ray_serve_num_deployment_grpc_error_requests_total"
    )
    assert len(num_deployment_errors) == 1
    assert num_deployment_errors[0]["deployment"] == "f"
    assert num_deployment_errors[0]["error_code"] == str(grpc.StatusCode.INTERNAL)
    assert (
        num_deployment_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    )
    assert num_deployment_errors[0]["application"] == real_app_name
    print("serve_num_deployment_grpc_error_requests working as expected.")

    latency_metrics = get_metric_dictionaries("ray_serve_http_request_latency_ms_sum")
    assert len(latency_metrics) == 1
    assert latency_metrics[0]["method"] == "GET"
    assert latency_metrics[0]["route"] == "/real_route"
    assert latency_metrics[0]["application"] == "app"
    assert latency_metrics[0]["status_code"] == "500"
    print("serve_http_request_latency_ms working as expected.")

    latency_metrics = get_metric_dictionaries("ray_serve_grpc_request_latency_ms_sum")
    assert len(latency_metrics) == 1
    assert latency_metrics[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    assert latency_metrics[0]["route"] == real_app_name
    assert latency_metrics[0]["application"] == real_app_name
    assert latency_metrics[0]["status_code"] == str(grpc.StatusCode.INTERNAL)
    print("serve_grpc_request_latency_ms_sum working as expected.")


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


def test_proxy_metrics_websocket_status_code_is_error(metrics_start_shutdown):
    """Verify that status codes aisde from 1000 or 1001 are errors."""

    def check_request_count_metrics(
        expected_error_count: int,
        expected_success_count: int,
    ):
        resp = httpx.get("http://127.0.0.1:9999").text
        error_count = 0
        success_count = 0
        for line in resp.split("\n"):
            if line.startswith("ray_serve_num_http_error_requests_total"):
                error_count += int(float(line.split(" ")[-1]))
            if line.startswith("ray_serve_num_http_requests_total"):
                success_count += int(float(line.split(" ")[-1]))

        assert error_count == expected_error_count
        assert success_count == expected_success_count
        return True

    fastapi_app = FastAPI()

    @serve.deployment
    @serve.ingress(fastapi_app)
    class WebSocketServer:
        @fastapi_app.websocket("/")
        async def accept_then_close(self, ws: WebSocket):
            await ws.accept()
            code = int(await ws.receive_text())
            await ws.close(code=code)

    serve.run(WebSocketServer.bind())

    # Regular disconnect (1000) is not an error.
    with connect("ws://localhost:8000/") as ws:
        with pytest.raises(ConnectionClosed):
            ws.send("1000")
            ws.recv()

    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=1,
    )

    # Goaway disconnect (1001) is not an error.
    with connect("ws://localhost:8000/") as ws:
        with pytest.raises(ConnectionClosed):
            ws.send("1001")
            ws.recv()

    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=2,
    )

    # Other codes are errors.
    with connect("ws://localhost:8000/") as ws:
        with pytest.raises(ConnectionClosed):
            ws.send("1011")
            ws.recv()

    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=1,
        expected_success_count=3,
    )

    # Other codes are errors.
    with connect("ws://localhost:8000/") as ws:
        with pytest.raises(ConnectionClosed):
            ws.send("3000")
            ws.recv()

    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=2,
        expected_success_count=4,
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

    assert "hello" == httpx.get(url_f).text
    assert "world" == httpx.get(url_g).text

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


def test_queue_wait_time_metric(metrics_start_shutdown):
    """Test that queue wait time metric is recorded correctly."""
    signal = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1)
    class SlowDeployment:
        async def __call__(self):
            await signal.wait.remote()
            return "done"

    handle = serve.run(SlowDeployment.bind(), name="app1", route_prefix="/slow")

    futures = [handle.remote() for _ in range(2)]
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
    )

    time.sleep(0.5)
    ray.get(signal.send.remote())
    [f.result() for f in futures]

    timeseries = PrometheusTimeseries()

    def check_queue_wait_time_metric():
        metrics = get_metric_dictionaries(
            "ray_serve_request_router_fulfillment_time_ms_sum", timeseries=timeseries
        )
        if not metrics:
            return False
        for metric in metrics:
            if (
                metric.get("deployment") == "SlowDeployment"
                and metric.get("application") == "app1"
            ):
                return True
        return False

    wait_for_condition(check_queue_wait_time_metric, timeout=10)

    def check_queue_wait_time_metric_value():
        value = get_metric_float(
            "ray_serve_request_router_fulfillment_time_ms_sum",
            timeseries=timeseries,
            expected_tags={"deployment": "SlowDeployment", "application": "app1"},
        )
        assert value > 400, f"Queue wait time should be greater than 500ms, got {value}"
        return True

    wait_for_condition(check_queue_wait_time_metric_value, timeout=10)

    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == 0, timeout=10
    )


def test_router_queue_len_metric(metrics_start_shutdown):
    """Test that router queue length metric is recorded correctly per replica."""
    signal = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=10)
    class TestDeployment:
        async def __call__(self, request: Request):
            await signal.wait.remote()
            return "done"

    serve.run(TestDeployment.bind(), name="app1", route_prefix="/test")

    # Send a request that will block
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(httpx.get, "http://localhost:8000/test", timeout=60)

        # Wait for request to reach the replica
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=15
        )

        timeseries = PrometheusTimeseries()

        # Check that the router queue length metric appears with correct tags
        def check_router_queue_len():
            metrics = get_metric_dictionaries(
                "ray_serve_request_router_queue_len", timeseries=timeseries
            )
            if not metrics:
                return False
            # Find metric for our deployment with replica_id tag
            for metric in metrics:
                if (
                    metric.get("deployment") == "TestDeployment"
                    and metric.get("application") == "app1"
                    and "replica_id" in metric
                ):
                    # Check that required tags are present
                    assert (
                        "handle_source" in metric
                    ), "handle_source tag should be present"
                    print(f"Found router queue len metric: {metric}")
                    return True
            return False

        wait_for_condition(check_router_queue_len, timeout=30)

        wait_for_condition(
            check_metric_float_eq,
            timeout=15,
            metric="ray_serve_request_router_queue_len",
            expected=1,
            expected_tags={"deployment": "TestDeployment", "application": "app1"},
            timeseries=timeseries,
        )
        print("Router queue len metric verified.")

        # Release request
        ray.get(signal.send.remote())
        future.result()


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
        "serve_multiplexed_model_load_latency_ms",
        "serve_multiplexed_model_unload_latency_ms",
        "serve_num_multiplexed_models",
        "serve_multiplexed_models_load_counter",
        "serve_multiplexed_models_unload_counter",
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


@pytest.mark.parametrize("use_factory_pattern", [False, True])
def test_proxy_metrics_with_route_patterns(metrics_start_shutdown, use_factory_pattern):
    """Test that proxy metrics use specific route patterns for FastAPI apps.

    This test verifies that:
    1. Route patterns are extracted from FastAPI apps at replica initialization
    2. Proxy metrics use parameterized patterns (e.g., /api/users/{user_id})
       instead of just route prefixes (e.g., /api)
    3. Individual request paths don't appear in metrics (avoiding high cardinality)
    4. Multiple requests to the same pattern are grouped together
    5. Both normal pattern and factory pattern work correctly
    """
    if use_factory_pattern:
        # Factory pattern: callable returns FastAPI app at runtime
        def create_app():
            app = FastAPI()

            @app.get("/")
            def root():
                return {"message": "root"}

            @app.get("/users/{user_id}")
            def get_user(user_id: str):
                return {"user_id": user_id}

            @app.get("/items/{item_id}/details")
            def get_item(item_id: str):
                return {"item_id": item_id}

            return app

        @serve.deployment
        @serve.ingress(create_app)
        class APIServer:
            pass

    else:
        # Normal pattern: routes defined in deployment class
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class APIServer:
            @app.get("/")
            def root(self):
                return {"message": "root"}

            @app.get("/users/{user_id}")
            def get_user(self, user_id: str):
                return {"user_id": user_id}

            @app.get("/items/{item_id}/details")
            def get_item(self, item_id: str):
                return {"item_id": item_id}

    serve.run(APIServer.bind(), name="api_app", route_prefix="/api")

    # Make requests to different route patterns with various parameter values
    base_url = "http://localhost:8000/api"
    assert httpx.get(f"{base_url}/").status_code == 200
    assert httpx.get(f"{base_url}/users/123").status_code == 200
    assert httpx.get(f"{base_url}/users/456").status_code == 200
    assert httpx.get(f"{base_url}/users/789").status_code == 200
    assert httpx.get(f"{base_url}/items/abc/details").status_code == 200
    assert httpx.get(f"{base_url}/items/xyz/details").status_code == 200

    # Wait for metrics to be updated
    def metrics_available():
        metrics = get_metric_dictionaries("ray_serve_num_http_requests_total")
        api_metrics = [m for m in metrics if m.get("application") == "api_app"]
        return len(api_metrics) >= 3

    wait_for_condition(metrics_available, timeout=20)

    # Verify metrics use route patterns, not individual paths
    metrics = get_metric_dictionaries("ray_serve_num_http_requests_total")
    api_metrics = [m for m in metrics if m.get("application") == "api_app"]

    routes = {m["route"] for m in api_metrics}

    print(f"Routes found in metrics: {routes}")

    # Should contain the route patterns (parameterized), not just the prefix
    # The root might be either "/api/" or "/api" depending on normalization
    assert any(
        r in routes for r in ["/api/", "/api"]
    ), f"Root route not found. Routes: {routes}"

    # Should contain parameterized user route
    assert (
        "/api/users/{user_id}" in routes
    ), f"User route pattern not found. Routes: {routes}"

    # Should contain nested parameterized route
    assert (
        "/api/items/{item_id}/details" in routes
    ), f"Item details route pattern not found. Routes: {routes}"

    # Should NOT contain individual request paths (that would be high cardinality)
    # These should not appear as they would create unbounded cardinality
    assert (
        "/api/users/123" not in routes
    ), "Individual user path found - high cardinality issue!"
    assert (
        "/api/users/456" not in routes
    ), "Individual user path found - high cardinality issue!"
    assert (
        "/api/users/789" not in routes
    ), "Individual user path found - high cardinality issue!"
    assert (
        "/api/items/abc/details" not in routes
    ), "Individual item path found - high cardinality issue!"
    assert (
        "/api/items/xyz/details" not in routes
    ), "Individual item path found - high cardinality issue!"

    # Verify that multiple requests to the same pattern are grouped
    user_route_metrics = [
        m for m in api_metrics if m["route"] == "/api/users/{user_id}"
    ]
    assert (
        len(user_route_metrics) == 1
    ), "Multiple metrics entries for same route pattern - should be grouped!"

    # Optionally verify the counter value if we can parse it from the metrics endpoint
    metrics_text = httpx.get("http://127.0.0.1:9999").text
    for line in metrics_text.split("\n"):
        if "serve_num_http_requests" in line and "/api/users/{user_id}" in line:
            # Extract the value from the prometheus format line
            value_str = line.split()[-1]
            user_metric_value = float(value_str)
            assert (
                user_metric_value == 3
            ), f"Expected exactly 3 requests to user route, got {user_metric_value}"
            break

    # Verify error metrics also use route patterns
    num_errors = get_metric_dictionaries("ray_serve_http_request_latency_ms_sum")
    api_latency_metrics = [m for m in num_errors if m.get("application") == "api_app"]
    latency_routes = {m["route"] for m in api_latency_metrics}

    # Latency metrics should also use patterns
    assert (
        "/api/users/{user_id}" in latency_routes or "/api/" in latency_routes
    ), f"Latency metrics should use route patterns. Found: {latency_routes}"


def test_routing_stats_delay_metric(metrics_start_shutdown):
    """Test that routing stats delay metric is reported correctly."""

    @serve.deployment
    class Model:
        def __call__(self):
            return "hello"

    serve.run(Model.bind(), name="app")
    timeseries = PrometheusTimeseries()

    # Wait for routing stats delay metric to be reported
    # This metric is recorded when the controller polls routing stats from replicas
    def check_routing_stats_delay_metric():
        metrics = get_metric_dictionaries(
            "ray_serve_routing_stats_delay_ms_count", timeseries=timeseries
        )
        if not metrics:
            return False
        # Check that at least one metric has expected tags
        for metric in metrics:
            assert metric["deployment"] == "Model"
            assert metric["application"] == "app"
            assert "replica" in metric
            return True
        return False

    wait_for_condition(check_routing_stats_delay_metric, timeout=60)

    # Verify the metric value is greater than 0
    def check_routing_stats_delay_metric_value():
        value = get_metric_float(
            "ray_serve_routing_stats_delay_ms_count",
            timeseries=timeseries,
            expected_tags={
                "deployment": "Model",
                "application": "app",
            },
        )
        return value > 0

    wait_for_condition(check_routing_stats_delay_metric_value, timeout=60)


def test_routing_stats_error_metric(metrics_start_shutdown):
    """Test that routing stats error metric is reported on exception and timeout."""
    signal = SignalActor.remote()

    @serve.deployment(
        request_router_config=RequestRouterConfig(
            request_routing_stats_period_s=0.1, request_routing_stats_timeout_s=0.5
        )
    )
    class FailingModel:
        def __init__(self, signal_actor):
            self.should_fail = False
            self.should_hang = False
            self.signal = signal_actor

        async def record_routing_stats(self):
            if self.should_hang:
                await self.signal.wait.remote()
            if self.should_fail:
                raise Exception("Intentional failure for testing")
            return {}

        def __call__(self):
            return "hello"

        def set_should_fail(self, value: bool):
            self.should_fail = value

        def set_should_hang(self, value: bool):
            self.should_hang = value

    handle = serve.run(FailingModel.bind(signal), name="error_app")
    timeseries = PrometheusTimeseries()

    # Make a request to ensure deployment is running
    handle.remote().result()

    # Trigger exception in record_routing_stats
    handle.set_should_fail.remote(True).result()

    # Make requests to trigger routing stats collection
    for _ in range(5):
        handle.remote().result()

    # Check that error metric with error_type="exception" is reported
    def check_exception_error_metric():
        metrics = get_metric_dictionaries(
            "ray_serve_routing_stats_error_total", timeseries=timeseries
        )
        for metric in metrics:
            if (
                metric.get("deployment") == "FailingModel"
                and metric.get("application") == "error_app"
                and metric.get("error_type") == "exception"
            ):
                assert "replica" in metric
                return True
        return False

    wait_for_condition(check_exception_error_metric, timeout=30)
    print("Exception error metric verified.")

    # Now test timeout case
    handle.set_should_fail.remote(False).result()
    handle.set_should_hang.remote(True).result()

    # Make requests to trigger routing stats timeout
    for _ in range(5):
        handle.remote().result()

    # Check that error metric with error_type="timeout" is reported
    def check_timeout_error_metric():
        metrics = get_metric_dictionaries(
            "ray_serve_routing_stats_error_total", timeseries=timeseries
        )
        for metric in metrics:
            if (
                metric.get("deployment") == "FailingModel"
                and metric.get("application") == "error_app"
                and metric.get("error_type") == "timeout"
            ):
                assert "replica" in metric
                return True
        return False

    wait_for_condition(check_timeout_error_metric, timeout=30)
    print("Timeout error metric verified.")

    ray.get(signal.send.remote(clear=True))


def test_deployment_and_application_status_metrics(metrics_start_shutdown):
    """Test that deployment and application status metrics are exported correctly.

    These metrics track the numeric status of deployments and applications:
    - serve_deployment_status: 0=UNKNOWN, 1=DEPLOY_FAILED, 2=UNHEALTHY,
      3=UPDATING, 4=UPSCALING, 5=DOWNSCALING, 6=HEALTHY
    - serve_application_status: 0=UNKNOWN, 1=NOT_STARTED, 2=DEPLOYING,
      3=DEPLOY_FAILED, 4=RUNNING, 5=UNHEALTHY, 6=DELETING
    """

    signal = SignalActor.remote()

    @serve.deployment(name="deployment_a")
    class DeploymentA:
        async def __init__(self):
            await signal.wait.remote()

        async def __call__(self):
            return "hello"

    @serve.deployment
    def deployment_b():
        return "world"

    # Deploy two applications with different deployments
    serve._run(DeploymentA.bind(), name="app1", route_prefix="/app1", _blocking=False)
    serve._run(deployment_b.bind(), name="app2", route_prefix="/app2", _blocking=False)

    timeseries = PrometheusTimeseries()

    # Wait for deployments to become healthy
    def check_status_metrics():
        # Check deployment status metrics
        deployment_metrics = get_metric_dictionaries(
            "ray_serve_deployment_status", timeseries=timeseries
        )
        if len(deployment_metrics) < 2:
            return False

        # Check application status metrics
        app_metrics = get_metric_dictionaries(
            "ray_serve_application_status", timeseries=timeseries
        )
        if len(app_metrics) < 2:
            return False

        return True

    wait_for_condition(check_status_metrics, timeout=30)

    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_deployment_status",
        expected=3,  # UPDATING
        expected_tags={"deployment": "deployment_a", "application": "app1"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_application_status",
        expected=5,  # DEPLOYING
        expected_tags={"application": "app1"},
        timeseries=timeseries,
    )

    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_deployment_status",
        expected=6,
        expected_tags={"deployment": "deployment_b", "application": "app2"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_application_status",
        expected=6,
        expected_tags={"application": "app2"},
        timeseries=timeseries,
    )

    ray.get(signal.send.remote())

    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_deployment_status",
        expected=6,
        expected_tags={"deployment": "deployment_a", "application": "app1"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_application_status",
        expected=6,
        expected_tags={"application": "app1"},
        timeseries=timeseries,
    )


def test_replica_startup_and_initialization_latency_metrics(metrics_start_shutdown):
    """Test that replica startup and initialization latency metrics are recorded."""

    @serve.deployment(num_replicas=2)
    class MyDeployment:
        def __init__(self):
            time.sleep(1)

        def __call__(self):
            return "hello"

    serve.run(MyDeployment.bind(), name="app", route_prefix="/f")
    url = get_application_url("HTTP", "app")
    assert "hello" == httpx.get(url).text

    # Verify startup latency metric count is exactly 1 (one replica started)
    wait_for_condition(
        check_metric_float_eq,
        timeout=20,
        metric="ray_serve_replica_startup_latency_ms_count",
        expected=1,
        expected_tags={"deployment": "MyDeployment", "application": "app"},
    )

    # Verify initialization latency metric count is exactly 1
    wait_for_condition(
        check_metric_float_eq,
        timeout=20,
        metric="ray_serve_replica_initialization_latency_ms_count",
        expected=1,
        expected_tags={"deployment": "MyDeployment", "application": "app"},
    )

    # Verify initialization latency metric value is greater than 500ms
    def check_initialization_latency_value():
        value = get_metric_float(
            "ray_serve_replica_initialization_latency_ms_sum",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        assert (
            value > 500
        ), f"Initialization latency value is {value}, expected to be greater than 500ms"
        return True

    wait_for_condition(check_initialization_latency_value, timeout=20)

    # Assert that 2 metrics are recorded (one per replica)
    def check_metrics_count():
        metrics = get_metric_dictionaries(
            "ray_serve_replica_initialization_latency_ms_count"
        )
        assert len(metrics) == 2, f"Expected 2 metrics, got {len(metrics)}"
        # All metrics should have same deployment and application
        for metric in metrics:
            assert metric["deployment"] == "MyDeployment"
            assert metric["application"] == "app"
        # Each replica should have a unique replica tag
        replica_ids = {metric["replica"] for metric in metrics}
        assert (
            len(replica_ids) == 2
        ), f"Expected 2 unique replica IDs, got {replica_ids}"
        return True

    wait_for_condition(check_metrics_count, timeout=20)


def test_replica_reconfigure_latency_metrics(metrics_start_shutdown):
    """Test that replica reconfigure latency metrics are recorded when user_config changes."""

    @serve.deployment(version="1")
    class Configurable:
        def __init__(self):
            self.config = None

        def reconfigure(self, config):
            time.sleep(1)
            self.config = config

        def __call__(self):
            return self.config

    # Initial deployment with version specified to enable in-place reconfigure
    serve.run(
        Configurable.options(user_config={"version": 1}).bind(),
        name="app",
        route_prefix="/config",
    )
    url = get_application_url("HTTP", "app")
    assert httpx.get(url).json() == {"version": 1}

    # Update user_config to trigger in-place reconfigure (same version, different config)
    serve.run(
        Configurable.options(user_config={"version": 2}).bind(),
        name="app",
        route_prefix="/config",
    )

    # Wait for the new config to take effect
    def config_updated():
        return httpx.get(url).json() == {"version": 2}

    wait_for_condition(config_updated, timeout=20)

    # Verify reconfigure latency metric count is exactly 1 (one reconfigure happened)
    wait_for_condition(
        check_metric_float_eq,
        timeout=20,
        metric="ray_serve_replica_reconfigure_latency_ms_count",
        expected=1,
        expected_tags={"deployment": "Configurable", "application": "app"},
    )

    # Verify reconfigure latency metric value is greater than 500ms (we slept for 1s)
    def check_reconfigure_latency_value():
        value = get_metric_float(
            "ray_serve_replica_reconfigure_latency_ms_sum",
            expected_tags={"deployment": "Configurable", "application": "app"},
        )
        assert value > 500, f"Reconfigure latency value is {value}, expected > 500ms"
        return True

    wait_for_condition(check_reconfigure_latency_value, timeout=20)


def test_health_check_latency_metrics(metrics_start_shutdown):
    """Test that health check latency metrics are recorded."""

    @serve.deployment(health_check_period_s=1)
    class MyDeployment:
        def __call__(self):
            return "hello"

        def check_health(self):
            time.sleep(1)

    serve.run(MyDeployment.bind(), name="app", route_prefix="/f")
    url = get_application_url("HTTP", "app")
    assert "hello" == httpx.get(url).text

    # Wait for at least one health check to complete and verify metric is recorded
    def check_health_check_latency_metrics():
        value = get_metric_float(
            "ray_serve_health_check_latency_ms_count",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        # Health check count should be at least 1
        assert value >= 1, f"Health check count is {value}, expected to be 1"
        return True

    wait_for_condition(check_health_check_latency_metrics, timeout=30)

    # Verify health check latency metric value is greater than 500ms
    def check_health_check_latency_value():
        value = get_metric_float(
            "ray_serve_health_check_latency_ms_sum",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        assert (
            value > 500
        ), f"Health check latency value is {value}, expected to be greater than 500ms"
        return True

    wait_for_condition(check_health_check_latency_value, timeout=30)


def test_health_check_failures_metrics(metrics_start_shutdown):
    """Test that health check failure metrics are recorded when health checks fail."""

    @serve.deployment(health_check_period_s=1, health_check_timeout_s=2)
    class FailingHealthCheck:
        def __init__(self):
            self.should_fail = False

        async def check_health(self):
            if self.should_fail:
                raise Exception("Health check failed!")

        async def __call__(self, request):
            action = (await request.body()).decode("utf-8")
            if action == "fail":
                self.should_fail = True
            return "ok"

    serve.run(FailingHealthCheck.bind(), name="app", route_prefix="/health")
    url = get_application_url("HTTP", "app")

    # Verify deployment is healthy initially
    assert httpx.get(url).text == "ok"

    # Trigger health check failure
    httpx.request("GET", url, content=b"fail")

    # Wait for at least one health check failure to be recorded
    def check_health_check_failure_metrics():
        value = get_metric_float(
            "ray_serve_health_check_failures_total",
            expected_tags={"deployment": "FailingHealthCheck", "application": "app"},
        )
        # Should have at least 1 failure
        return value >= 1

    wait_for_condition(check_health_check_failure_metrics, timeout=30)


def test_replica_shutdown_duration_metrics(metrics_start_shutdown):
    """Test that replica shutdown duration metrics are recorded."""

    @serve.deployment
    class MyDeployment:
        def __call__(self):
            return "hello"

        def __del__(self):
            time.sleep(1)

    # Deploy the application
    serve.run(MyDeployment.bind(), name="app", route_prefix="/f")
    url = get_application_url("HTTP", "app")
    assert "hello" == httpx.get(url).text

    # Delete the application to trigger shutdown
    serve.delete("app", _blocking=True)

    # Verify shutdown duration metric count is exactly 1 (one replica stopped)
    wait_for_condition(
        check_metric_float_eq,
        timeout=30,
        metric="ray_serve_replica_shutdown_duration_ms_count",
        expected=1,
        expected_tags={"deployment": "MyDeployment", "application": "app"},
    )
    print("serve_replica_shutdown_duration_ms working as expected.")

    # Verify shutdown duration metric value is greater than 500ms
    def check_shutdown_duration_value():
        value = get_metric_float(
            "ray_serve_replica_shutdown_duration_ms_sum",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        assert (
            value > 500
        ), f"Shutdown duration value is {value}, expected to be greater than 500ms"
        return True

    wait_for_condition(check_shutdown_duration_value, timeout=30)


def test_batching_metrics(metrics_start_shutdown):
    @serve.deployment
    class BatchedDeployment:
        @serve.batch(max_batch_size=4, batch_wait_timeout_s=0.5)
        async def batch_handler(self, requests: List[str]) -> List[str]:
            # Simulate some processing time
            await asyncio.sleep(0.05)
            return [f"processed:{r}" for r in requests]

        async def __call__(self, request: Request):
            data = await request.body()
            return await self.batch_handler(data.decode())

    app_name = "batched_app"
    serve.run(BatchedDeployment.bind(), name=app_name, route_prefix="/batch")

    http_url = "http://localhost:8000/batch"

    # Send multiple concurrent requests to trigger batching
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(lambda i=i: httpx.post(http_url, content=f"req{i}"))
            for i in range(8)
        ]
        results = [f.result() for f in futures]

    # Verify all requests succeeded
    assert all(r.status_code == 200 for r in results)

    # Verify specific metric values and tags
    timeseries = PrometheusTimeseries()
    expected_tags = {
        "deployment": "BatchedDeployment",
        "application": app_name,
        "function_name": "batch_handler",
    }

    # Check batches_processed_total counter exists and has correct tags
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batches_processed_total",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_wait_time_ms histogram was recorded for 2 batches
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_wait_time_ms_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_execution_time_ms histogram was recorded for 2 batches
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_execution_time_ms_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_utilization_percent histogram: 2 batches at 100% each = 200 sum
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_utilization_percent_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check actual_batch_size histogram: 2 batches of 4 requests each = 8 sum
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_actual_batch_size_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_queue_length gauge exists (should be 0 after processing)
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_queue_length",
            expected=0,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )


def test_autoscaling_metrics(metrics_start_shutdown):
    """Test that autoscaling metrics are emitted correctly.

    This tests the following metrics:
    - ray_serve_autoscaling_target_replicas: Target number of replicas
        Tags: deployment, application
    - ray_serve_autoscaling_desired_replicas: Raw decision before bounds
        Tags: deployment, application
    - ray_serve_autoscaling_total_requests: Total requests seen by autoscaler
        Tags: deployment, application
    - ray_serve_autoscaling_policy_execution_time_ms: Policy execution time
        Tags: deployment, application, policy_scope
    - ray_serve_autoscaling_replica_metrics_delay_ms: Replica metrics delay
        Tags: deployment, application, replica
    - ray_serve_autoscaling_handle_metrics_delay_ms: Handle metrics delay
        Tags: deployment, application, handle
    """
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 5,
            "target_ongoing_requests": 2,
            "upscale_delay_s": 0,
            "downscale_delay_s": 5,
            "look_back_period_s": 1,
        },
        max_ongoing_requests=10,
        graceful_shutdown_timeout_s=0.1,
    )
    class AutoscalingDeployment:
        async def __call__(self):
            await signal.wait.remote()

    serve.run(AutoscalingDeployment.bind(), name="autoscaling_app")

    # Send requests to trigger autoscaling
    handle = serve.get_deployment_handle("AutoscalingDeployment", "autoscaling_app")
    [handle.remote() for _ in range(10)]

    timeseries = PrometheusTimeseries()
    base_tags = {
        "deployment": "AutoscalingDeployment",
        "application": "autoscaling_app",
    }

    # Test 1: Check that target_replicas metric is 5 (10 requests / target_ongoing_requests=2)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_autoscaling_target_replicas",
        expected=5,
        expected_tags=base_tags,
        timeseries=timeseries,
    )
    print("Target replicas metric verified.")

    # Test 2: Check that autoscaling decision metric is 5 (10 requests / target_ongoing_requests=2)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_autoscaling_desired_replicas",
        expected=5,
        expected_tags=base_tags,
        timeseries=timeseries,
    )
    print("Autoscaling decision metric verified.")

    # Test 3: Check that total requests metric is 10
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_autoscaling_total_requests",
        expected=10,
        expected_tags=base_tags,
        timeseries=timeseries,
    )
    print("Total requests metric verified.")

    # Test 4: Check that policy execution time metric is emitted with policy_scope=deployment
    def check_policy_execution_time_metric():
        value = get_metric_float(
            "ray_serve_autoscaling_policy_execution_time_ms",
            expected_tags={**base_tags, "policy_scope": "deployment"},
            timeseries=timeseries,
        )
        assert value >= 0
        return True

    wait_for_condition(check_policy_execution_time_metric, timeout=15)
    print("Policy execution time metric verified.")

    # Test 5: Check that metrics delay gauges are emitted with proper tags
    def check_metrics_delay_metrics():
        # Check for handle metrics delay (depends on where metrics are collected)
        value = get_metric_float(
            "ray_serve_autoscaling_handle_metrics_delay_ms",
            expected_tags=base_tags,
            timeseries=timeseries,
        )
        if value >= 0:
            # Verify handle tag exists by checking metric dictionaries
            metrics_dicts = get_metric_dictionaries(
                "ray_serve_autoscaling_handle_metrics_delay_ms",
                timeout=5,
                timeseries=timeseries,
            )
            for m in metrics_dicts:
                if (
                    m.get("deployment") == "AutoscalingDeployment"
                    and m.get("application") == "autoscaling_app"
                ):
                    assert m.get("handle") is not None
                    print(
                        f"Handle delay metric verified with handle tag: {m.get('handle')}"
                    )
                    return True

        # Fallback: Check for replica metrics delay
        value = get_metric_float(
            "ray_serve_autoscaling_replica_metrics_delay_ms",
            expected_tags=base_tags,
            timeseries=timeseries,
        )
        if value >= 0:
            metrics_dicts = get_metric_dictionaries(
                "ray_serve_autoscaling_replica_metrics_delay_ms",
                timeout=5,
                timeseries=timeseries,
            )
            for m in metrics_dicts:
                if (
                    m.get("deployment") == "AutoscalingDeployment"
                    and m.get("application") == "autoscaling_app"
                ):
                    assert m.get("replica") is not None
                    print(
                        f"Replica delay metric verified with replica tag: {m.get('replica')}"
                    )
                    return True

        return False

    wait_for_condition(check_metrics_delay_metrics, timeout=15)
    print("Metrics delay metrics verified.")

    # Release signal to complete requests
    ray.get(signal.send.remote())


def test_user_autoscaling_stats_metrics(metrics_start_shutdown):
    """Test that user-defined autoscaling stats metrics are emitted correctly.

    This tests the following metrics:
    - ray_serve_user_autoscaling_stats_latency_ms: Time to execute user stats function
        Tags: application, deployment, replica
    - ray_serve_record_autoscaling_stats_failed_total: Failed stats collection
        Tags: application, deployment, replica, exception_name
    """

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 5,
            "target_ongoing_requests": 2,
        },
    )
    class DeploymentWithCustomStats:
        def __init__(self):
            self.call_count = 0

        async def record_autoscaling_stats(self):
            """Custom autoscaling stats function."""
            self.call_count += 1
            return {"custom_metric": self.call_count}

        def __call__(self):
            return "ok"

    serve.run(DeploymentWithCustomStats.bind(), name="custom_stats_app")

    # Make a request to ensure the deployment is running
    handle = serve.get_deployment_handle(
        "DeploymentWithCustomStats", "custom_stats_app"
    )
    handle.remote().result()

    timeseries = PrometheusTimeseries()
    base_tags = {
        "deployment": "DeploymentWithCustomStats",
        "application": "custom_stats_app",
    }

    # Test: Check that user autoscaling stats latency metric is emitted
    def check_user_stats_latency_metric():
        value = get_metric_float(
            "ray_serve_user_autoscaling_stats_latency_ms_sum",
            expected_tags=base_tags,
            timeseries=timeseries,
        )
        if value >= 0:
            # Verify replica tag exists
            metrics_dicts = get_metric_dictionaries(
                "ray_serve_user_autoscaling_stats_latency_ms_sum",
                timeout=5,
                timeseries=timeseries,
            )
            for m in metrics_dicts:
                if (
                    m.get("deployment") == "DeploymentWithCustomStats"
                    and m.get("application") == "custom_stats_app"
                ):
                    assert m.get("replica") is not None
                    print(
                        f"User stats latency metric verified with replica tag: {m.get('replica')}"
                    )
                    return True
        return False

    wait_for_condition(check_user_stats_latency_metric, timeout=15)
    print("User autoscaling stats latency metric verified.")


def test_user_autoscaling_stats_failure_metrics(metrics_start_shutdown):
    """Test that user autoscaling stats failure metrics are emitted on error."""

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 5,
            "target_ongoing_requests": 2,
        },
    )
    class DeploymentWithFailingStats:
        async def record_autoscaling_stats(self):
            """Custom autoscaling stats function that raises an error."""
            raise ValueError("Intentional error for testing")

        def __call__(self):
            return "ok"

    serve.run(DeploymentWithFailingStats.bind(), name="failing_stats_app")

    # Make a request to ensure the deployment is running
    handle = serve.get_deployment_handle(
        "DeploymentWithFailingStats", "failing_stats_app"
    )
    handle.remote().result()

    timeseries = PrometheusTimeseries()

    # Test: Check that failure counter is incremented
    def check_stats_failure_metric():
        metrics_dicts = get_metric_dictionaries(
            "ray_serve_record_autoscaling_stats_failed_total",
            timeout=5,
            timeseries=timeseries,
        )
        for m in metrics_dicts:
            if (
                m.get("deployment") == "DeploymentWithFailingStats"
                and m.get("application") == "failing_stats_app"
            ):
                assert m.get("replica") is not None
                assert m.get("exception_name") == "ValueError"
                print(
                    f"Stats failure metric verified with exception_name: {m.get('exception_name')}"
                )
                return True
        return False

    wait_for_condition(check_stats_failure_metric, timeout=15)
    print("User autoscaling stats failure metric verified.")


def test_long_poll_pending_clients_metric(metrics_start_shutdown):
    """Check that pending clients gauge is tracked correctly."""
    timeseries = PrometheusTimeseries()

    # Create a LongPollHost with a longer timeout so we can observe pending state
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(5.0, 5.0)
    )

    # Write initial values
    ray.get(host.notify_changed.remote({"key_1": 100}))
    ray.get(host.notify_changed.remote({"key_2": 200}))

    # Get the current snapshot IDs
    result = ray.get(host.listen_for_change.remote({"key_1": -1, "key_2": -1}))
    key_1_snapshot_id = result["key_1"].snapshot_id
    key_2_snapshot_id = result["key_2"].snapshot_id

    # Start a listen call that will block waiting for updates
    # (since we're using up-to-date snapshot IDs)
    pending_ref = host.listen_for_change.remote(
        {"key_1": key_1_snapshot_id, "key_2": key_2_snapshot_id}
    )

    # Check that pending clients gauge shows 1 for each key
    # (wait_for_condition will retry until the metric is available)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_pending_clients",
        expected=1,
        expected_tags={"namespace": "key_1"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_pending_clients",
        expected=1,
        expected_tags={"namespace": "key_2"},
        timeseries=timeseries,
    )

    # Trigger an update for key_1
    ray.get(host.notify_changed.remote({"key_1": 101}))

    # Wait for the pending call to complete
    ray.get(pending_ref)

    # After update, pending clients for key_1 should be 0
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_pending_clients",
        expected=0,
        expected_tags={"namespace": "key_1"},
        timeseries=timeseries,
    )


def test_long_poll_latency_metric(metrics_start_shutdown):
    """Check that long poll latency histogram is recorded on the client side."""
    timeseries = PrometheusTimeseries()

    # Create a LongPollHost
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.5, 0.5)
    )

    # Write initial value so the key exists
    ray.get(host.notify_changed.remote({"test_key": "initial_value"}))

    # Track received updates
    received_updates = []
    update_event = threading.Event()

    def on_update(value):
        received_updates.append(value)
        update_event.set()

    # Create event loop for the client
    loop = asyncio.new_event_loop()

    def run_loop():
        asyncio.set_event_loop(loop)
        loop.run_forever()

    loop_thread = threading.Thread(target=run_loop, daemon=True)
    loop_thread.start()

    # Create the LongPollClient
    client = LongPollClient(
        host_actor=host,
        key_listeners={"test_key": on_update},
        call_in_event_loop=loop,
    )

    # Wait for initial update (client starts with snapshot_id -1)
    assert update_event.wait(timeout=10), "Timed out waiting for initial update"
    assert len(received_updates) == 1
    assert received_updates[0] == "initial_value"

    # Clear event and trigger another update
    update_event.clear()
    ray.get(host.notify_changed.remote({"test_key": "updated_value"}))

    # Wait for the update to be received
    assert update_event.wait(timeout=10), "Timed out waiting for update"
    assert len(received_updates) == 2
    assert received_updates[1] == "updated_value"

    # Stop the client
    client.stop()
    loop.call_soon_threadsafe(loop.stop)
    loop_thread.join(timeout=5)

    # Check that latency metric was recorded
    # The metric should have at least 2 observations (initial + update)
    def check_latency_metric_exists():
        metric_value = get_metric_float(
            "ray_serve_long_poll_latency_ms_count",
            expected_tags={"namespace": "test_key"},
            timeseries=timeseries,
        )
        # Should have at least 2 observations
        return metric_value == 2

    wait_for_condition(check_latency_metric_exists, timeout=15)

    # Verify the latency sum is positive (latency > 0)
    latency_sum = get_metric_float(
        "ray_serve_long_poll_latency_ms_sum",
        expected_tags={"namespace": "test_key"},
        timeseries=timeseries,
    )
    assert latency_sum > 0, "Latency sum should be positive"


def test_long_poll_host_sends_counted(metrics_start_shutdown):
    """Check that the transmissions by the long_poll are counted."""

    timeseries = PrometheusTimeseries()
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
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=1,
        expected_tags={"namespace_or_state": "key_1"},
        timeseries=timeseries,
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
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=1,
        expected_tags={"namespace_or_state": "key_2"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=2,
        expected_tags={"namespace_or_state": "key_1"},
        timeseries=timeseries,
    )

    # Check that a timeout result is counted.
    object_ref = host.listen_for_change.remote({"key_2": result_2["key_2"].snapshot_id})
    _ = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=1,
        expected_tags={"namespace_or_state": "TIMEOUT"},
        timeseries=timeseries,
    )


def test_event_loop_monitoring_metrics(metrics_start_shutdown):
    """Test that event loop monitoring metrics are emitted correctly.

    This tests the following metrics:
    - serve_event_loop_scheduling_latency_ms: Event loop lag in milliseconds
        Tags: component, loop_type, actor_id
    - serve_event_loop_monitoring_iterations: Heartbeat counter
        Tags: component, loop_type, actor_id
    - serve_event_loop_tasks: Number of pending asyncio tasks
        Tags: component, loop_type, actor_id

    Components monitored:
    - Proxy: main loop only
    - Replica: main loop + user_code loop (when separate thread enabled)
    - Router: router loop (when separate loop enabled, runs on replica)
    """

    @serve.deployment(name="g")
    class ChildDeployment:
        def __call__(self):
            return "child"

    @serve.deployment(name="f")
    class SimpleDeployment:
        def __init__(self, child):
            self.child = child

        async def __call__(self):
            return await self.child.remote()

    serve.run(
        SimpleDeployment.bind(ChildDeployment.bind()), name="app", route_prefix="/test"
    )

    # Make a request to ensure everything is running
    url = get_application_url("HTTP", "app")
    assert httpx.get(url).text == "child"

    timeseries = PrometheusTimeseries()

    # Test 1: Check proxy main loop metrics
    def check_proxy_main_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "proxy" and m.get("loop_type") == "main":
                assert "actor_id" in m, "actor_id tag should be present"
                print(f"Proxy main loop metric found: {m}")
                return True
        return False

    wait_for_condition(check_proxy_main_loop_metrics, timeout=30)
    print("Proxy main loop monitoring metrics verified.")

    # Test 1a: Check proxy router loop metrics
    def check_proxy_router_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "proxy" and m.get("loop_type") == "router":
                assert "actor_id" in m, "actor_id tag should be present"
                print(f"Proxy router loop metric found: {m}")
                return True
        return False

    if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
        wait_for_condition(check_proxy_router_loop_metrics, timeout=30)
        print("Proxy router loop monitoring metrics verified.")
    else:
        print("Proxy router loop monitoring metrics not verified.")

    # Test 2: Check replica main loop metrics
    def check_replica_main_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "replica" and m.get("loop_type") == "main":
                assert "actor_id" in m, "actor_id tag should be present"
                assert m.get("deployment") in [
                    "f",
                    "g",
                ], "deployment tag should be 'f' or 'g'"
                assert m.get("application") == "app", "application tag should be 'app'"
                print(f"Replica main loop metric found: {m}")
                return True
        return False

    wait_for_condition(check_replica_main_loop_metrics, timeout=30)
    print("Replica main loop monitoring metrics verified.")

    # Test 3: Check replica user_code loop metrics (enabled by default)
    def check_replica_user_code_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "replica" and m.get("loop_type") == "user_code":
                assert "actor_id" in m, "actor_id tag should be present"
                assert m.get("deployment") in [
                    "f",
                    "g",
                ], "deployment tag should be 'f' or 'g'"
                assert m.get("application") == "app", "application tag should be 'app'"
                print(f"Replica user_code loop metric found: {m}")
                return True
        return False

    if RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD:
        wait_for_condition(check_replica_user_code_loop_metrics, timeout=30)
        print("Replica user_code loop monitoring metrics verified.")
    else:
        print("Replica user_code loop monitoring metrics not verified.")

    # Test 4: Check router loop metrics (enabled by default)
    def check_router_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "replica" and m.get("loop_type") == "router":
                assert "actor_id" in m, "actor_id tag should be present"
                print(f"Router loop metric found: {m}")
                return True
        return False

    if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
        wait_for_condition(check_router_loop_metrics, timeout=30)
        print("Router loop monitoring metrics verified.")
    else:
        print("Router loop monitoring metrics not verified.")

    # Test 5: Check that scheduling latency histogram exists and has reasonable values
    def check_scheduling_latency_metric():
        # Check for the histogram count metric
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_scheduling_latency_ms_count",
            timeout=10,
            timeseries=timeseries,
        )
        # Should have metrics for proxy main, replica main, replica user_code, router
        component_loop_pairs = set()
        for m in metrics:
            component = m.get("component")
            loop_type = m.get("loop_type")
            if component and loop_type:
                component_loop_pairs.add((component, loop_type))

        expected_pairs = {
            ("proxy", "main"),
            ("replica", "main"),
        }
        if RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD:
            expected_pairs.add(("replica", "user_code"))
        if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
            expected_pairs.add(("replica", "router"))
            expected_pairs.add(("proxy", "router"))
        return expected_pairs.issubset(component_loop_pairs)

    wait_for_condition(check_scheduling_latency_metric, timeout=30)
    print("Scheduling latency histogram metrics verified.")

    # Test 6: Check that tasks gauge exists
    def check_tasks_gauge_metric():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_tasks",
            timeout=10,
            timeseries=timeseries,
        )
        # Should have metrics for proxy main, replica main, replica user_code, router
        component_loop_pairs = set()
        for m in metrics:
            component = m.get("component")
            loop_type = m.get("loop_type")
            if component and loop_type:
                component_loop_pairs.add((component, loop_type))

        expected_pairs = {
            ("proxy", "main"),
            ("replica", "main"),
        }
        if RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD:
            expected_pairs.add(("replica", "user_code"))
        if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
            expected_pairs.add(("replica", "router"))
            expected_pairs.add(("proxy", "router"))
        return expected_pairs.issubset(component_loop_pairs)

    wait_for_condition(check_tasks_gauge_metric, timeout=30)
    print("Event loop tasks gauge metrics verified.")


def test_actor_summary(serve_instance):
    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    class_names = {actor.class_name for actor in actors}
    assert class_names.issuperset(
        {"ServeController", "ProxyActor", "ServeReplica:app:f"}
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
