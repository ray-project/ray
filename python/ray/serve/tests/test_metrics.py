import http
import json
import os
import sys
import threading
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
    fetch_prometheus_metrics,
)
from ray.serve._private.long_poll import LongPollHost, UpdatedObject
from ray.serve._private.test_utils import (
    get_application_url,
    ping_grpc_call_method,
    ping_grpc_list_applications,
)
from ray.serve._private.utils import block_until_http_ready
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


def get_metric_dictionaries(name: str, timeout: float = 20) -> List[Dict]:
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

    def metric_available() -> bool:
        metrics = httpx.get("http://127.0.0.1:9999", timeout=10).text
        assert name in metrics
        return True

    wait_for_condition(metric_available, retry_interval_ms=1000, timeout=timeout)

    metrics = httpx.get("http://127.0.0.1:9999").text
    serve_metrics = [line for line in metrics.splitlines() if "ray_serve_" in line]
    print("metrics", "\n".join(serve_metrics))

    metric_dicts = []
    for line in metrics.split("\n"):
        if name + "{" in line:
            dict_body_start, dict_body_end = line.find("{") + 1, line.rfind("}")
            metric_dict_str = f"dict({line[dict_body_start:dict_body_end]})"
            metric_dicts.append(eval(metric_dict_str))

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
        "serve_replica_processing_queries", timeout=5
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

    num_requests = get_metric_dictionaries("serve_num_http_requests")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == ""
    assert num_requests[0]["method"] == "GET"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == "404"
    print("serve_num_http_requests working as expected.")

    num_requests = get_metric_dictionaries("serve_num_grpc_requests")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == ""
    assert num_requests[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == str(grpc.StatusCode.NOT_FOUND)
    print("serve_num_grpc_requests working as expected.")

    num_errors = get_metric_dictionaries("serve_num_http_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == ""
    assert num_errors[0]["error_code"] == "404"
    assert num_errors[0]["method"] == "GET"
    print("serve_num_http_error_requests working as expected.")

    num_errors = get_metric_dictionaries("serve_num_grpc_error_requests")
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

    num_errors = get_metric_dictionaries("serve_num_http_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/status_code_timeout"
    assert num_errors[0]["error_code"] == "408"
    assert num_errors[0]["method"] == "GET"
    assert num_errors[0]["application"] == "status_code_timeout"

    num_errors = get_metric_dictionaries("serve_num_grpc_error_requests")
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

    num_errors = get_metric_dictionaries("serve_num_http_error_requests")
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

    num_errors = get_metric_dictionaries("serve_num_grpc_error_requests")
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
        "serve_num_deployment_http_error_requests"
    )
    assert len(num_deployment_errors) == 1
    assert num_deployment_errors[0]["deployment"] == "f"
    assert num_deployment_errors[0]["error_code"] == "500"
    assert num_deployment_errors[0]["method"] == "GET"
    assert num_deployment_errors[0]["application"] == "app"
    print("serve_num_deployment_http_error_requests working as expected.")

    num_deployment_errors = get_metric_dictionaries(
        "serve_num_deployment_grpc_error_requests"
    )
    assert len(num_deployment_errors) == 1
    assert num_deployment_errors[0]["deployment"] == "f"
    assert num_deployment_errors[0]["error_code"] == str(grpc.StatusCode.INTERNAL)
    assert (
        num_deployment_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    )
    assert num_deployment_errors[0]["application"] == real_app_name
    print("serve_num_deployment_grpc_error_requests working as expected.")

    latency_metrics = get_metric_dictionaries("serve_http_request_latency_ms_sum")
    assert len(latency_metrics) == 1
    assert latency_metrics[0]["method"] == "GET"
    assert latency_metrics[0]["route"] == "/real_route"
    assert latency_metrics[0]["application"] == "app"
    assert latency_metrics[0]["status_code"] == "500"
    print("serve_http_request_latency_ms working as expected.")

    latency_metrics = get_metric_dictionaries("serve_grpc_request_latency_ms_sum")
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
        lambda: len(get_metric_dictionaries("serve_deployment_request_counter_total"))
        == 2,
        timeout=40,
    )

    metrics = get_metric_dictionaries("serve_deployment_request_counter_total")
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

    start_metrics = get_metric_dictionaries("serve_deployment_replica_starts_total")
    assert len(start_metrics) == 2
    expected_output = {("f", "app1"), ("g", "app2")}
    assert {
        (start_metric["deployment"], start_metric["application"])
        for start_metric in start_metrics
    } == expected_output

    # Latency metrics
    wait_for_condition(
        lambda: len(
            get_metric_dictionaries("serve_deployment_processing_latency_ms_count")
        )
        == 2,
        timeout=40,
    )
    for metric_name in [
        "serve_deployment_processing_latency_ms_count",
        "serve_deployment_processing_latency_ms_sum",
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
        lambda: len(get_metric_dictionaries("serve_replica_processing_queries")) == 2
    )
    processing_queries = get_metric_dictionaries("serve_replica_processing_queries")
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
        lambda: len(get_metric_dictionaries("serve_deployment_error_counter_total"))
        == 1,
        timeout=40,
    )
    err_requests = get_metric_dictionaries("serve_deployment_error_counter_total")
    assert len(err_requests) == 1
    expected_output = ("/h", "h", "app3")
    assert (
        err_requests[0]["route"],
        err_requests[0]["deployment"],
        err_requests[0]["application"],
    ) == expected_output

    wait_for_condition(
        lambda: len(get_metric_dictionaries("serve_deployment_replica_healthy")) == 3,
    )
    health_metrics = get_metric_dictionaries("serve_deployment_replica_healthy")
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
        metrics = get_metric_dictionaries("serve_num_http_requests")
        api_metrics = [m for m in metrics if m.get("application") == "api_app"]
        return len(api_metrics) >= 3

    wait_for_condition(metrics_available, timeout=20)

    # Verify metrics use route patterns, not individual paths
    metrics = get_metric_dictionaries("serve_num_http_requests")
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
    num_errors = get_metric_dictionaries("serve_http_request_latency_ms_sum")
    api_latency_metrics = [m for m in num_errors if m.get("application") == "api_app"]
    latency_routes = {m["route"] for m in api_latency_metrics}

    # Latency metrics should also use patterns
    assert (
        "/api/users/{user_id}" in latency_routes or "/api/" in latency_routes
    ), f"Latency metrics should use route patterns. Found: {latency_routes}"


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
        metric="serve_long_poll_host_transmission_counter",
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
        metric="serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "key_2"},
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="serve_long_poll_host_transmission_counter",
        expected=2,
        expected_tags={"namespace_or_state": "key_1"},
    )

    # Check that a timeout result is counted.
    object_ref = host.listen_for_change.remote({"key_2": result_2["key_2"].snapshot_id})
    _ = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="serve_long_poll_host_transmission_counter",
        expected=1,
        expected_tags={"namespace_or_state": "TIMEOUT"},
    )


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
