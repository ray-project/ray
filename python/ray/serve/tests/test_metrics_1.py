import os
import sys
from typing import Dict, List, Optional

import grpc
import pytest
import requests
from starlette.requests import Request

import ray
from ray import serve
from ray._private.test_utils import (
    SignalActor,
    fetch_prometheus_metrics,
    wait_for_condition,
)
from ray.serve._private.test_utils import (
    ping_grpc_call_method,
    ping_grpc_list_applications,
)
from ray.serve._private.utils import block_until_http_ready
from ray.serve.config import HTTPOptions, gRPCOptions

TEST_METRICS_EXPORT_PORT = 9999


@pytest.fixture
def serve_start_shutdown(request):
    serve.shutdown()
    ray.shutdown()
    ray._private.utils.reset_ray_address()

    param = request.param if hasattr(request, "param") else None
    request_timeout_s = param if param else None
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    ray.init(
        _metrics_export_port=TEST_METRICS_EXPORT_PORT,
        _system_config={
            "metrics_report_interval_ms": 100,
            "task_retry_delay_ms": 50,
        },
    )
    grpc_port = 9000
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]
    yield serve.start(
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
            request_timeout_s=request_timeout_s,
        ),
        http_options=HTTPOptions(
            request_timeout_s=request_timeout_s,
        ),
    )
    serve.shutdown()
    ray.shutdown()
    ray._private.utils.reset_ray_address()


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

    metrics = requests.get("http://127.0.0.1:9999").text
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

    metrics = fetch_prometheus_metrics([f"localhost:{TEST_METRICS_EXPORT_PORT}"])
    metric_samples = metrics.get(metric_name, None)
    if metric_samples is None:
        metric_sum = 0
    else:
        metric_samples = [
            sample for sample in metric_samples if tags.items() <= sample.labels.items()
        ]
        metric_sum = sum(sample.value for sample in metric_samples)

    # Check the metrics sum to the expected number
    assert float(metric_sum) == float(
        expected
    ), f"The following metrics don't sum to {expected}: {metric_samples}. {metrics}"

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
        metrics = requests.get("http://127.0.0.1:9999").text
        return name in metrics

    wait_for_condition(metric_available, retry_interval_ms=1000, timeout=timeout)

    metrics = requests.get("http://127.0.0.1:9999").text

    metric_dicts = []
    for line in metrics.split("\n"):
        if name + "{" in line:
            dict_body_start, dict_body_end = line.find("{") + 1, line.rfind("}")
            metric_dict_str = f"dict({line[dict_body_start:dict_body_end]})"
            metric_dicts.append(eval(metric_dict_str))

    print(f"{name=} {metric_dicts=}")
    return metric_dicts


def test_serve_metrics_for_successful_connection(serve_start_shutdown):
    @serve.deployment(name="metrics")
    async def f(request):
        return "hello"

    app_name = "app1"
    handle = serve.run(target=f.bind(), name=app_name)

    # send 10 concurrent requests
    url = "http://127.0.0.1:8000/metrics"
    ray.get([block_until_http_ready.remote(url) for _ in range(10)])
    [handle.remote(url) for _ in range(10)]

    # Ping gPRC proxy
    channel = grpc.insecure_channel("localhost:9000")
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=[app_name]
    )

    def verify_metrics(do_assert=False):
        try:
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
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


def test_http_replica_gauge_metrics(serve_start_shutdown):
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
        resp = requests.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_replica_processing_queries" in metrics:
                assert "1.0" in metrics
        return True

    wait_for_condition(ensure_request_processing, timeout=5)


def test_proxy_metrics_not_found(serve_start_shutdown):
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
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
            return False
        for metric in _expected_metrics:
            if do_assert:
                assert metric in resp
            if metric not in resp:
                return False
        return True

    # Trigger HTTP 404 error
    requests.get("http://127.0.0.1:8000/B/")
    requests.get("http://127.0.0.1:8000/B/")

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
        resp = requests.get("http://127.0.0.1:9999").text
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


def test_proxy_metrics_internal_error(serve_start_shutdown):
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
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
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
    requests.get("http://127.0.0.1:8000/A/")
    requests.get("http://127.0.0.1:8000/A/")
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
        resp = requests.get("http://127.0.0.1:9999").text
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


def test_proxy_metrics_fields_not_found(serve_start_shutdown):
    """Tests the proxy metrics' fields' behavior for not found."""

    # Should generate 404 responses
    broken_url = "http://127.0.0.1:8000/fake_route"
    _ = requests.get(broken_url).text
    print("Sent requests to broken URL.")

    # Ping gRPC proxy for not existing application.
    channel = grpc.insecure_channel("localhost:9000")
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
    "serve_start_shutdown",
    [
        1,
    ],
    indirect=True,
)
def test_proxy_timeout_metrics(serve_start_shutdown):
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

    r = requests.get("http://127.0.0.1:8000/status_code_timeout")
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
