import os
import random
import sys
from typing import DefaultDict, Dict, List, Optional

import grpc
import pytest
import requests
from fastapi import FastAPI

import ray
import ray.util.state as state_api
from ray import serve
from ray._private.test_utils import (
    SignalActor,
    fetch_prometheus_metrics,
    wait_for_condition,
)
from ray.serve._private.constants import DEFAULT_LATENCY_BUCKET_MS
from ray.serve._private.long_poll import LongPollHost, UpdatedObject
from ray.serve._private.test_utils import (
    ping_fruit_stand,
    ping_grpc_call_method,
    ping_grpc_list_applications,
)
from ray.serve._private.utils import block_until_http_ready
from ray.serve.config import gRPCOptions
from ray.serve.handle import DeploymentHandle
from ray.serve.metrics import Counter, Gauge, Histogram
from ray.serve.tests.test_config_files.grpc_deployment import g, g2

TEST_METRICS_EXPORT_PORT = 9999


@pytest.fixture
def serve_start_shutdown():
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
    print("metrics", metrics)

    metric_dicts = []
    for line in metrics.split("\n"):
        if name + "{" in line:
            dict_body_start, dict_body_end = line.find("{") + 1, line.rfind("}")
            metric_dict_str = f"dict({line[dict_body_start:dict_body_end]})"
            metric_dicts.append(eval(metric_dict_str))

    print(metric_dicts)
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
    requests.get(broken_url).text
    print("Sent requests to broken URL.")

    # Ping gRPC proxy for not existing application.
    channel = grpc.insecure_channel("localhost:9000")
    fake_app_name = "fake-app"
    ping_grpc_call_method(channel=channel, app_name=fake_app_name, test_not_found=True)

    num_requests = get_metric_dictionaries("serve_num_http_requests")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == "/fake_route"
    assert num_requests[0]["method"] == "GET"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == "404"
    print("serve_num_http_requests working as expected.")

    num_requests = get_metric_dictionaries("serve_num_grpc_requests")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == fake_app_name
    assert num_requests[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == str(grpc.StatusCode.NOT_FOUND)
    print("serve_num_grpc_requests working as expected.")

    num_errors = get_metric_dictionaries("serve_num_http_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/fake_route"
    assert num_errors[0]["error_code"] == "404"
    assert num_errors[0]["method"] == "GET"
    print("serve_num_http_error_requests working as expected.")

    num_errors = get_metric_dictionaries("serve_num_grpc_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == fake_app_name
    assert num_errors[0]["error_code"] == str(grpc.StatusCode.NOT_FOUND)
    assert num_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    print("serve_num_grpc_error_requests working as expected.")


def test_proxy_metrics_fields_internal_error(serve_start_shutdown):
    """Tests the proxy metrics' fields' behavior for internal error."""

    @serve.deployment()
    def f(*args):
        return 1 / 0

    real_app_name = "app"
    real_app_name2 = "app2"
    serve.run(f.bind(), name=real_app_name, route_prefix="/real_route")
    serve.run(f.bind(), name=real_app_name2, route_prefix="/real_route2")

    # Deployment should generate divide-by-zero errors
    correct_url = "http://127.0.0.1:8000/real_route"
    requests.get(correct_url).text
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


def test_replica_metrics_fields(serve_start_shutdown):
    """Test replica metrics fields"""

    @serve.deployment
    def f():
        return "hello"

    @serve.deployment
    def g():
        return "world"

    serve.run(f.bind(), name="app1", route_prefix="/f")
    serve.run(g.bind(), name="app2", route_prefix="/g")
    url_f = "http://127.0.0.1:8000/f"
    url_g = "http://127.0.0.1:8000/g"

    assert "hello" == requests.get(url_f).text
    assert "world" == requests.get(url_g).text

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
    assert 500 == requests.get("http://127.0.0.1:8000/h").status_code
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

    health_metrics = get_metric_dictionaries("serve_deployment_replica_healthy")
    assert len(health_metrics) == 3, health_metrics
    expected_output = {
        ("f", "app1"),
        ("g", "app2"),
        ("h", "app3"),
    }
    assert {
        (health_metric["deployment"], health_metric["application"])
        for health_metric in health_metrics
    } == expected_output


class TestRequestContextMetrics:
    def _generate_metrics_summary(self, metrics):
        """Generate "route", "application" information from metrics.
        Args:
            metrics: list of metrics, each item is a dictionary generated from
                get_metric_dictionaries func.
        Return: return a Tuple[dictionary, dictionary]
            First dictionary: key is deployment name, value is a set
            including all routes. string is to indicate the applicationn name.
            Second dictionary: key is the deployment name, value is application name.
        """
        metrics_summary_route = DefaultDict(set)
        metrics_summary_app = DefaultDict(str)

        for request_metrics in metrics:
            metrics_summary_route[request_metrics["deployment"]].add(
                request_metrics["route"]
            )
            metrics_summary_app[request_metrics["deployment"]] = request_metrics[
                "application"
            ]
        return metrics_summary_route, metrics_summary_app

    def verify_metrics(self, metric, expected_output):
        for key in expected_output:
            assert metric[key] == expected_output[key]

    def test_request_context_pass_for_http_proxy(self, serve_start_shutdown):
        """Test HTTP proxy passing request context"""

        @serve.deployment(graceful_shutdown_timeout_s=0.001)
        def f():
            return "hello"

        @serve.deployment(graceful_shutdown_timeout_s=0.001)
        def g():
            return "world"

        @serve.deployment(graceful_shutdown_timeout_s=0.001)
        def h():
            return 1 / 0

        serve.run(f.bind(), name="app1", route_prefix="/app1")
        serve.run(g.bind(), name="app2", route_prefix="/app2")
        serve.run(h.bind(), name="app3", route_prefix="/app3")

        resp = requests.get("http://127.0.0.1:8000/app1")
        assert resp.status_code == 200
        assert resp.text == "hello"
        resp = requests.get("http://127.0.0.1:8000/app2")
        assert resp.status_code == 200
        assert resp.text == "world"
        resp = requests.get("http://127.0.0.1:8000/app3")
        assert resp.status_code == 500

        wait_for_condition(
            lambda: len(
                get_metric_dictionaries("serve_deployment_processing_latency_ms_sum")
            )
            == 3,
            timeout=40,
        )

        def wait_for_route_and_name(
            metric_name: str,
            deployment_name: str,
            app_name: str,
            route: str,
            timeout: float = 5,
        ):
            """Waits for app name and route to appear in deployment's metric."""

            def check():
                # Check replica qps & latency
                (
                    qps_metrics_route,
                    qps_metrics_app_name,
                ) = self._generate_metrics_summary(get_metric_dictionaries(metric_name))
                assert qps_metrics_app_name[deployment_name] == app_name
                assert qps_metrics_route[deployment_name] == {route}
                return True

            wait_for_condition(check, timeout=timeout)

        # Check replica qps & latency
        wait_for_route_and_name(
            "serve_deployment_request_counter", "f", "app1", "/app1"
        )
        wait_for_route_and_name(
            "serve_deployment_request_counter", "g", "app2", "/app2"
        )
        wait_for_route_and_name("serve_deployment_error_counter", "h", "app3", "/app3")

        # Check http proxy qps & latency
        for metric_name in [
            "serve_num_http_requests",
            "serve_http_request_latency_ms_sum",
        ]:
            metrics = get_metric_dictionaries(metric_name)
            assert {metric["route"] for metric in metrics} == {
                "/app1",
                "/app2",
                "/app3",
            }

        for metric_name in [
            "serve_handle_request_counter",
            "serve_num_router_requests",
            "serve_deployment_processing_latency_ms_sum",
        ]:
            metrics_route, metrics_app_name = self._generate_metrics_summary(
                get_metric_dictionaries(metric_name)
            )
            msg = f"Incorrect metrics for {metric_name}"
            assert metrics_route["f"] == {"/app1"}, msg
            assert metrics_route["g"] == {"/app2"}, msg
            assert metrics_route["h"] == {"/app3"}, msg
            assert metrics_app_name["f"] == "app1", msg
            assert metrics_app_name["g"] == "app2", msg
            assert metrics_app_name["h"] == "app3", msg

    def test_request_context_pass_for_grpc_proxy(self, serve_start_shutdown):
        """Test gRPC proxy passing request context"""

        @serve.deployment(graceful_shutdown_timeout_s=0.001)
        class H:
            def __call__(self, *args, **kwargs):
                return 1 / 0

        h = H.bind()
        app_name1 = "app1"
        depl_name1 = "grpc-deployment"
        app_name2 = "app2"
        depl_name2 = "grpc-deployment-model-composition"
        app_name3 = "app3"
        depl_name3 = "H"
        serve.run(g, name=app_name1, route_prefix="/app1")
        serve.run(g2, name=app_name2, route_prefix="/app2")
        serve.run(h, name=app_name3, route_prefix="/app3")

        channel = grpc.insecure_channel("localhost:9000")
        ping_grpc_call_method(channel, app_name1)
        ping_fruit_stand(channel, app_name2)
        with pytest.raises(grpc.RpcError):
            ping_grpc_call_method(channel, app_name3)

        # app1 has 1 deployment, app2 has 3 deployments, and app3 has 1 deployment.
        wait_for_condition(
            lambda: len(
                get_metric_dictionaries("serve_deployment_processing_latency_ms_sum")
            )
            == 5,
            timeout=40,
        )

        def wait_for_route_and_name(
            _metric_name: str,
            deployment_name: str,
            app_name: str,
            route: str,
            timeout: float = 5,
        ):
            """Waits for app name and route to appear in deployment's metric."""

            def check():
                # Check replica qps & latency
                (
                    qps_metrics_route,
                    qps_metrics_app_name,
                ) = self._generate_metrics_summary(
                    get_metric_dictionaries(_metric_name)
                )
                assert qps_metrics_app_name[deployment_name] == app_name
                assert qps_metrics_route[deployment_name] == {route}
                return True

            wait_for_condition(check, timeout=timeout)

        # Check replica qps & latency
        wait_for_route_and_name(
            "serve_deployment_request_counter", depl_name1, app_name1, app_name1
        )
        wait_for_route_and_name(
            "serve_deployment_request_counter", depl_name2, app_name2, app_name2
        )
        wait_for_route_and_name(
            "serve_deployment_error_counter", depl_name3, app_name3, app_name3
        )

        # Check grpc proxy qps & latency
        for metric_name in [
            "serve_num_grpc_requests",
            "serve_grpc_request_latency_ms_sum",
        ]:
            metrics = get_metric_dictionaries(metric_name)
            assert {metric["route"] for metric in metrics} == {
                "app1",
                "app2",
                "app3",
            }

        for metric_name in [
            "serve_handle_request_counter",
            "serve_num_router_requests",
            "serve_deployment_processing_latency_ms_sum",
        ]:
            metrics_route, metrics_app_name = self._generate_metrics_summary(
                get_metric_dictionaries(metric_name)
            )
            msg = f"Incorrect metrics for {metric_name}"
            assert metrics_route[depl_name1] == {"app1"}, msg
            assert metrics_route[depl_name2] == {"app2"}, msg
            assert metrics_route[depl_name3] == {"app3"}, msg
            assert metrics_app_name[depl_name1] == "app1", msg
            assert metrics_app_name[depl_name2] == "app2", msg
            assert metrics_app_name[depl_name3] == "app3", msg

    def test_request_context_pass_for_handle_passing(self, serve_start_shutdown):
        """Test handle passing contexts between replicas"""

        @serve.deployment
        def g1():
            return "ok1"

        @serve.deployment
        def g2():
            return "ok2"

        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class G:
            def __init__(self, handle1: DeploymentHandle, handle2: DeploymentHandle):
                self.handle1 = handle1
                self.handle2 = handle2

            @app.get("/api")
            async def app1(self):
                return await self.handle1.remote()

            @app.get("/api2")
            async def app2(self):
                return await self.handle2.remote()

        serve.run(G.bind(g1.bind(), g2.bind()), name="app")
        resp = requests.get("http://127.0.0.1:8000/api")
        assert resp.text == '"ok1"'
        resp = requests.get("http://127.0.0.1:8000/api2")
        assert resp.text == '"ok2"'

        # G deployment metrics:
        #   {xxx, route:/api}, {xxx, route:/api2}
        # g1 deployment metrics:
        #   {xxx, route:/api}
        # g2 deployment metrics:
        #   {xxx, route:/api2}
        wait_for_condition(
            lambda: len(get_metric_dictionaries("serve_deployment_request_counter"))
            == 4,
            timeout=40,
        )
        (
            requests_metrics_route,
            requests_metrics_app_name,
        ) = self._generate_metrics_summary(
            get_metric_dictionaries("serve_deployment_request_counter")
        )
        assert requests_metrics_route["G"] == {"/api", "/api2"}
        assert requests_metrics_route["g1"] == {"/api"}
        assert requests_metrics_route["g2"] == {"/api2"}
        assert requests_metrics_app_name["G"] == "app"
        assert requests_metrics_app_name["g1"] == "app"
        assert requests_metrics_app_name["g2"] == "app"

    def test_customer_metrics_with_context(self, serve_start_shutdown):
        @serve.deployment
        class Model:
            def __init__(self):
                self.counter = Counter(
                    "my_counter",
                    description="my counter metrics",
                    tag_keys=(
                        "my_static_tag",
                        "my_runtime_tag",
                        "route",
                    ),
                )
                self.counter.set_default_tags({"my_static_tag": "static_value"})
                self.histogram = Histogram(
                    "my_histogram",
                    description=("my histogram "),
                    boundaries=DEFAULT_LATENCY_BUCKET_MS,
                    tag_keys=(
                        "my_static_tag",
                        "my_runtime_tag",
                        "route",
                    ),
                )
                self.histogram.set_default_tags({"my_static_tag": "static_value"})
                self.gauge = Gauge(
                    "my_gauge",
                    description=("my_gauge"),
                    tag_keys=(
                        "my_static_tag",
                        "my_runtime_tag",
                        "route",
                    ),
                )
                self.gauge.set_default_tags({"my_static_tag": "static_value"})

            def __call__(self):
                self.counter.inc(tags={"my_runtime_tag": "100"})
                self.histogram.observe(200, tags={"my_runtime_tag": "200"})
                self.gauge.set(300, tags={"my_runtime_tag": "300"})
                return [
                    # NOTE(zcin): this is to match the current implementation in
                    # Serve's _add_serve_metric_default_tags().
                    ray.serve.context._INTERNAL_REPLICA_CONTEXT.deployment,
                    ray.serve.context._INTERNAL_REPLICA_CONTEXT.replica_id.unique_id,
                ]

        serve.run(Model.bind(), name="app", route_prefix="/app")
        resp = requests.get("http://127.0.0.1:8000/app")
        deployment_name, replica_id = resp.json()
        wait_for_condition(
            lambda: len(get_metric_dictionaries("my_gauge")) == 1,
            timeout=40,
        )

        counter_metrics = get_metric_dictionaries("my_counter")
        assert len(counter_metrics) == 1
        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "100",
            "replica": replica_id,
            "deployment": deployment_name,
            "application": "app",
            "route": "/app",
        }
        self.verify_metrics(counter_metrics[0], expected_metrics)

        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "300",
            "replica": replica_id,
            "deployment": deployment_name,
            "application": "app",
            "route": "/app",
        }
        gauge_metrics = get_metric_dictionaries("my_gauge")
        assert len(counter_metrics) == 1
        self.verify_metrics(gauge_metrics[0], expected_metrics)

        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "200",
            "replica": replica_id,
            "deployment": deployment_name,
            "application": "app",
            "route": "/app",
        }
        histogram_metrics = get_metric_dictionaries("my_histogram_sum")
        assert len(histogram_metrics) == 1
        self.verify_metrics(histogram_metrics[0], expected_metrics)

    @pytest.mark.parametrize("use_actor", [False, True])
    def test_serve_metrics_outside_serve(self, use_actor, serve_start_shutdown):
        """Make sure ray.serve.metrics work in ray actor"""
        if use_actor:

            @ray.remote
            class MyActor:
                def __init__(self):
                    self.counter = Counter(
                        "my_counter",
                        description="my counter metrics",
                        tag_keys=(
                            "my_static_tag",
                            "my_runtime_tag",
                        ),
                    )
                    self.counter.set_default_tags({"my_static_tag": "static_value"})
                    self.histogram = Histogram(
                        "my_histogram",
                        description=("my histogram "),
                        boundaries=DEFAULT_LATENCY_BUCKET_MS,
                        tag_keys=(
                            "my_static_tag",
                            "my_runtime_tag",
                        ),
                    )
                    self.histogram.set_default_tags({"my_static_tag": "static_value"})
                    self.gauge = Gauge(
                        "my_gauge",
                        description=("my_gauge"),
                        tag_keys=(
                            "my_static_tag",
                            "my_runtime_tag",
                        ),
                    )
                    self.gauge.set_default_tags({"my_static_tag": "static_value"})

                def test(self):
                    self.counter.inc(tags={"my_runtime_tag": "100"})
                    self.histogram.observe(200, tags={"my_runtime_tag": "200"})
                    self.gauge.set(300, tags={"my_runtime_tag": "300"})
                    return "hello"

        else:
            counter = Counter(
                "my_counter",
                description="my counter metrics",
                tag_keys=(
                    "my_static_tag",
                    "my_runtime_tag",
                ),
            )
            histogram = Histogram(
                "my_histogram",
                description=("my histogram "),
                boundaries=DEFAULT_LATENCY_BUCKET_MS,
                tag_keys=(
                    "my_static_tag",
                    "my_runtime_tag",
                ),
            )
            gauge = Gauge(
                "my_gauge",
                description=("my_gauge"),
                tag_keys=(
                    "my_static_tag",
                    "my_runtime_tag",
                ),
            )

            @ray.remote
            def fn():
                counter.set_default_tags({"my_static_tag": "static_value"})
                histogram.set_default_tags({"my_static_tag": "static_value"})
                gauge.set_default_tags({"my_static_tag": "static_value"})
                counter.inc(tags={"my_runtime_tag": "100"})
                histogram.observe(200, tags={"my_runtime_tag": "200"})
                gauge.set(300, tags={"my_runtime_tag": "300"})
                return "hello"

        @serve.deployment
        class Model:
            def __init__(self):
                if use_actor:
                    self.my_actor = MyActor.remote()

            async def __call__(self):
                if use_actor:
                    return await self.my_actor.test.remote()
                else:
                    return await fn.remote()

        serve.run(Model.bind(), name="app", route_prefix="/app")
        resp = requests.get("http://127.0.0.1:8000/app")
        assert resp.text == "hello"
        wait_for_condition(
            lambda: len(get_metric_dictionaries("my_gauge")) == 1,
            timeout=40,
        )

        counter_metrics = get_metric_dictionaries("my_counter")
        assert len(counter_metrics) == 1
        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "100",
        }
        self.verify_metrics(counter_metrics[0], expected_metrics)

        gauge_metrics = get_metric_dictionaries("my_gauge")
        assert len(counter_metrics) == 1
        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "300",
        }
        self.verify_metrics(gauge_metrics[0], expected_metrics)

        histogram_metrics = get_metric_dictionaries("my_histogram_sum")
        assert len(histogram_metrics) == 1
        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "200",
        }
        self.verify_metrics(histogram_metrics[0], expected_metrics)


def test_multiplexed_metrics(serve_start_shutdown):
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
            resp = requests.get("http://127.0.0.1:9999").text
        # Requests will fail if we are crashing the controller
        except requests.ConnectionError:
            return False
        for metric in expected_metrics:
            assert metric in resp
        return True

    wait_for_condition(
        verify_metrics,
        timeout=40,
        retry_interval_ms=1000,
    )


@serve.deployment
class WaitForSignal:
    async def __call__(self):
        signal = ray.get_actor("signal123")
        await signal.wait.remote()


@serve.deployment
class Router:
    def __init__(self, handles):
        self.handles = handles

    async def __call__(self, index: int):
        return await self.handles[index - 1].remote()


@ray.remote
def call(deployment_name, app_name, *args):
    handle = DeploymentHandle(deployment_name, app_name)
    handle.remote(*args)


@ray.remote
class CallActor:
    def __init__(self, deployment_name: str, app_name: str):
        self.handle = DeploymentHandle(deployment_name, app_name)

    async def call(self, *args):
        await self.handle.remote(*args)


class TestHandleMetrics:
    def test_queued_queries_basic(self, serve_start_shutdown):
        signal = SignalActor.options(name="signal123").remote()
        serve.run(WaitForSignal.options(max_ongoing_requests=1).bind(), name="app1")

        # First call should get assigned to a replica
        # call.remote("WaitForSignal", "app1")
        caller = CallActor.remote("WaitForSignal", "app1")
        caller.call.remote()

        for i in range(5):
            # call.remote("WaitForSignal", "app1")
            # c.call.remote()
            caller.call.remote()
            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_deployment_queued_queries",
                tags={"application": "app1"},
                expected=i + 1,
            )

        # Release signal
        ray.get(signal.send.remote())
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=0,
        )

    def test_queued_queries_multiple_handles(self, serve_start_shutdown):
        signal = SignalActor.options(name="signal123").remote()
        serve.run(WaitForSignal.options(max_ongoing_requests=1).bind(), name="app1")

        # Send first request
        call.remote("WaitForSignal", "app1")
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=0,
        )

        # Send second request (which should stay queued)
        call.remote("WaitForSignal", "app1")
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=1,
        )

        # Send third request (which should stay queued)
        call.remote("WaitForSignal", "app1")
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=2,
        )

        # Release signal
        ray.get(signal.send.remote())
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_deployment_queued_queries",
            tags={"application": "app1", "deployment": "WaitForSignal"},
            expected=0,
        )

    def test_queued_queries_disconnected(self, serve_start_shutdown):
        """Check that disconnected queued queries are tracked correctly."""

        signal = SignalActor.remote()

        @serve.deployment(
            max_ongoing_requests=1,
        )
        async def hang_on_first_request():
            await signal.wait.remote()

        serve.run(hang_on_first_request.bind())

        print("Deployed hang_on_first_request deployment.")

        wait_for_condition(
            check_metric_float_eq,
            timeout=15,
            metric="ray_serve_num_scheduling_tasks",
            # Router is eagerly created on HTTP proxy, so there are metrics emitted
            # from proxy router
            expected=0,
            # TODO(zcin): this tag shouldn't be necessary, there shouldn't be a mix of
            # metrics from new and old sessions.
            expected_tags={
                "SessionName": ray._private.worker.global_worker.node.session_name
            },
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_metric_float_eq,
            timeout=15,
            metric="serve_num_scheduling_tasks_in_backoff",
            # Router is eagerly created on HTTP proxy, so there are metrics emitted
            # from proxy router
            expected=0,
            # TODO(zcin): this tag shouldn't be necessary, there shouldn't be a mix of
            # metrics from new and old sessions.
            expected_tags={
                "SessionName": ray._private.worker.global_worker.node.session_name
            },
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

        @ray.remote(num_cpus=0)
        def do_request():
            r = requests.get("http://localhost:8000/")
            r.raise_for_status()
            return r

        # Make a request to block the deployment from accepting other requests.
        request_refs = [do_request.remote()]
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
        )

        print("First request is executing.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_ongoing_http_requests",
            expected=1,
        )
        print("ray_serve_num_ongoing_http_requests updated successfully.")

        num_queued_requests = 3
        request_refs.extend([do_request.remote() for _ in range(num_queued_requests)])
        print(f"{num_queued_requests} more requests now queued.")

        # First request should be processing. All others should be queued.
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_deployment_queued_queries",
            expected=num_queued_requests,
        )
        print("ray_serve_deployment_queued_queries updated successfully.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_ongoing_http_requests",
            expected=num_queued_requests + 1,
        )
        print("ray_serve_num_ongoing_http_requests updated successfully.")

        # There should be 2 scheduling tasks (which is the max, since
        # 2 = 2 * 1 replica) that are attempting to schedule the hanging requests.
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks",
            expected=2,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks_in_backoff",
            expected=2,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

        # Disconnect all requests by cancelling the Ray tasks.
        [ray.cancel(ref, force=True) for ref in request_refs]
        print("Cancelled all HTTP requests.")

        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_deployment_queued_queries",
            expected=0,
        )
        print("ray_serve_deployment_queued_queries updated successfully.")

        # Task should get cancelled.
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_ongoing_http_requests",
            expected=0,
        )
        print("ray_serve_num_ongoing_http_requests updated successfully.")

        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks",
            expected=0,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_sum_metric_eq,
            timeout=15,
            metric_name="ray_serve_num_scheduling_tasks_in_backoff",
            expected=0,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

        # Unblock hanging request.
        ray.get(signal.send.remote())

    def test_running_requests_gauge(self, serve_start_shutdown):
        signal = SignalActor.options(name="signal123").remote()
        serve.run(
            Router.options(num_replicas=2, ray_actor_options={"num_cpus": 0}).bind(
                [
                    WaitForSignal.options(
                        name="d1",
                        ray_actor_options={"num_cpus": 0},
                        max_ongoing_requests=2,
                        num_replicas=3,
                    ).bind(),
                    WaitForSignal.options(
                        name="d2",
                        ray_actor_options={"num_cpus": 0},
                        max_ongoing_requests=2,
                        num_replicas=3,
                    ).bind(),
                ],
            ),
            name="app1",
        )

        requests_sent = {1: 0, 2: 0}
        for i in range(5):
            index = random.choice([1, 2])
            print(f"Sending request to d{index}")
            call.remote("Router", "app1", index)
            requests_sent[index] += 1

            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_num_ongoing_requests_at_replicas",
                tags={"application": "app1", "deployment": "d1"},
                expected=requests_sent[1],
            )

            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_num_ongoing_requests_at_replicas",
                tags={"application": "app1", "deployment": "d2"},
                expected=requests_sent[2],
            )

            wait_for_condition(
                check_sum_metric_eq,
                metric_name="ray_serve_num_ongoing_requests_at_replicas",
                tags={"application": "app1", "deployment": "Router"},
                expected=i + 1,
            )

        # Release signal, the number of running requests should drop to 0
        ray.get(signal.send.remote())
        wait_for_condition(
            check_sum_metric_eq,
            metric_name="ray_serve_num_ongoing_requests_at_replicas",
            tags={"application": "app1"},
            expected=0,
        )


def test_long_poll_host_sends_counted(serve_instance):
    """Check that the transmissions by the long_poll are counted."""

    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.01, 0.01)
    )

    # Write a value.
    ray.get(host.notify_changed.remote("key_1", 999))
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
    ray.get(host.notify_changed.remote("key_1", 1000))
    ray.get(host.notify_changed.remote("key_2", 1000))
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
    actors = state_api.list_actors(filters=[("state", "=", "ALIVE")])
    class_names = {actor["class_name"] for actor in actors}
    assert class_names.issuperset(
        {"ServeController", "ProxyActor", "ServeReplica:app:f"}
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
