import os
from functools import partial
from multiprocessing import Pool
from typing import List, Dict, DefaultDict

import requests
import pytest

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.utils import block_until_http_ready
import ray.util.state as state_api
from fastapi import FastAPI
from ray.serve.metrics import Counter, Histogram, Gauge
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    RAY_SERVE_ENABLE_NEW_ROUTING,
)
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request


@pytest.fixture
def serve_start_shutdown():
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    ray.init(
        _metrics_export_port=9999,
        _system_config={
            "metrics_report_interval_ms": 100,
            "task_retry_delay_ms": 50,
        },
    )
    yield serve.start()
    serve.shutdown()
    ray.shutdown()


def test_serve_metrics_for_successful_connection(serve_start_shutdown):
    @serve.deployment(name="metrics")
    async def f(request):
        return "hello"

    handle = serve.run(f.bind())

    # send 10 concurrent requests
    url = "http://127.0.0.1:8000/metrics"
    ray.get([block_until_http_ready.remote(url) for _ in range(10)])
    ray.get([handle.remote(url) for _ in range(10)])

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
    assert processing_requests[0]["deployment"] == "app1_A"
    assert processing_requests[0]["application"] == "app1"
    print("serve_replica_processing_queries exists.")

    pending_requests = get_metric_dictionaries(
        "serve_replica_pending_queries", timeout=5
    )
    assert len(pending_requests) == 1
    assert pending_requests[0]["deployment"] == "app1_A"
    assert pending_requests[0]["application"] == "app1"
    print("serve_replica_pending_queries exists.")

    def ensure_request_processing():
        resp = requests.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        expected_metrics = {
            "serve_replica_processing_queries",
            "serve_replica_pending_queries",
        }
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_replica_processing_queries" in metrics:
                assert "1.0" in metrics
                expected_metrics.discard("serve_replica_processing_queries")
            elif "serve_replica_pending_queries" in metrics:
                assert "0.0" in metrics
                expected_metrics.discard("serve_replica_pending_queries")
        assert len(expected_metrics) == 0
        return True

    wait_for_condition(ensure_request_processing, timeout=5)


def test_http_metrics(serve_start_shutdown):
    # NOTE: These metrics should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
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

    # NOTE: This metric should be documented at
    # https://docs.ray.io/en/latest/serve/monitoring.html#metrics
    # Any updates here should be reflected there too.
    expected_metrics.append("serve_num_deployment_http_error_requests")
    expected_metrics.append("serve_http_request_latency_ms")

    @serve.deployment(name="A")
    class A:
        async def __init__(self):
            pass

        async def __call__(self):
            # Trigger RayActorError
            os._exit(0)

    serve.run(A.bind(), name="app")
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
                    assert 'deployment="app_A"' in metrics and "2.0" in metrics
                if 'deployment="app_A"' not in metrics or "2.0" not in metrics:
                    return False
        return True

    # There is a latency in updating the counter
    try:
        wait_for_condition(verify_error_count, retry_interval_ms=1000, timeout=10)
    except RuntimeError:
        verify_error_count(do_assert=True)


def test_http_metrics_fields(serve_start_shutdown):
    """Tests the http metrics' fields' behavior."""

    @serve.deployment(route_prefix="/real_route")
    def f(*args):
        return 1 / 0

    serve.run(f.bind(), name="app")

    # Should generate 404 responses
    broken_url = "http://127.0.0.1:8000/fake_route"
    for _ in range(10):
        requests.get(broken_url).text
    print("Sent requests to broken URL.")

    num_requests = get_metric_dictionaries("serve_num_http_requests")
    assert len(num_requests) == 1
    assert num_requests[0]["route"] == "/fake_route"
    assert num_requests[0]["method"] == "GET"
    assert num_requests[0]["application"] == ""
    assert num_requests[0]["status_code"] == "404"
    print("serve_num_http_requests working as expected.")

    num_errors = get_metric_dictionaries("serve_num_http_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/fake_route"
    assert num_errors[0]["error_code"] == "404"
    assert num_errors[0]["method"] == "GET"
    print("serve_num_http_error_requests working as expected.")

    # Deployment should generate divide-by-zero errors
    correct_url = "http://127.0.0.1:8000/real_route"
    for _ in range(10):
        requests.get(correct_url).text
    print("Sent requests to correct URL.")

    num_deployment_errors = get_metric_dictionaries(
        "serve_num_deployment_http_error_requests"
    )
    assert len(num_deployment_errors) == 1
    assert num_deployment_errors[0]["deployment"] == "app_f"
    assert num_deployment_errors[0]["error_code"] == "500"
    assert num_deployment_errors[0]["method"] == "GET"
    assert num_deployment_errors[0]["application"] == "app"
    print("serve_num_deployment_http_error_requests working as expected.")

    latency_metrics = get_metric_dictionaries("serve_http_request_latency_ms_sum")
    assert len(latency_metrics) == 1
    assert latency_metrics[0]["route"] == "/real_route"
    assert latency_metrics[0]["application"] == "app"
    assert latency_metrics[0]["status_code"] == "500"
    print("serve_http_request_latency_ms working as expected.")


def test_http_redirect_metrics(serve_start_shutdown):
    """Tests the http redirect metrics' behavior."""

    def verify_metrics_with_route(metrics, expected_metrics):
        assert len(metrics) == len(expected_metrics)
        for metric_dict in metrics:
            match_metric = None
            for expected_metric in expected_metrics:
                if expected_metric["route"] == metric_dict["route"]:
                    match_metric = expected_metric
                    break
            assert match_metric is not None
            for key in match_metric:
                assert match_metric[key] == metric_dict[key]

    @serve.deployment
    class Model:
        def __call__(self, *args):
            return "123"

    serve.run(
        DAGDriver.bind(Model.bind(), http_adapter=json_request), route_prefix="/bar"
    )
    resp = requests.get("http://localhost:8000/bar", json=["123"])
    assert resp.status_code == 200
    assert resp.text == '"123"'

    wait_for_condition(
        lambda: len(get_metric_dictionaries("serve_num_http_requests")) == 2,
        timeout=40,
    )
    num_http_requests = get_metric_dictionaries("serve_num_http_requests")
    expected_output = [
        {
            "route": "/bar/",
            "application": "default",
            "method": "GET",
            "status_code": "200",
        },
        {
            "route": "/bar",
            "application": "default",
            "method": "GET",
            "status_code": "307",
        },
    ]
    verify_metrics_with_route(num_http_requests, expected_output)

    wait_for_condition(
        lambda: len(get_metric_dictionaries("serve_http_request_latency_ms_sum")) == 2,
        timeout=40,
    )
    http_latency = get_metric_dictionaries("serve_num_http_requests")
    expected_output = [
        {"route": "/bar/", "application": "default", "status_code": "200"},
        {"route": "/bar", "application": "default", "status_code": "307"},
    ]
    verify_metrics_with_route(http_latency, expected_output)


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

    def verify_metrics(metric, expected_output):
        for key in expected_output:
            assert metric[key] == expected_output[key]

    wait_for_condition(
        lambda: len(get_metric_dictionaries("serve_deployment_request_counter")) == 2,
        timeout=40,
    )

    num_requests = get_metric_dictionaries("serve_deployment_request_counter")
    assert len(num_requests) == 2
    expected_output = {"route": "/f", "deployment": "app1_f", "application": "app1"}
    verify_metrics(num_requests[0], expected_output)

    start_metrics = get_metric_dictionaries("serve_deployment_replica_starts")
    assert len(start_metrics) == 2
    expected_output = {"deployment": "app1_f", "application": "app1"}
    verify_metrics(start_metrics[0], expected_output)
    expected_output = {"deployment": "app2_g", "application": "app2"}
    verify_metrics(start_metrics[1], expected_output)

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
        expected_output1 = {"deployment": "app1_f", "application": "app1"}
        expected_output2 = {"deployment": "app2_g", "application": "app2"}
        verify_metrics(latency_metrics[0], expected_output1)
        verify_metrics(latency_metrics[1], expected_output2)

    wait_for_condition(
        lambda: len(get_metric_dictionaries("serve_replica_processing_queries")) == 2
    )
    processing_queries = get_metric_dictionaries("serve_replica_processing_queries")
    expected_output1 = {"deployment": "app1_f", "application": "app1"}
    expected_output2 = {"deployment": "app2_g", "application": "app2"}
    verify_metrics(processing_queries[0], expected_output1)
    verify_metrics(processing_queries[1], expected_output2)

    @serve.deployment
    def h():
        return 1 / 0

    serve.run(h.bind(), name="app3", route_prefix="/h")
    assert 500 == requests.get("http://127.0.0.1:8000/h").status_code
    wait_for_condition(
        lambda: len(get_metric_dictionaries("serve_deployment_error_counter")) == 1,
        timeout=40,
    )
    err_requests = get_metric_dictionaries("serve_deployment_error_counter")
    assert len(err_requests) == 1
    expected_output = {"route": "/h", "deployment": "app3_h", "application": "app3"}
    verify_metrics(err_requests[0], expected_output)

    health_metrics = get_metric_dictionaries("serve_deployment_replica_healthy")
    assert len(health_metrics) == 3
    expected_outputs = [
        {"deployment": "app1_f", "application": "app1"},
        {"deployment": "app2_g", "application": "app2"},
        {"deployment": "app3_h", "application": "app3"},
    ]
    for i in range(len(health_metrics)):
        verify_metrics(health_metrics[i], expected_outputs[i])


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
            "serve_deployment_request_counter", "app1_f", "app1", "/app1"
        )
        wait_for_route_and_name(
            "serve_deployment_request_counter", "app2_g", "app2", "/app2"
        )
        wait_for_route_and_name(
            "serve_deployment_error_counter", "app3_h", "app3", "/app3"
        )

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
            assert metrics_route["app1_f"] == {"/app1"}
            assert metrics_route["app2_g"] == {"/app2"}
            assert metrics_route["app3_h"] == {"/app3"}
            assert metrics_app_name["app1_f"] == "app1"
            assert metrics_app_name["app2_g"] == "app2"
            assert metrics_app_name["app3_h"] == "app3"

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
            def __init__(self, handle1, handle2):
                self.handle1 = handle1
                self.handle2 = handle2

            @app.get("/api")
            async def app1(self):
                return await (await self.handle1.remote())

            @app.get("/api2")
            async def app2(self):
                return await (await self.handle2.remote())

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
        assert requests_metrics_route["app_G"] == {"/api", "/api2"}
        assert requests_metrics_route["app_g1"] == {"/api"}
        assert requests_metrics_route["app_g2"] == {"/api2"}
        assert requests_metrics_app_name["app_G"] == "app"
        assert requests_metrics_app_name["app_g1"] == "app"
        assert requests_metrics_app_name["app_g2"] == "app"

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
                    ray.serve.context._INTERNAL_REPLICA_CONTEXT.deployment,
                    ray.serve.context._INTERNAL_REPLICA_CONTEXT.replica_tag,
                ]

        serve.run(Model.bind(), name="app", route_prefix="/app")
        resp = requests.get("http://127.0.0.1:8000/app")
        deployment_name, replica_tag = resp.json()
        wait_for_condition(
            lambda: len(get_metric_dictionaries("my_gauge")) == 1,
            timeout=40,
        )

        counter_metrics = get_metric_dictionaries("my_counter")
        assert len(counter_metrics) == 1
        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "100",
            "replica": replica_tag,
            "deployment": deployment_name,
            "application": "app",
            "route": "/app",
        }
        self.verify_metrics(counter_metrics[0], expected_metrics)

        expected_metrics = {
            "my_static_tag": "static_value",
            "my_runtime_tag": "300",
            "replica": replica_tag,
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
            "replica": replica_tag,
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


def test_queued_queries_disconnected(serve_start_shutdown):
    """Check that disconnected queued queries are tracked correctly."""

    signal = SignalActor.remote()

    @serve.deployment(
        max_concurrent_queries=1,
        graceful_shutdown_timeout_s=0.0001,
    )
    async def hang_on_first_request():
        await signal.wait.remote()

    serve.run(hang_on_first_request.bind())

    print("Deployed hang_on_first_request deployment.")

    def check_metric(metric: str, expected: float) -> bool:
        metrics = requests.get("http://127.0.0.1:9999").text
        metric_value = -1
        for line in metrics.split("\n"):
            if metric in line:
                metric_value = line.split(" ")[-1]

        assert float(metric_value) == expected
        return True

    if RAY_SERVE_ENABLE_NEW_ROUTING:
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="ray_serve_num_scheduling_tasks",
            expected=0,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="serve_num_scheduling_tasks_in_backoff",
            expected=0,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

    def first_request_executing(request_future) -> bool:
        try:
            request_future.get(timeout=0.1)
        except Exception:
            return ray.get(signal.cur_num_waiters.remote()) == 1

    if RAY_SERVE_ENABLE_NEW_ROUTING:
        # No scheduling tasks should be running once the first request is assigned.
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="ray_serve_num_scheduling_tasks",
            expected=0,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="serve_num_scheduling_tasks_in_backoff",
            expected=0,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

    url = "http://localhost:8000/"

    pool = Pool()

    # Make a request to block the deployment from accepting other requests
    fut = pool.apply_async(partial(requests.get, url))
    wait_for_condition(lambda: first_request_executing(fut), timeout=5)
    print("Executed first request.")
    wait_for_condition(
        check_metric,
        timeout=15,
        metric="ray_serve_num_ongoing_http_requests",
        expected=1,
    )
    print("ray_serve_num_ongoing_http_requests updated successfully.")

    num_requests = 5
    for _ in range(num_requests):
        pool.apply_async(partial(requests.get, url))
    print(f"Executed {num_requests} more requests.")

    # First request should be processing. All others should be queued.
    wait_for_condition(
        check_metric,
        timeout=15,
        metric="ray_serve_deployment_queued_queries",
        expected=num_requests,
    )
    print("ray_serve_deployment_queued_queries updated successfully.")
    wait_for_condition(
        check_metric,
        timeout=15,
        metric="ray_serve_num_ongoing_http_requests",
        expected=num_requests + 1,
    )
    print("ray_serve_num_ongoing_http_requests updated successfully.")

    if RAY_SERVE_ENABLE_NEW_ROUTING:
        # There should be 2 scheduling tasks (which is the max, since
        # 2 = 2 * 1 replica) that are attempting to schedule the hanging requests.
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="ray_serve_num_scheduling_tasks",
            expected=2,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="serve_num_scheduling_tasks_in_backoff",
            expected=2,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")

    # Disconnect all requests by terminating the process pool.
    pool.terminate()
    print("Terminated all requests.")

    wait_for_condition(
        check_metric,
        timeout=15,
        metric="ray_serve_deployment_queued_queries",
        expected=0,
    )
    print("ray_serve_deployment_queued_queries updated successfully.")

    # TODO (shrekris-anyscale): This should be 0 once async task cancellation
    # is implemented.
    wait_for_condition(
        check_metric,
        timeout=15,
        metric="ray_serve_num_ongoing_http_requests",
        expected=1,
    )
    print("ray_serve_num_ongoing_http_requests updated successfully.")

    if RAY_SERVE_ENABLE_NEW_ROUTING:
        # TODO(shrekris-anyscale): This should be 0 once async task cancellation
        # is implemented.
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="ray_serve_num_scheduling_tasks",
            expected=2,
        )
        print("ray_serve_num_scheduling_tasks updated successfully.")
        wait_for_condition(
            check_metric,
            timeout=15,
            metric="serve_num_scheduling_tasks_in_backoff",
            expected=2,
        )
        print("serve_num_scheduling_tasks_in_backoff updated successfully.")


def test_actor_summary(serve_instance):
    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")
    actors = state_api.list_actors(filters=[("state", "=", "ALIVE")])
    class_names = {actor["class_name"] for actor in actors}
    assert class_names.issuperset(
        {"ServeController", "HTTPProxyActor", "ServeReplica:app_f"}
    )


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

    print(metric_dicts)
    return metric_dicts


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
