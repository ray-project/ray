import os
from typing import List, Dict, DefaultDict

import requests
import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.utils import block_until_http_ready
import ray.experimental.state.api as state_api
from fastapi import FastAPI
from ray.serve.metrics import Counter, Histogram, Gauge
from ray.serve._private.constants import DEFAULT_LATENCY_BUCKET_MS


@pytest.fixture
def serve_start_shutdown():
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    ray.init(
        _metrics_export_port=9999,
        _system_config={
            "metrics_report_interval_ms": 1000,
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
    print("serve_http_request_latency_ms working as expected.")


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
        timeout=20,
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
        timeout=20,
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

    processing_queries = get_metric_dictionaries("serve_replica_processing_queries")
    assert len(processing_queries) == 2
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
        timeout=20,
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

        for request_metrcis in metrics:
            metrics_summary_route[request_metrcis["deployment"]].add(
                request_metrcis["route"]
            )
            metrics_summary_app[request_metrcis["deployment"]] = request_metrcis[
                "application"
            ]
        return metrics_summary_route, metrics_summary_app

    def test_request_context_pass_for_http_proxy(self, serve_start_shutdown):
        """Test HTTP proxy passing request context"""

        @serve.deployment
        def f():
            return "hello"

        @serve.deployment
        def g():
            return "world"

        @serve.deployment
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
            timeout=20,
        )

        # Check replica qps & latency
        qps_metrics_route, qps_metrics_app_name = self._generate_metrics_summary(
            get_metric_dictionaries("serve_deployment_request_counter")
        )
        print(qps_metrics_route)
        assert qps_metrics_route["app1_f"] == {"/app1"}
        assert qps_metrics_route["app2_g"] == {"/app2"}
        assert qps_metrics_app_name["app1_f"] == "app1"
        assert qps_metrics_app_name["app2_g"] == "app2"
        qps_metrics_route, qps_metrics_app_name = self._generate_metrics_summary(
            get_metric_dictionaries("serve_deployment_error_counter")
        )
        assert qps_metrics_route["app3_h"] == {"/app3"}
        assert qps_metrics_app_name["app3_h"] == "app3"

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
                get_metric_dictionaries("serve_handle_request_counter")
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
            timeout=20,
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
            timeout=20,
        )
        counter_metrics = get_metric_dictionaries("my_counter")
        assert len(counter_metrics) == 1
        counter_metrics[0]["my_static_tag"] == "static_value"
        counter_metrics[0]["my_runtime_tag"] == "100"
        counter_metrics[0]["replica"] == replica_tag
        counter_metrics[0]["deployment"] == deployment_name
        counter_metrics[0]["application"] == "app"

        gauge_metrics = get_metric_dictionaries("my_gauge")
        assert len(counter_metrics) == 1
        gauge_metrics[0]["my_static_tag"] == "static_value"
        gauge_metrics[0]["my_runtime_tag"] == "300"
        gauge_metrics[0]["replica"] == replica_tag
        gauge_metrics[0]["deployment"] == deployment_name
        gauge_metrics[0]["application"] == "app"

        histogram_metrics = get_metric_dictionaries("my_histogram_sum")
        assert len(histogram_metrics) == 1
        histogram_metrics[0]["my_static_tag"] == "static_value"
        histogram_metrics[0]["my_runtime_tag"] == "200"
        histogram_metrics[0]["replica"] == replica_tag
        histogram_metrics[0]["deployment"] == deployment_name
        gauge_metrics[0]["application"] == "app"

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
            timeout=20,
        )
        counter_metrics = get_metric_dictionaries("my_counter")
        assert len(counter_metrics) == 1
        counter_metrics[0]["my_static_tag"] == "static_value"
        counter_metrics[0]["my_runtime_tag"] == "100"

        gauge_metrics = get_metric_dictionaries("my_gauge")
        assert len(counter_metrics) == 1
        gauge_metrics[0]["my_static_tag"] == "static_value"
        gauge_metrics[0]["my_runtime_tag"] == "300"

        histogram_metrics = get_metric_dictionaries("my_histogram_sum")
        assert len(histogram_metrics) == 1
        histogram_metrics[0]["my_static_tag"] == "static_value"
        histogram_metrics[0]["my_runtime_tag"] == "200"


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
    """Gets a list of metric's dictionaries from metrics' text output.

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

    return metric_dicts


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
