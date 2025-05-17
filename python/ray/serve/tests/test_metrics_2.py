import http
import sys
import threading

import grpc
import pytest
import requests
from fastapi import FastAPI, WebSocket
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

import ray
from ray import serve
from ray._private.test_utils import (
    SignalActor,
    wait_for_condition,
)
from ray.serve._private.test_utils import (
    ping_grpc_call_method,
)
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.tests.test_metrics_1 import get_metric_dictionaries


@pytest.fixture
def serve_start_shutdown(request):
    serve.shutdown()
    ray.shutdown()
    ray._private.utils.reset_ray_address()

    param = request.param if hasattr(request, "param") else None
    request_timeout_s = param if param else None
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""
    ray.init(
        _metrics_export_port=9999,
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


def test_proxy_disconnect_metrics(serve_start_shutdown):
    """Test that disconnect metrics are reported correctly."""

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
    conn = http.client.HTTPConnection("127.0.0.1", 8000)
    conn.request("GET", "/disconnect")
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
    )
    conn.close()  # Forcefully close the connection
    ray.get(signal.send.remote(clear=True))

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

    num_errors = get_metric_dictionaries("serve_num_http_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "/disconnect"
    assert num_errors[0]["error_code"] == "499"
    assert num_errors[0]["method"] == "GET"
    assert num_errors[0]["application"] == "disconnect"

    num_errors = get_metric_dictionaries("serve_num_grpc_error_requests")
    assert len(num_errors) == 1
    assert num_errors[0]["route"] == "disconnect"
    assert num_errors[0]["error_code"] == str(grpc.StatusCode.CANCELLED)
    assert num_errors[0]["method"] == "/ray.serve.UserDefinedService/__call__"
    assert num_errors[0]["application"] == "disconnect"


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
    _ = requests.get(correct_url).text
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
def test_proxy_metrics_http_status_code_is_error(serve_start_shutdown):
    """Verify that 2xx and 3xx status codes aren't errors, others are."""

    def check_request_count_metrics(
        expected_error_count: int,
        expected_success_count: int,
    ):
        resp = requests.get("http://127.0.0.1:9999").text
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

    # 200 is not an error.
    r = requests.get("http://127.0.0.1:8000/", data=b"200")
    assert r.status_code == 200
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=1,
    )

    # 2xx is not an error.
    r = requests.get("http://127.0.0.1:8000/", data=b"250")
    assert r.status_code == 250
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=2,
    )

    # 3xx is not an error.
    r = requests.get("http://127.0.0.1:8000/", data=b"300")
    assert r.status_code == 300
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=0,
        expected_success_count=3,
    )

    # 4xx is an error.
    r = requests.get("http://127.0.0.1:8000/", data=b"400")
    assert r.status_code == 400
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=1,
        expected_success_count=4,
    )

    # 5xx is an error.
    r = requests.get("http://127.0.0.1:8000/", data=b"500")
    assert r.status_code == 500
    wait_for_condition(
        check_request_count_metrics,
        expected_error_count=2,
        expected_success_count=5,
    )


def test_proxy_metrics_websocket_status_code_is_error(serve_start_shutdown):
    """Verify that status codes aisde from 1000 or 1001 are errors."""

    def check_request_count_metrics(
        expected_error_count: int,
        expected_success_count: int,
    ):
        resp = requests.get("http://127.0.0.1:9999").text
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
