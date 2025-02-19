import grpc
import pytest
import requests
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple
from uuid import UUID

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import PlainTextResponse

import ray
from ray import serve
from ray.actor import ActorHandle
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.anyscale.serve._private.constants import (
    ANYSCALE_RAY_SERVE_ENABLE_DIRECT_INGRESS,
)


@pytest.fixture
def _skip_if_ff_not_enabled():
    if not ANYSCALE_RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip(
            reason="ANYSCALE_RAY_SERVE_ENABLE_DIRECT_INGRESS not set.",
        )


@serve.deployment
class Hybrid:
    def __init__(
        self,
        *,
        message: str = "",
        raise_error: bool = False,
        wait_signal: Optional[ActorHandle] = None,
        fail_hc_signal: Optional[ActorHandle] = None,
        shutdown_signal: Optional[ActorHandle] = None,
        initialize_signal: Optional[ActorHandle] = None,
    ):
        self._message = message
        self._raise_error = raise_error
        self._wait_signal = wait_signal
        self._fail_hc_signal = fail_hc_signal
        self._shutdown_signal = shutdown_signal

        if initialize_signal is not None:
            ray.get(initialize_signal.wait.remote())

    def check_health(self):
        # Fail health check once the signal is sent, else pass.
        if self._fail_hc_signal is not None:
            obj_ref = self._fail_hc_signal.wait.remote()
            ready, _ = ray.wait([obj_ref], timeout=0.1)
            if len(ready) == 1:
                raise RuntimeError("Failing health check!")

    def __del__(self):
        if self._shutdown_signal is not None:
            ray.get(self._shutdown_signal.wait.remote())

    async def __call__(self, request: Request):
        if self._raise_error:
            raise RuntimeError("oops!")

        if self._wait_signal:
            await self._wait_signal.wait.remote()

        return self._message

    async def Method1(
        self, request: serve_pb2.UserDefinedMessage
    ) -> serve_pb2.UserDefinedResponse:
        if self._raise_error:
            raise RuntimeError("oops!")

        if self._wait_signal:
            await self._wait_signal.wait.remote()

        return serve_pb2.UserDefinedResponse(greeting=self._message)


def test_no_proxies_are_started(_skip_if_ff_not_enabled, serve_instance):
    assert len(serve.status().proxies) == 0


def test_basic(_skip_if_ff_not_enabled, serve_instance):
    serve.run(Hybrid.bind(message="Hello world!"))

    # Basic HTTP request.
    r = requests.get("http://localhost:8000/")
    r.raise_for_status()
    assert r.text == "Hello world!"

    # Basic gRPC request.
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    assert stub.Method1(serve_pb2.UserDefinedMessage()).greeting == "Hello world!"


def test_internal_server_error(_skip_if_ff_not_enabled, serve_instance):
    serve.run(Hybrid.bind(raise_error=True))

    # Basic HTTP request.
    r = requests.get("http://localhost:8000/")
    assert r.status_code == 500
    assert r.text == "Internal Server Error"

    # Basic gRPC request.
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    with pytest.raises(grpc.RpcError) as exception_info:
        stub.Method1(serve_pb2.UserDefinedMessage())

    rpc_error = exception_info.value
    assert rpc_error.code() == grpc.StatusCode.UNKNOWN


def test_fastapi_app(_skip_if_ff_not_enabled, serve_instance):
    fastapi_app = FastAPI()

    @serve.deployment
    @serve.ingress(fastapi_app)
    class FastAPIDeployment:
        @fastapi_app.get("/")
        def root(self) -> PlainTextResponse:
            return PlainTextResponse("Hello from root!")

        @fastapi_app.post("/{wildcard}")
        def post(self, wildcard: str) -> PlainTextResponse:
            return PlainTextResponse(
                f"Hello from {wildcard}!",
                status_code=201,
            )

    serve.run(FastAPIDeployment.bind())

    # Test GET /.
    r = requests.get("http://localhost:8000/")
    r.raise_for_status()
    assert r.text == "Hello from root!"

    # Test POST /{wildcard}.
    r = requests.post("http://localhost:8000/foobar")
    assert r.status_code == 201
    assert r.text == "Hello from foobar!"


@pytest.mark.parametrize("use_fastapi", [False, True])
def test_http_request_id(_skip_if_ff_not_enabled, serve_instance, use_fastapi: bool):
    if use_fastapi:
        fastapi_app = FastAPI()

        @serve.deployment
        @serve.ingress(fastapi_app)
        class EchoRequestID:
            @fastapi_app.get("/")
            async def root(self, request: Request) -> PlainTextResponse:
                return PlainTextResponse(request.headers.get("x-request-id", ""))

    else:

        @serve.deployment
        class EchoRequestID:
            async def __call__(self, request: Request) -> str:
                return PlainTextResponse(request.headers.get("x-request-id", ""))

    serve.run(EchoRequestID.bind())

    # Case 1: no x-request-id passed, should get populated and returned as a header.
    r = requests.get("http://localhost:8000/")
    r.raise_for_status()
    assert r.text != "" and r.text == r.headers["x-request-id"]
    # This call would raise if the request ID isn't a valid UUID.
    UUID(r.text, version=4)

    # Case 2: x-request-id passed, result and header should match it.
    r = requests.get("http://localhost:8000/", headers={"x-request-id": "TEST-HEADER"})
    r.raise_for_status()
    assert r.text == "TEST-HEADER" and r.text == r.headers["x-request-id"]


def test_grpc_request_id(_skip_if_ff_not_enabled, serve_instance):
    pytest.skip("TODO: duplicate HTTP tests for gRPC")


def test_multiplexed_model_id(_skip_if_ff_not_enabled, serve_instance):
    pytest.skip("TODO: test that sends a MM ID and checks that it's set correctly")


def test_health_check(_skip_if_ff_not_enabled, serve_instance):
    wait_signal = SignalActor.remote()
    fail_hc_signal = SignalActor.remote()
    shutdown_signal = SignalActor.remote()
    initialize_signal = SignalActor.remote()

    # Use private `_run` API so we can test the behavior before replicas initialize.
    serve._run(
        # Set a high health check period so we have time to check behavior before the
        # controller restarts the replica.
        Hybrid.options(health_check_period_s=1).bind(
            wait_signal=wait_signal,
            fail_hc_signal=fail_hc_signal,
            shutdown_signal=shutdown_signal,
            initialize_signal=initialize_signal,
        ),
        _blocking=False,
    )

    def _do_grpc_hc() -> Tuple[grpc.StatusCode, str]:
        channel = grpc.insecure_channel("localhost:9000")
        stub = serve_pb2_grpc.RayServeAPIServiceStub(channel)
        try:
            response, call = stub.Healthz.with_call(serve_pb2.HealthzRequest())
            return call.code(), response.message
        except grpc.RpcError as e:
            return e.code(), ""

    # Wait for replica constructor to start. The direct ingress server should not be
    # listening on the port at all yet.
    wait_for_condition(lambda: ray.get(initialize_signal.cur_num_waiters.remote()) == 1)
    for _ in range(10):
        with pytest.raises(requests.ConnectionError):
            requests.get("http://localhost:8000/-/healthz")

        code, _ = _do_grpc_hc()
        assert code == grpc.StatusCode.UNAVAILABLE

    def _verify_health_check(
        *,
        passing: bool,
        message: str,
    ) -> bool:
        # Check HTTP health check.
        expected_status = 200 if passing else 503
        r = requests.get("http://localhost:8000/-/healthz")
        assert r.status_code == expected_status
        assert r.text == message

        # Check gRPC health check.
        expected_code = grpc.StatusCode.OK if passing else grpc.StatusCode.UNAVAILABLE
        code, response_message = _do_grpc_hc()
        assert code == expected_code
        # NOTE(edoakes): we can't access the response message if the gRPC call fails
        # due to StatusCode.UNAVAILABLE.
        if passing:
            assert response_message == message

        return True

    # Signal the constructor to finish and verify that health checks start to pass.
    ray.get(initialize_signal.send.remote())
    wait_for_condition(
        lambda: _verify_health_check(passing=True, message="OK"),
    )

    # Signal the health check method to fail and verify that health checks fail.
    ray.get(fail_hc_signal.send.remote())
    wait_for_condition(
        lambda: _verify_health_check(passing=False, message="UNHEALTHY"),
    )

    # Signal the health check method to pass and verify that health checks pass.
    ray.get(fail_hc_signal.send.remote(clear=True))
    wait_for_condition(
        lambda: _verify_health_check(passing=True, message="OK"),
    )

    # Initiate graceful shutdown and verify that health checks fail.
    serve.delete("default", _blocking=False)
    wait_for_condition(
        lambda: ray.get(shutdown_signal.cur_num_waiters.remote()) == 1,
    )
    for _ in range(10):
        assert _verify_health_check(passing=False, message="DRAINING")

    ray.get(shutdown_signal.send.remote())
    wait_for_condition(
        lambda: len(serve.status().applications) == 0,
    )


def test_max_ongoing_requests(_skip_if_ff_not_enabled, serve_instance):
    wait_signal = SignalActor.remote()

    serve.run(
        Hybrid.options(max_ongoing_requests=5).bind(
            message="done waiting!", wait_signal=wait_signal
        )
    )

    def _do_http_request() -> bool:
        r = requests.get("http://localhost:8000/")
        if r.status_code == 200:
            return True
        elif r.status_code == 503:
            return False
        else:
            raise RuntimeError(f"Unexpected status code: {r.status_code}")

    def _do_grpc_request() -> bool:
        channel = grpc.insecure_channel("localhost:9000")
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

        try:
            stub.Method1(serve_pb2.UserDefinedMessage())
            return True
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                return False

            raise RuntimeError(f"Unexpected status code: {e.code()}")

    for _do_request in [_do_grpc_request, _do_http_request]:
        with ThreadPoolExecutor() as tpe:
            # Submit `max_ongoing_requests` blocking requests.
            futures = [tpe.submit(_do_request) for _ in range(5)]
            wait_for_condition(
                lambda: ray.get(wait_signal.cur_num_waiters.remote()) == 5
            )
            assert all(not f.done() for f in futures)

            # Send another request beyond `max_ongoing_requests`, should error.
            assert _do_request() is False

            # Unblock the requests, check they finish successfully.
            ray.get(wait_signal.send.remote())
            assert all(f.result() is True for f in futures)

        # Now a new request should succeed.
        assert _do_request() is True

        ray.get(wait_signal.send.remote(clear=True))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
