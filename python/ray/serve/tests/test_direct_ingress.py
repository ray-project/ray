import asyncio
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple
from uuid import UUID

import grpc
import httpx
import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import PlainTextResponse

import ray
from ray import serve
from ray._common.test_utils import Semaphore, SignalActor, wait_for_condition
from ray.actor import ActorHandle
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    DEFAULT_AUTOSCALING_POLICY_NAME,
    HEALTHY_MESSAGE,
    RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT,
    RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT,
    RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT,
    RAY_SERVE_DIRECT_INGRESS_PORT_RETRY_COUNT,
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
    RAY_SERVE_ENABLE_HA_PROXY,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.test_utils import (
    check_deployment_status,
    check_num_replicas_gte,
    check_num_replicas_lte,
    get_application_url,
    get_application_urls,
    ping_grpc_list_applications,
    send_signal_on_cancellation,
)
from ray.serve.autoscaling_policy import default_autoscaling_policy
from ray.serve.config import ProxyLocation
from ray.serve.context import _get_global_client
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve.schema import (
    ApplicationStatus,
    DeploymentStatus,
    RequestProtocol,
    ServeDeploySchema,
    ServeInstanceDetails,
)
from ray.serve.tests.conftest import TEST_GRPC_SERVICER_FUNCTIONS


@ray.remote
class Collector:
    def __init__(self):
        self.items = []

    def add(self, item):
        self.items.append(item)

    def get(self):
        return self.items


@pytest.fixture
def _skip_if_ff_not_enabled():
    if not RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip(
            reason="RAY_SERVE_ENABLE_DIRECT_INGRESS not set.",
        )


@pytest.fixture
def _skip_if_haproxy_enabled():
    if RAY_SERVE_ENABLE_HA_PROXY:
        pytest.skip(
            reason="RAY_SERVE_ENABLE_HA_PROXY is set.",
        )


@pytest.fixture(scope="module")
def _shared_serve_instance():
    """Module-scoped serve instance fixture."""
    # Save original env var value
    env_var_name = "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S"
    original_value = os.environ.get(env_var_name)

    if RAY_SERVE_ENABLE_HA_PROXY:
        # Setting a longer minimum draining period ensures that the client connecting
        # to the uvicorn server closes the connection first. This prevents the socket
        # used by the uvicorn server from entering the TIME_WAIT tcp state, which blocks
        # the port from being immediately reused and causes failures in subsequent tests
        # that condition on specific ports assignments.
        os.environ[env_var_name] = "6"

    ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    serve.start(
        proxy_location=ProxyLocation.HeadOnly,
        http_options={"host": "0.0.0.0"},
        grpc_options={
            "port": 9000,
            "grpc_servicer_functions": TEST_GRPC_SERVICER_FUNCTIONS,
        },
    )
    yield _get_global_client()
    # Cleanup after all tests in this module complete
    serve.shutdown()
    ray.shutdown()

    # Restore original env var value
    if original_value is not None:
        os.environ[env_var_name] = original_value
    elif env_var_name in os.environ:
        del os.environ[env_var_name]


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    # Clear all state for 2.x applications and deployments.
    _shared_serve_instance.delete_all_apps()
    # Clear the ServeHandle cache between tests to avoid them piling up.
    _shared_serve_instance.shutdown_cached_handles()


@serve.deployment(name="default-deployment")
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

    async def check_health(self):
        # Fail health check once the signal is sent, else pass.
        if self._fail_hc_signal is not None:
            try:
                await asyncio.wait_for(
                    asyncio.gather(self._fail_hc_signal.wait.remote()), timeout=0.1
                )
                raise RuntimeError("Failing health check!")
            except asyncio.TimeoutError:
                pass

    async def __del__(self):
        if self._shutdown_signal is not None:
            await self._shutdown_signal.wait.remote()

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


def get_target_groups(
    app_name: str = SERVE_DEFAULT_APP_NAME,
    from_proxy_manager: bool = False,
):
    client = _get_global_client(_health_check_controller=True)
    target_groups = ray.get(
        client._controller.get_target_groups.remote(app_name, from_proxy_manager)
    )
    return target_groups


def test_proxy_is_started_on_head_only_mode(_skip_if_ff_not_enabled, serve_instance):
    assert len(serve.status().proxies) == 1


def get_http_ports(route_prefix=None, first_only=True):
    target_groups = get_target_groups(app_name=None, from_proxy_manager=True)
    if first_only:
        http_target_group = next(
            (
                tg
                for tg in target_groups
                if tg.protocol == RequestProtocol.HTTP
                and (route_prefix is None or tg.route_prefix == route_prefix)
            )
        )
        http_targets = http_target_group.targets
        http_ports = [target.port for target in http_targets]
        return http_ports
    else:
        http_ports = []
        for target_group in target_groups:
            if target_group.protocol == RequestProtocol.HTTP and (
                route_prefix is None or target_group.route_prefix == route_prefix
            ):
                http_ports.extend([target.port for target in target_group.targets])
        return http_ports


def get_grpc_ports(route_prefix=None, first_only=True):
    target_groups = get_target_groups(app_name=None, from_proxy_manager=True)
    if first_only:
        grpc_target_group = next(
            (
                tg
                for tg in target_groups
                if tg.protocol == RequestProtocol.GRPC
                and (route_prefix is None or tg.route_prefix == route_prefix)
            )
        )
        grpc_targets = grpc_target_group.targets
        grpc_ports = [target.port for target in grpc_targets]
        return grpc_ports
    else:
        grpc_ports = []
        for target_group in target_groups:
            if target_group.protocol == RequestProtocol.GRPC and (
                route_prefix is None or target_group.route_prefix == route_prefix
            ):
                grpc_ports.extend([target.port for target in target_group.targets])
        return grpc_ports


def test_basic(_skip_if_ff_not_enabled, serve_instance):
    serve.run(Hybrid.bind(message="Hello world!"))

    http_urls = get_application_urls("HTTP")
    grpc_urls = get_application_urls("gRPC", from_proxy_manager=True)
    # Basic HTTP request.
    for http_url in http_urls:
        r = httpx.get(http_url)
        r.raise_for_status()
        assert r.text == "Hello world!"

    # Basic gRPC request.
    for grpc_url in grpc_urls:
        channel = grpc.insecure_channel(grpc_url)
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        assert stub.Method1(serve_pb2.UserDefinedMessage()).greeting == "Hello world!"
        channel.close()


def test_internal_server_error(_skip_if_ff_not_enabled, serve_instance):
    serve.run(Hybrid.bind(raise_error=True))

    http_urls = get_application_urls("HTTP")
    grpc_urls = get_application_urls("gRPC", from_proxy_manager=True)

    # Basic HTTP request.
    for http_url in http_urls:
        r = httpx.get(http_url)
        assert r.status_code == 500
        assert r.text == "Internal Server Error"

    # Basic gRPC request.
    for grpc_url in grpc_urls:
        channel = grpc.insecure_channel(grpc_url)
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        try:
            with pytest.raises(grpc.RpcError) as exception_info:
                stub.Method1(serve_pb2.UserDefinedMessage())

            rpc_error = exception_info.value
            assert rpc_error.code() == grpc.StatusCode.INTERNAL
        finally:
            # Force close the gRPC channel to ensure ports are released
            channel.close()


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
    http_urls = get_application_urls("HTTP")

    # Test GET /.
    for http_url in http_urls:
        r = httpx.get(http_url)
        r.raise_for_status()
        assert r.text == "Hello from root!"

    # Test POST /{wildcard}.
    for http_url in http_urls:
        r = httpx.post(f"{http_url}/foobar")
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
    http_url = get_application_url("HTTP")

    # Case 1: no x-request-id passed, should get populated and returned as a header.
    r = httpx.get(http_url)
    r.raise_for_status()
    assert r.text != "" and r.text == r.headers["x-request-id"]
    # This call would raise if the request ID isn't a valid UUID.
    UUID(r.text, version=4)

    # Case 2: x-request-id passed, result and header should match it.
    r = httpx.get(http_url, headers={"x-request-id": "TEST-HEADER"})
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
    # Here I am assuming that min port will always be available. But that may be true
    # since that port maybe occupied by some other parallel test. But we have no way of
    # knowing which port will be used ahead of replica initialization. May need to revisit
    # this in the future.
    http_port = RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT
    grpc_port = RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT

    def _do_grpc_hc() -> Tuple[grpc.StatusCode, str]:
        channel = grpc.insecure_channel(f"localhost:{grpc_port}")
        stub = serve_pb2_grpc.RayServeAPIServiceStub(channel)
        try:
            response, call = stub.Healthz.with_call(serve_pb2.HealthzRequest())
            return call.code(), response.message
        except grpc.RpcError as e:
            return e.code(), ""
        finally:
            channel.close()

    # Wait for replica constructor to start. The direct ingress server should not be
    # listening on the port at all yet.
    wait_for_condition(lambda: ray.get(initialize_signal.cur_num_waiters.remote()) == 1)
    for _ in range(10):
        with pytest.raises(httpx.ConnectError):
            httpx.get(f"http://localhost:{http_port}/-/healthz")

        code, _ = _do_grpc_hc()
        assert code == grpc.StatusCode.UNAVAILABLE

    def _verify_health_check(
        *,
        passing: bool,
        message: str,
    ) -> bool:
        # Check HTTP health check.
        expected_status = 200 if passing else 503
        r = httpx.get(f"http://localhost:{http_port}/-/healthz")
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
        lambda: _verify_health_check(passing=True, message=HEALTHY_MESSAGE),
    )

    # Signal the health check method to fail and verify that health checks fail.
    ray.get(fail_hc_signal.send.remote())
    wait_for_condition(
        lambda: _verify_health_check(passing=False, message="UNHEALTHY"),
    )

    # Signal the health check method to pass and verify that health checks pass.
    ray.get(fail_hc_signal.send.remote(clear=True))
    wait_for_condition(
        lambda: _verify_health_check(passing=True, message=HEALTHY_MESSAGE),
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


def test_port_retry_logic(_skip_if_ff_not_enabled, serve_instance):
    """Test that replicas retry port allocation when ports are in use."""
    import socket

    # Create a function to occupy a port
    def occupy_port(port: int, max_attempts: int = 10):
        import errno

        attempts = 0
        while attempts < max_attempts:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                sock.bind(("localhost", port))
                sock.listen(1)
                return sock
            except OSError as exc:
                sock.close()
                # If the port is already in use, try the next one; otherwise
                # re-raise unexpected errors.
                if exc.errno != errno.EADDRINUSE:
                    raise

                attempts += 1
                # backoff to wait for the port to be released
                time.sleep(0.5)

        raise RuntimeError(
            f"Unable to bind a socket after {max_attempts} attempts at port {port}."
        )

    # Start occupying the min HTTP and gRPC ports
    http_sock = occupy_port(RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT)
    grpc_sock = occupy_port(RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT)

    try:
        # Deploy an app - it should retry port allocation and eventually fall back
        # to shared ingress since we're occupying the ports
        serve.run(Hybrid.bind(message="Hello world!"))

        target_groups = get_target_groups(from_proxy_manager=True)

        # Check HTTP target group
        http_target_group = next(
            (tg for tg in target_groups if tg.protocol == RequestProtocol.HTTP)
        )
        assert (
            http_target_group.targets[0].port != RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT
        )

        # Check gRPC target group
        grpc_target_group = next(
            (tg for tg in target_groups if tg.protocol == RequestProtocol.GRPC)
        )
        assert (
            grpc_target_group.targets[0].port != RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT
        )

        http_url = get_application_url("HTTP")

        # Verify the service still works through shared ingress
        r = httpx.get(http_url)
        r.raise_for_status()
        assert r.text == "Hello world!"

    finally:
        # Clean up the sockets
        http_sock.close()
        grpc_sock.close()


def test_replica_gives_up_after_max_port_retries_for_http(
    _skip_if_ff_not_enabled, serve_instance
):
    """Test that replicas give up after max port retries."""
    import socket

    occupied_ports = []
    # TODO(sheikh): Control env variables
    for port in range(
        RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT,
        RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT
        + RAY_SERVE_DIRECT_INGRESS_PORT_RETRY_COUNT,
    ):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port))
        sock.listen(1)
        occupied_ports.append(sock)

    serve._run(Hybrid.bind(message="Hello world!"), _blocking=False)

    # wait to deployment to be DEPLOY_FAILED
    def _func():
        serve_details = ServeInstanceDetails(
            **ServeSubmissionClient("http://localhost:8265").get_serve_details()
        )
        status = (
            serve_details.applications["default"]
            .deployments["default-deployment"]
            .status
        )
        assert status == DeploymentStatus.DEPLOY_FAILED
        return True

    wait_for_condition(_func, timeout=20)

    serve.delete("default", _blocking=True)


def test_replica_gives_up_after_max_port_retries_for_grpc(
    _skip_if_ff_not_enabled, serve_instance
):
    """Test that replicas give up after max port retries."""
    import socket

    occupied_ports = []
    for port in range(
        RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT,
        RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT
        + RAY_SERVE_DIRECT_INGRESS_PORT_RETRY_COUNT,
    ):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("localhost", port))
            sock.listen(1)
        except socket.error:
            # Port may already be in use, continue to next port
            pass
        occupied_ports.append(sock)

    serve._run(Hybrid.bind(message="Hello world!"), _blocking=False)

    # wait to deployment to be DEPLOY_FAILED
    def _func():
        serve_details = ServeInstanceDetails(
            **ServeSubmissionClient("http://localhost:8265").get_serve_details()
        )
        status = (
            serve_details.applications["default"]
            .deployments["default-deployment"]
            .status
        )
        assert status == DeploymentStatus.DEPLOY_FAILED
        return True

    wait_for_condition(_func, timeout=20)

    serve.delete("default", _blocking=True)


def test_no_port_available(_skip_if_ff_not_enabled, serve_instance):
    """Test that replicas give up after max port retries."""
    import socket

    occupied_ports = []
    for port in range(
        RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT, RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT
    ):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("localhost", port))
        sock.listen(1)
        occupied_ports.append(sock)

    """Test that multiple replicas on the same node occupy unique ports."""
    serve._run(
        Hybrid.options(name="default-deployment").bind(message="Hello world!"),
        _blocking=False,
    )

    # check that the deployment failed
    def _func():
        serve_details = ServeInstanceDetails(
            **ServeSubmissionClient("http://localhost:8265").get_serve_details()
        )
        assert (
            serve_details.applications["default"]
            .deployments["default-deployment"]
            .status
            == DeploymentStatus.DEPLOY_FAILED
        )
        assert (
            serve_details.applications["default"].status
            == ApplicationStatus.DEPLOY_FAILED
        )
        return True

    wait_for_condition(_func, timeout=20)


def test_replica_releases_ports_on_shutdown(_skip_if_ff_not_enabled, serve_instance):
    """Test that replicas release ports on shutdown."""
    serve.run(Hybrid.options(num_replicas=4).bind(message="Hello world!"))

    http_ports = get_http_ports()
    grpc_ports = get_grpc_ports()
    assert set(http_ports) == {30000, 30001, 30002, 30003}
    assert set(grpc_ports) == {40000, 40001, 40002, 40003}

    assert len(http_ports) == 4
    assert len(grpc_ports) == 4

    def _is_port_in_use(port):
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return sock.connect_ex(("0.0.0.0", port)) == 0

    # Check that the ports are occupied
    for http_port in http_ports:
        assert _is_port_in_use(http_port)
    for grpc_port in grpc_ports:
        assert _is_port_in_use(grpc_port)

    # make requests to the application
    for http_port in http_ports:
        req = httpx.get(f"http://localhost:{http_port}/")
        assert req.status_code == 200
        assert req.text == "Hello world!"
    for grpc_port in grpc_ports:
        channel = grpc.insecure_channel(f"localhost:{grpc_port}")
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        assert stub.Method1(serve_pb2.UserDefinedMessage()).greeting == "Hello world!"
        channel.close()
    # Shutdown the replica
    serve.delete("default", _blocking=True)

    # Check that the ports are released
    for http_port in http_ports:
        assert not _is_port_in_use(http_port)
    for grpc_port in grpc_ports:
        assert not _is_port_in_use(grpc_port)

    # redeploy the application
    serve.run(Hybrid.options(num_replicas=4).bind(message="Hello world!"))

    http_ports = get_http_ports()
    grpc_ports = get_grpc_ports()
    assert set(http_ports) == {30000, 30001, 30002, 30003}
    assert set(grpc_ports) == {40000, 40001, 40002, 40003}

    assert len(http_ports) == 4
    assert len(grpc_ports) == 4


def test_get_serve_instance_details(_skip_if_ff_not_enabled, serve_instance):
    """Test that get_serve_instance_details returns the correct information."""
    serve.run(Hybrid.options(num_replicas=4).bind(message="Hello world!"))

    target_groups = get_target_groups(from_proxy_manager=True)
    assert len(target_groups) == 2
    assert len(target_groups[0].targets) == 4
    assert len(target_groups[1].targets) == 4
    assert target_groups[0].fallback_target is None
    assert target_groups[1].fallback_target is None


def test_only_ingress_deployment_replicas_are_used_for_target_groups(
    _skip_if_ff_not_enabled, serve_instance
):
    @serve.deployment(num_replicas=2)
    class DownstreamDeployment:
        def __init__(self):
            pass

        def __call__(self):
            return "downstream-deployment"

    @serve.deployment(num_replicas=3)
    class IngressDeployment:
        def __init__(self, downstream_deployment: DownstreamDeployment):
            self.downstream_deployment = downstream_deployment

        async def __call__(self):
            res = await self.downstream_deployment.remote()
            return f"ingress-deployment-{res}"

        async def Method1(
            self, request: serve_pb2.UserDefinedMessage
        ) -> serve_pb2.UserDefinedResponse:
            res = await self.downstream_deployment.remote()
            return serve_pb2.UserDefinedResponse(greeting=f"ingress-deployment-{res}")

    serve.run(
        IngressDeployment.options(name="ingress-deployment").bind(
            DownstreamDeployment.options(name="downstream-deployment").bind()
        )
    )

    target_groups = get_target_groups(from_proxy_manager=True)
    assert len(target_groups) == 2
    assert len(target_groups[0].targets) == 3
    assert len(target_groups[1].targets) == 3
    assert target_groups[0].fallback_target is None
    assert target_groups[1].fallback_target is None

    # test that the target groups are unique and contain the correct ports for ingress deployment
    http_ports = get_http_ports()
    grpc_ports = get_grpc_ports()
    assert len(set(http_ports) & {30000, 30001, 30002, 30003, 30004}) == 3
    assert len(set(grpc_ports) & {40000, 40001, 40002, 40003, 40004}) == 3

    http_urls = get_application_urls("HTTP")
    grpc_urls = get_application_urls("gRPC", from_proxy_manager=True)

    for http_url in http_urls:
        req = httpx.get(http_url)
        assert req.status_code == 200
        assert req.text == "ingress-deployment-downstream-deployment"

    for grpc_url in grpc_urls:
        channel = grpc.insecure_channel(grpc_url)
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        assert (
            stub.Method1(serve_pb2.UserDefinedMessage()).greeting
            == "ingress-deployment-downstream-deployment"
        )
        channel.close()


def test_crashed_replica_port_is_released_and_reused(
    _skip_if_ff_not_enabled, serve_instance
):
    """Test that crashed replica port is released and reused."""
    serve.run(Hybrid.options(num_replicas=4).bind(message="Hello world!"))

    http_ports = get_http_ports()
    grpc_ports = get_grpc_ports()
    assert set(http_ports) == {30000, 30001, 30002, 30003}
    assert set(grpc_ports) == {40000, 40001, 40002, 40003}

    # delete the application
    serve.delete("default", _blocking=True)

    # run the deployment again
    serve.run(Hybrid.options(num_replicas=4).bind(message="Hello world!"))

    new_http_ports = get_http_ports()
    new_grpc_ports = get_grpc_ports()

    assert set(http_ports) == set(new_http_ports)
    assert set(grpc_ports) == set(new_grpc_ports)

    # get pid of the replicas
    serve_details = ServeInstanceDetails(
        **ServeSubmissionClient("http://localhost:8265").get_serve_details()
    )
    replicas = (
        serve_details.applications["default"].deployments["default-deployment"].replicas
    )
    pids = [replica.pid for replica in replicas]

    # kill the replicas
    import os
    import signal

    # force kill the replicas
    os.kill(pids[0], signal.SIGKILL)
    # keyboard interrupt the replicas
    os.kill(pids[1], signal.SIGINT)
    # TODO(sheikh): Find a way to gracefully stop the replicas

    def _func():
        # get pid of the replicas
        serve_details = ServeInstanceDetails(
            **ServeSubmissionClient("http://localhost:8265").get_serve_details()
        )
        replicas = (
            serve_details.applications["default"]
            .deployments["default-deployment"]
            .replicas
        )
        new_pids = [replica.pid for replica in replicas]
        assert new_pids != pids and len(new_pids) == 4
        return True

    wait_for_condition(_func, timeout=20)

    # wait for deployment to be running
    def _func2():
        serve_details = ServeInstanceDetails(
            **ServeSubmissionClient("http://localhost:8265").get_serve_details()
        )
        assert (
            serve_details.applications["default"]
            .deployments["default-deployment"]
            .status
            == DeploymentStatus.HEALTHY
        )
        return True

    wait_for_condition(lambda: _func2(), timeout=30)

    # check that the ports are released
    after_crash_http_ports = get_http_ports()
    after_crash_grpc_ports = get_grpc_ports()

    assert len(after_crash_http_ports) == 4
    assert len(after_crash_grpc_ports) == 4

    # show that smart port selection is working even with crashed ports
    assert set(after_crash_http_ports) == set(http_ports)
    assert set(after_crash_grpc_ports) == set(grpc_ports)

    # make requests to the application
    for http_port in http_ports:
        req = httpx.get(f"http://localhost:{http_port}/")
        assert req.status_code == 200
        assert req.text == "Hello world!"
    for grpc_port in grpc_ports:
        channel = grpc.insecure_channel(f"localhost:{grpc_port}")
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        assert stub.Method1(serve_pb2.UserDefinedMessage()).greeting == "Hello world!"
        channel.close()


def test_multiple_applications_on_same_node(_skip_if_ff_not_enabled, serve_instance):
    """Test that multiple applications, such that each app has a ingress deployment"""

    @serve.deployment(num_replicas=2)
    def deployment_1():
        return "deployment-1"

    @serve.deployment(num_replicas=2)
    def deployment_2():
        return "deployment-2"

    serve.run(
        deployment_1.options(name="deployment-1").bind(),
        name="app-1",
        route_prefix="/app-1",
    )
    serve.run(
        deployment_2.options(name="deployment-2").bind(),
        name="app-2",
        route_prefix="/app-2",
    )

    http_ports_1 = get_http_ports("/app-1")
    http_ports_2 = get_http_ports("/app-2")
    grpc_ports_1 = get_grpc_ports("/app-1")
    grpc_ports_2 = get_grpc_ports("/app-2")

    assert set(http_ports_1) == {30000, 30001}
    assert set(http_ports_2) == {30002, 30003}
    assert set(grpc_ports_1) == {40000, 40001}
    assert set(grpc_ports_2) == {40002, 40003}

    # make a request to the ingress deployment
    for http_port in http_ports_1:
        req = httpx.get(f"http://localhost:{http_port}/app-1")
        assert req.status_code == 200
        assert req.text == "deployment-1"

    # make a request to the other ingress deployment
    for http_port in http_ports_2:
        req = httpx.get(f"http://localhost:{http_port}/app-2")
        assert req.status_code == 200
        assert req.text == "deployment-2"


def test_app_with_composite_deployments(_skip_if_ff_not_enabled, serve_instance):
    """Test that an app with composite deployments can be deployed. verify
    that ports are occupied by all deployments in the app but only the ingress
    deployment is used for the target groups"""

    @serve.deployment(num_replicas=3)
    class ChildDeployment:
        def __call__(self):
            return "child-deployment"

    @serve.deployment(num_replicas=2)
    class IngressDeployment:
        def __init__(self, child_deployment: ChildDeployment):
            self.child_deployment = child_deployment

        async def __call__(self):
            return await self.child_deployment.remote()

        async def Method1(
            self, request: serve_pb2.UserDefinedMessage
        ) -> serve_pb2.UserDefinedResponse:
            res = await self.child_deployment.remote()
            return serve_pb2.UserDefinedResponse(greeting=res)

    serve.run(
        IngressDeployment.options(name="ingress-deployment").bind(
            ChildDeployment.options(name="child-deployment").bind()
        ),
        name="app-1",
        route_prefix="/app-1",
    )

    # test that the target groups are unique and contain the correct ports for ingress deployment
    http_ports = get_http_ports()
    grpc_ports = get_grpc_ports()
    # difficult to say which ports are used for the target groups
    assert len(set(http_ports) & {30000, 30001, 30002, 30003, 30004}) == 2
    assert len(set(grpc_ports) & {40000, 40001, 40002, 40003, 40004}) == 2

    http_urls = get_application_urls("HTTP", app_name="app-1")
    grpc_urls = get_application_urls("gRPC", app_name="app-1", from_proxy_manager=True)

    # make a request to the ingress deployment
    for http_url in http_urls:
        req = httpx.get(http_url)
        assert req.status_code == 200
        assert req.text == "child-deployment"

    # grpc request
    for grpc_url in grpc_urls:
        channel = grpc.insecure_channel(grpc_url)
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        assert (
            stub.Method1(serve_pb2.UserDefinedMessage()).greeting == "child-deployment"
        )
        channel.close()

    def _is_port_in_use(ports):
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        for port in ports:
            if sock.connect_ex(("0.0.0.0", port)) == 0:
                return True
        return False

    # assert that child deployment is not occupying ports
    assert not _is_port_in_use(
        [30002, 30003, 30004]
    ), "Child deployment is occupying ports"
    assert not _is_port_in_use(
        [40002, 40003, 40004]
    ), "Child deployment is occupying ports"


def test_only_running_apps_are_used_for_target_groups(
    _skip_if_ff_not_enabled, serve_instance
):
    """Test that only running apps are used for target groups"""

    signal_actor = SignalActor.remote()

    @serve.deployment(num_replicas=2)
    def deployment_1():
        return "deployment-1"

    @serve.deployment(num_replicas=2)
    class Deployment2:
        async def __init__(self, signal_actor: SignalActor):
            self.signal_actor = signal_actor
            await self.signal_actor.wait.remote()

        async def __call__(self):
            return "deployment-2"

    serve.run(
        deployment_1.options(name="deployment-1").bind(),
        name="app-1",
        route_prefix="/app-1",
    )
    serve._run(
        Deployment2.options(name="deployment-2").bind(signal_actor=signal_actor),
        name="app-2",
        route_prefix="/app-2",
        _blocking=False,
    )
    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 2, timeout=10
    )
    serve_details = ServeInstanceDetails(
        **ServeSubmissionClient("http://localhost:8265").get_serve_details()
    )
    assert (
        serve_details.applications["app-2"].deployments["deployment-2"].status
        == DeploymentStatus.UPDATING
    )
    assert serve_details.applications["app-2"].status == ApplicationStatus.DEPLOYING
    assert serve_details.applications["app-1"].status == ApplicationStatus.RUNNING

    http_ports = get_http_ports(first_only=False)
    grpc_ports = get_grpc_ports(first_only=False)
    # In HAProxy mode, we don't return itself or the Serve proxy as a target yet.
    # This will change when we support scale to/from zero.
    assert set(http_ports) == (
        {30000, 30001} if RAY_SERVE_ENABLE_HA_PROXY else {30000, 30001, 8000}
    )
    assert set(grpc_ports) == (
        {40000, 40001} if RAY_SERVE_ENABLE_HA_PROXY else {40000, 40001, 9000}
    )

    ray.get(signal_actor.send.remote())

    def _func():
        serve_details = ServeInstanceDetails(
            **ServeSubmissionClient("http://localhost:8265").get_serve_details()
        )
        assert serve_details.applications["app-2"].status == ApplicationStatus.RUNNING
        return True

    wait_for_condition(_func, timeout=10)

    http_ports = get_http_ports("/app-1", first_only=False)
    grpc_ports = get_grpc_ports("/app-1", first_only=False)
    assert set(http_ports) == {30000, 30001}
    assert set(grpc_ports) == {40000, 40001}

    http_urls = get_application_urls("HTTP", app_name="app-1")

    # make requests to the application
    for http_url in http_urls:
        req = httpx.get(http_url)
        assert req.status_code == 200
        assert req.text == "deployment-1"

    http_ports = get_http_ports("/app-2", first_only=False)
    grpc_ports = get_grpc_ports("/app-2", first_only=False)
    assert set(http_ports) == {30002, 30003}
    assert set(grpc_ports) == {40002, 40003}

    http_urls = get_application_urls("HTTP", app_name="app-2")

    # make requests to the application
    for http_url in http_urls:
        req = httpx.get(http_url)
        assert req.status_code == 200
        assert req.text == "deployment-2"


def test_some_replicas_not_running(_skip_if_ff_not_enabled, serve_instance):
    signal_actor = Semaphore.remote(2)

    @serve.deployment(num_replicas=4)
    class Deployment1:
        async def __init__(self):
            await signal_actor.acquire.remote()

        def __call__(self):
            return "deployment-1"

    serve._run(
        Deployment1.options(name="deployment-1").bind(),
        name="app-1",
        route_prefix="/app-1",
        _blocking=False,
    )

    def _func():
        http_ports = get_http_ports("/app-1", first_only=False)
        grpc_ports = get_grpc_ports("/app-1", first_only=False)
        assert set(http_ports) == {30000, 30001}
        assert set(grpc_ports) == {40000, 40001}
        return True

    wait_for_condition(_func, timeout=10)

    # check status of the deployment
    serve_details = ServeInstanceDetails(
        **ServeSubmissionClient("http://localhost:8265").get_serve_details()
    )
    assert (
        serve_details.applications["app-1"].deployments["deployment-1"].status
        == DeploymentStatus.UPDATING
    )
    assert serve_details.applications["app-1"].status == ApplicationStatus.DEPLOYING


def test_port_recovery_on_controller_restart(_skip_if_ff_not_enabled, serve_instance):
    """Test that ports are recovered on controller restart."""
    client = serve_instance
    serve.run(Hybrid.options(num_replicas=4).bind(message="Hello world!"))
    pre_controller_restart_http_ports = get_http_ports()
    pre_controller_restart_grpc_ports = get_grpc_ports()

    ray.kill(client._controller, no_restart=False)

    def validate_port_recovery():
        post_controller_restart_http_ports = get_http_ports()
        post_controller_restart_grpc_ports = get_grpc_ports()

        assert set(post_controller_restart_http_ports) == set(
            pre_controller_restart_http_ports
        )
        assert set(post_controller_restart_grpc_ports) == set(
            pre_controller_restart_grpc_ports
        )
        return True

    wait_for_condition(validate_port_recovery)


class TestDirectIngressBackpressure:
    def _do_http_request(self, url: str) -> bool:
        r = httpx.get(url, timeout=10)
        if r.status_code == 200:
            return True
        elif r.status_code == 503:
            return False
        else:
            raise RuntimeError(f"Unexpected status code: {r.status_code}")

    def _do_grpc_request(self, url: str) -> bool:
        channel = grpc.insecure_channel(url)
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        try:
            stub.Method1(serve_pb2.UserDefinedMessage(), timeout=20)
            return True
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                return False

            raise RuntimeError(f"Unexpected status code: {e.code()}")
        finally:
            channel.close()

    def test_max_ongoing_requests(self, _skip_if_ff_not_enabled, serve_instance):
        wait_signal = SignalActor.remote()

        serve.run(
            Hybrid.options(max_ongoing_requests=5).bind(
                message="done waiting!", wait_signal=wait_signal
            )
        )
        http_url = get_application_url("HTTP")
        grpc_url = get_application_url("gRPC", from_proxy_manager=True)

        for _do_request in [self._do_grpc_request, self._do_http_request]:
            url = grpc_url if _do_request == self._do_grpc_request else http_url
            num_requests = 5
            with ThreadPoolExecutor(num_requests + 5) as tpe:
                # Submit `max_ongoing_requests` blocking requests.
                futures = [tpe.submit(_do_request, url) for _ in range(num_requests)]
                wait_for_condition(
                    lambda: ray.get(wait_signal.cur_num_waiters.remote())
                    == num_requests
                )
                assert all(not f.done() for f in futures)

                # Send another request beyond `max_ongoing_requests`
                queued_requests = [
                    tpe.submit(_do_request, url) for _ in range(num_requests + 5)
                ]
                wait_for_condition(
                    lambda: ray.get(wait_signal.cur_num_waiters.remote())
                    == num_requests
                )
                assert all(not f.done() for f in queued_requests)

                # Unblock the requests, check they finish successfully.
                ray.get(wait_signal.send.remote())
                assert all(f.result() is True for f in futures)
                assert all(f.result() is True for f in queued_requests)

            # Now a new request showld succeed.
            assert _do_request(url) is True

            ray.get(wait_signal.send.remote(clear=True))

    def test_backpressure_queued_requests(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test that the backpressure logic works"""
        signal = SignalActor.remote()

        serve.run(
            Hybrid.options(max_ongoing_requests=1).bind(
                message="done waiting!", wait_signal=signal
            )
        )
        http_url = get_application_url("HTTP")
        grpc_url = get_application_url("gRPC", from_proxy_manager=True)
        for _do_request in [self._do_grpc_request, self._do_http_request]:
            url = grpc_url if _do_request == self._do_grpc_request else http_url
            num_requests = 1000
            with ThreadPoolExecutor(num_requests) as tpe:
                futures = [tpe.submit(_do_request, url) for _ in range(1)]
                wait_for_condition(
                    lambda: ray.get(signal.cur_num_waiters.remote()) == 1
                )
                futures.extend(
                    [tpe.submit(_do_request, url) for _ in range(num_requests - 1)]
                )
                ray.get(signal.send.remote())
                wait_for_condition(
                    lambda: ray.get(signal.cur_num_waiters.remote()) == 0
                )
                assert sum(f.result() is True for f in futures) == num_requests

            ray.get(signal.send.remote(clear=True))

    def test_drop_after_max_queued_requests(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test that the backpressure logic works"""
        signal = SignalActor.remote()

        serve.run(
            Hybrid.options(max_ongoing_requests=10, max_queued_requests=10).bind(
                message="done waiting!", wait_signal=signal
            )
        )
        http_url = get_application_url("HTTP")
        grpc_url = get_application_url("gRPC", from_proxy_manager=True)
        for _do_request in [self._do_grpc_request, self._do_http_request]:
            url = grpc_url if _do_request == self._do_grpc_request else http_url
            num_requests = 1000
            with ThreadPoolExecutor(num_requests) as tpe:
                futures = [tpe.submit(_do_request, url) for _ in range(10)]
                wait_for_condition(
                    lambda: ray.get(signal.cur_num_waiters.remote()) == 10
                )
                futures.extend(
                    [tpe.submit(_do_request, url) for _ in range(num_requests - 10)]
                )

                def _func():
                    count = sum(
                        f.done() and f.result(timeout=0) is False for f in futures
                    )
                    assert count == num_requests - 20
                    return True

                wait_for_condition(_func, timeout=10)
                signal.send.remote()
                wait_for_condition(
                    lambda: ray.get(signal.cur_num_waiters.remote()) == 0
                )
                # assert 2 requests succeeded
                assert sum(f.result() is True for f in futures) == 20

            signal.send.remote(clear=True)

    def test_mixed_http_grpc_backpressure(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test backpressure with simultaneous HTTP and gRPC requests"""
        signal = SignalActor.remote()

        serve.run(
            Hybrid.options(max_ongoing_requests=5, max_queued_requests=5).bind(
                message="done waiting!", wait_signal=signal
            )
        )
        http_url = get_application_url("HTTP")
        grpc_url = get_application_url("gRPC", from_proxy_manager=True)

        num_requests = 500
        with ThreadPoolExecutor(num_requests) as tpe:
            # Submit mixed HTTP and gRPC requests
            http_futures = []
            grpc_futures = []
            http_futures.extend(
                [tpe.submit(self._do_http_request, http_url) for _ in range(5)]
            )
            grpc_futures.extend(
                [tpe.submit(self._do_grpc_request, grpc_url) for _ in range(5)]
            )

            # Wait for ongoing requests to block (should be 10 total across both protocols)
            wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 5)

            http_futures.extend(
                [
                    tpe.submit(self._do_http_request, http_url)
                    for _ in range((num_requests // 2) - 5)
                ]
            )
            grpc_futures.extend(
                [
                    tpe.submit(self._do_grpc_request, grpc_url)
                    for _ in range((num_requests // 2) - 5)
                ]
            )

            def _func():
                # Only check results for futures that are actually done
                http_rejected = sum(
                    f.done() and f.result(timeout=0) is False for f in http_futures
                )
                grpc_rejected = sum(
                    f.done() and f.result(timeout=0) is False for f in grpc_futures
                )
                total_rejected = http_rejected + grpc_rejected
                # Should have 10 ongoing + 10 queued = 20 allowed, so 10 rejected
                assert total_rejected == num_requests - 10
                return True

            wait_for_condition(_func, timeout=20)

            # Unblock and verify
            ray.get(signal.send.remote())

            http_successful = sum(1 for f in http_futures if f.result() is True)
            grpc_successful = sum(1 for f in grpc_futures if f.result() is True)
            total_successful = http_successful + grpc_successful

            # Should have exactly 20 successful (10 ongoing + 10 queued)
            assert total_successful == 10

    def test_health_check_during_backpressure(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test that health checks work correctly during backpressure"""
        signal = SignalActor.remote()
        fail_hc_signal = SignalActor.remote()

        serve.run(
            Hybrid.options(
                max_ongoing_requests=1, max_queued_requests=2, health_check_period_s=0.5
            ).bind(
                message="done waiting!",
                wait_signal=signal,
                fail_hc_signal=fail_hc_signal,
            )
        )
        # this is specifically checking the health check on the replica
        http_url = get_application_url("HTTP", from_proxy_manager=True)
        num_requests = 100
        with ThreadPoolExecutor(num_requests) as tpe:
            # Submit requests to create backpressure
            futures = [tpe.submit(self._do_http_request, http_url) for _ in range(1)]

            # Wait for backpressure
            wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)

            futures.extend(
                [
                    tpe.submit(self._do_http_request, http_url)
                    for _ in range(num_requests - 1)
                ]
            )

            # Health check should still pass during backpressure
            hc_response = httpx.get(f"{http_url}/-/healthz")
            assert hc_response.status_code == 200
            assert hc_response.text == HEALTHY_MESSAGE

            # Fail health check
            ray.get(fail_hc_signal.send.remote())

            # Health check should fail even during backpressure
            def _check_unhealthy():
                hc_response = httpx.get(f"{http_url}/-/healthz")
                assert hc_response.status_code == 503
                assert hc_response.text == "UNHEALTHY"
                return True

            wait_for_condition(_check_unhealthy, timeout=2)

            # Restore health check
            ray.get(fail_hc_signal.send.remote(clear=True))

            # Unblock requests
            ray.get(signal.send.remote())

            # Verify some requests succeeded
            successful = sum(1 for f in futures if f.result() is True)
            assert successful == 3

            # check remaining requests are rejected
            rejected = sum(1 for f in futures if f.done() and f.result() is False)
            assert rejected == num_requests - 3

    def test_multiple_deployment_backpressure_isolation(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test that backpressure is isolated between different deployments"""
        signal1 = SignalActor.remote()
        signal2 = SignalActor.remote()

        @serve.deployment(name="deployment-1")
        class Deployment1:
            def __init__(self, signal):
                self.signal = signal

            async def __call__(self, request):
                await self.signal.wait.remote()
                return "deployment-1"

            async def Method1(self, request):
                await self.signal.wait.remote()
                return serve_pb2.UserDefinedResponse(greeting="deployment-1")

        @serve.deployment(name="deployment-2")
        class Deployment2:
            def __init__(self, signal):
                self.signal = signal

            async def __call__(self, request):
                await self.signal.wait.remote()
                return "deployment-2"

            async def Method1(self, request):
                await self.signal.wait.remote()
                return serve_pb2.UserDefinedResponse(greeting="deployment-2")

        # Deploy with different backpressure settings
        serve.run(
            Deployment1.options(max_ongoing_requests=1, max_queued_requests=1).bind(
                signal1
            ),
            name="app-1",
            route_prefix="/app-1",
        )
        serve.run(
            Deployment2.options(max_ongoing_requests=5, max_queued_requests=5).bind(
                signal2
            ),
            name="app-2",
            route_prefix="/app-2",
        )

        http_url_1 = get_application_url("HTTP", app_name="app-1")
        http_url_2 = get_application_url("HTTP", app_name="app-2")
        grpc_url_1 = get_application_url(
            "gRPC", app_name="app-1", from_proxy_manager=True
        )
        grpc_url_2 = get_application_url(
            "gRPC", app_name="app-2", from_proxy_manager=True
        )

        for do_request in [self._do_http_request, self._do_grpc_request]:
            url1 = http_url_1 if do_request == self._do_http_request else grpc_url_1
            url2 = http_url_2 if do_request == self._do_http_request else grpc_url_2
            num_requests = 20
            with ThreadPoolExecutor(num_requests) as tpe:
                # Saturate deployment-1 (should cause backpressure)
                futures_1 = [tpe.submit(do_request, url1) for _ in range(1)]

                # Wait for both to have ongoing requests
                wait_for_condition(
                    lambda: ray.get(signal1.cur_num_waiters.remote()) == 1
                )
                futures_1.extend([tpe.submit(do_request, url1) for _ in range(9)])

                # Submit to deployment-2 (should not be affected by deployment-1's backpressure)
                futures_2 = [tpe.submit(do_request, url2) for _ in range(5)]
                wait_for_condition(
                    lambda: ray.get(signal2.cur_num_waiters.remote()) == 5
                )
                futures_2.extend([tpe.submit(do_request, url2) for _ in range(5)])

                def _func():
                    # deployment-1 should have rejected requests
                    rejected_1 = sum(
                        1
                        for f in futures_1
                        if f.done() and f.result(timeout=0) is False
                    )
                    assert rejected_1 == 8  # Most should be rejected (10 - 2 allowed)

                    # deployment-2 should not have rejected requests yet (higher limits)
                    rejected_2 = sum(
                        1
                        for f in futures_2
                        if f.done() and f.result(timeout=0) is False
                    )
                    assert rejected_2 == 0  # None should be rejected yet

                    return True

                wait_for_condition(_func, timeout=20)

                # Unblock both
                ray.get(signal1.send.remote())
                ray.get(signal2.send.remote())

                # Verify deployment-2 succeeded more than deployment-1
                successful_1 = sum(1 for f in futures_1 if f.result() is True)
                successful_2 = sum(1 for f in futures_2 if f.result() is True)

                assert successful_1 == 2  # At most 2 for deployment-1
                assert successful_2 == 10

            ray.get(signal1.send.remote(clear=True))
            ray.get(signal2.send.remote(clear=True))

    def test_backpressure_with_composite_deployments(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test backpressure with composite deployments"""
        signal = SignalActor.remote()

        @serve.deployment(max_ongoing_requests=1, max_queued_requests=2)
        class ChildDeployment:
            def __init__(self, signal):
                self.signal = signal

            async def __call__(self):
                await self.signal.wait.remote()
                return "child-deployment"

        @serve.deployment(max_ongoing_requests=1000)
        class CompositeDeployment:
            def __init__(self, child_deployment: ChildDeployment):
                self.child_deployment = child_deployment

            async def __call__(self):
                await self.child_deployment.remote()
                return "composite-deployment"

            async def Method1(self, request):
                await self.child_deployment.remote()
                return serve_pb2.UserDefinedResponse(greeting="composite-deployment")

        serve.run(
            CompositeDeployment.options(name="composite-deployment").bind(
                ChildDeployment.options(name="child-deployment").bind(signal)
            ),
            name="composite-app",
            route_prefix="/composite-app",
        )

        http_url = get_application_url("HTTP", app_name="composite-app")
        grpc_url = get_application_url(
            "gRPC", app_name="composite-app", from_proxy_manager=True
        )

        num_requests = 10
        for do_request in [self._do_http_request, self._do_grpc_request]:
            url = http_url if do_request == self._do_http_request else grpc_url
            with ThreadPoolExecutor(num_requests) as tpe:
                futures = []
                # there is a race condition in the router where if multiple requests
                # are submitted at the same time, then we could reject more requests
                # than strictly necessary. Hence we submit 1 request first and then
                # submit the rest of the requests.
                futures.append(tpe.submit(do_request, url))
                wait_for_condition(
                    lambda: ray.get(signal.cur_num_waiters.remote()) == 1
                )
                futures.extend(
                    [tpe.submit(do_request, url) for _ in range(num_requests - 1)]
                )

                def _func():
                    rejected = sum(
                        [f.done() and f.result(timeout=0) is False for f in futures]
                    )
                    assert rejected == num_requests - 3
                    return True

                wait_for_condition(_func, timeout=5)

                ray.get(signal.send.remote())

                successful = sum(1 for f in futures if f.result() is True)
                assert successful == 3

            ray.get(signal.send.remote(clear=True))

    def test_client_disconnect_during_request(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        signal = SignalActor.remote()
        collector = Collector.remote()

        @serve.deployment(max_ongoing_requests=1, max_queued_requests=10)
        class A:
            async def __call__(self):
                await signal.wait.remote()
                await collector.add.remote(
                    ray.serve.context._get_serve_request_context().request_id
                )

        serve.run(A.options(name="A").bind(), name="app-1", route_prefix="/app-1")
        http_url = get_application_url("HTTP", app_name="app-1")

        num_requests = 100
        with ThreadPoolExecutor(num_requests) as tpe:
            futures = [tpe.submit(httpx.get, http_url, timeout=0.5) for _ in range(1)]

            wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)

            futures.extend(
                [
                    tpe.submit(httpx.get, http_url, timeout=0.5)
                    for _ in range(num_requests - 1)
                ]
            )

            # wait for all futures to fail with a timeout
            def _func():
                for future in futures:
                    assert future.done()
                    try:
                        future.result()
                    except Exception as e:
                        assert isinstance(e, httpx.ReadTimeout)
                return True

            wait_for_condition(_func, timeout=10)

            ray.get(signal.send.remote())

            # check that the collector has the correct request ids
            assert len(ray.get(collector.get.remote())) == 0

    def test_graceful_shutdown_wait_loop(self, _skip_if_ff_not_enabled, serve_instance):
        """Test that the graceful shutdown wait loop works"""
        signal = SignalActor.remote()

        @serve.deployment(
            graceful_shutdown_timeout_s=20,
            graceful_shutdown_wait_loop_s=0.01,
            max_ongoing_requests=10,
            max_queued_requests=10,
        )
        class A:
            async def __call__(self):
                await signal.wait.remote()
                return "ok"

        serve.run(A.options(name="A").bind(), name="app-1", route_prefix="/app-1")
        http_url = get_application_url("HTTP", app_name="app-1")

        num_requests = 20
        with ThreadPoolExecutor(num_requests) as tpe:
            futures = [tpe.submit(httpx.get, http_url, timeout=10) for _ in range(10)]
            wait_for_condition(
                lambda: ray.get(signal.cur_num_waiters.remote()) == 10, timeout=10
            )

            # Submit the remaining requests
            futures = [tpe.submit(httpx.get, http_url, timeout=10) for _ in range(10)]

            serve.delete("app-1", _blocking=False)
            # send the signal to unblock all requests
            ray.get(signal.send.remote())

            # wait for all requests to finish
            for future in futures:
                assert future.result().status_code == 200

    def test_requests_are_not_running_serially(
        self, _skip_if_ff_not_enabled, serve_instance
    ):
        """Test that requests are processed concurrently, not serially"""

        @serve.deployment(
            max_ongoing_requests=20,
        )
        class A:
            async def __call__(self):
                await asyncio.sleep(1)
                return "ok"

        serve.run(A.options(name="A").bind(), name="app-1", route_prefix="/app-1")
        http_url = get_application_url("HTTP", app_name="app-1")

        num_requests = 20

        with ThreadPoolExecutor(num_requests) as tpe:
            futures = [
                tpe.submit(httpx.get, http_url, timeout=None)
                for _ in range(num_requests)
            ]

            def _func():
                for future in futures:
                    assert future.result().status_code == 200
                return True

            wait_for_condition(_func, timeout=5)


class TestDirectIngressAutoscaling:
    @pytest.mark.parametrize("min_replicas", [1, 2])
    def test_autoscaling_scale_up_down_basic(
        self, _skip_if_ff_not_enabled, serve_instance, min_replicas
    ):
        """Send 100 requests and check that we autoscale up, and then back down."""

        signal = SignalActor.remote()

        @serve.deployment(
            autoscaling_config={
                "metrics_interval_s": 0.1,
                "min_replicas": min_replicas,
                "max_replicas": 3,
                "look_back_period_s": 0.2,
                "downscale_delay_s": 0.5,
                "upscale_delay_s": 0,
                "target_num_ongoing_requests": 100,
            },
            # We will send over a lot of queries. This will make sure replicas are
            # killed quickly during cleanup.
            graceful_shutdown_timeout_s=1,
            max_ongoing_requests=1000,
        )
        class A:
            async def __call__(self, request: Request):
                await signal.wait.remote()
                return "ok"

        serve.run(A.options(name="A").bind(), name="app-1", route_prefix="/app-1")
        wait_for_condition(
            check_deployment_status,
            name="A",
            expected_status=DeploymentStatus.HEALTHY,
            app_name="app-1",
        )

        http_url = get_application_url("HTTP", app_name="app-1")
        # Send 100 concurrent HTTP requests
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(httpx.get, http_url, timeout=None) for _ in range(100)
            ]

            # scale up one more replica from min_replicas
            wait_for_condition(
                check_num_replicas_gte,
                name="A",
                target=min_replicas + 1,
                app_name="app-1",
            )
            signal.send.remote()

            # verify that all requests completed successfully
            for future in futures:
                assert future.result().status_code == 200

        # As the queue is drained, we should scale back down.
        wait_for_condition(
            check_num_replicas_lte, name="A", target=min_replicas, app_name="app-1"
        )

    def test_autoscaling_scale_from_and_to_zero(
        self, _skip_if_ff_not_enabled, _skip_if_haproxy_enabled, serve_instance
    ):
        signal = SignalActor.remote()

        @serve.deployment(
            autoscaling_config={
                "metrics_interval_s": 0.1,
                "min_replicas": 0,
                "max_replicas": 3,
                "look_back_period_s": 0.2,
                "downscale_delay_s": 0.5,
                "upscale_delay_s": 0,
                "target_num_ongoing_requests": 100,
            },
            # We will send over a lot of queries. This will make sure replicas are
            # killed quickly during cleanup.
            graceful_shutdown_timeout_s=1,
            max_ongoing_requests=1000,
        )
        class A:
            async def __call__(self, request: Request):
                await signal.wait.remote()
                return "ok"

        serve.run(A.options(name="A").bind(), name="app-1", route_prefix="/app-1")
        wait_for_condition(
            check_deployment_status,
            name="A",
            expected_status=DeploymentStatus.HEALTHY,
            app_name="app-1",
        )

        http_url = get_application_url("HTTP", app_name="app-1")
        # Send 100 concurrent HTTP requests
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(httpx.get, http_url, timeout=None) for _ in range(50)
            ]

            # scale up one more replica from min_replicas
            wait_for_condition(
                check_num_replicas_gte, name="A", target=1, app_name="app-1"
            )

            # now that replicas are running, check that http ports are occupied
            def _func():
                _ = get_application_url("HTTP", app_name="app-1")
                _ = get_application_url("gRPC", app_name="app-1")
                return True

            wait_for_condition(_func, timeout=10)

            signal.send.remote()

            # verify that all requests completed successfully
            for future in futures:
                assert future.result().status_code == 200

        # As the queue is drained, we should scale back down.
        wait_for_condition(check_num_replicas_lte, name="A", target=0, app_name="app-1")

        # check that http ports are released
        http_ports = get_http_ports(route_prefix="/app-1")
        assert len(http_ports) == 1
        assert http_ports[0] == 8000  # proxy port

        # check that grpc ports are released
        grpc_ports = get_grpc_ports(route_prefix="/app-1")
        assert len(grpc_ports) == 1
        assert grpc_ports[0] == 9000  # proxy port


def test_disconnect(_skip_if_ff_not_enabled, serve_instance):
    """Test gRPC client disconnect/cancellation behavior."""
    running_signal = SignalActor.remote()
    cancelled_signal = SignalActor.remote()

    @serve.deployment(name="disconnect-deployment")
    class DisconnectTest:
        async def wait_for_signal(self):
            async with send_signal_on_cancellation(cancelled_signal):
                await running_signal.wait.remote()

        async def __call__(self, request: Request):
            await self.wait_for_signal()
            return "completed"

        async def Method1(
            self, request: serve_pb2.UserDefinedMessage
        ) -> serve_pb2.UserDefinedResponse:
            await self.wait_for_signal()
            return serve_pb2.UserDefinedResponse(greeting="completed")

    serve.run(DisconnectTest.bind())

    http_url = get_application_url("HTTP")
    grpc_url = get_application_url("gRPC", from_proxy_manager=True)

    # Test gRPC cancellation
    channel = grpc.insecure_channel(grpc_url)
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)

    # Send request and wait for it to start executing
    request = serve_pb2.UserDefinedMessage()
    future = stub.Method1.future(request=request)

    # Wait for the request to start processing
    wait_for_condition(
        lambda: ray.get(running_signal.cur_num_waiters.remote()) == 1, timeout=10
    )

    # Cancel the request
    future.cancel()

    # Verify that cancellation was detected by the deployment
    ray.get(cancelled_signal.wait.remote(), timeout=5)

    # Verify the future was cancelled
    with pytest.raises(grpc.FutureCancelledError):
        future.result()

    channel.close()

    # Clean up signals
    ray.get(running_signal.send.remote(clear=True))
    ray.get(cancelled_signal.send.remote(clear=True))

    # Test HTTP cancellation
    http_url = get_application_url("HTTP")

    try:
        httpx.get(http_url, timeout=1)
    except httpx.TimeoutException:
        pass
    else:
        raise RuntimeError("Request should have been cancelled")

    wait_for_condition(
        lambda: ray.get(running_signal.cur_num_waiters.remote()) == 1, timeout=10
    )

    try:
        ray.get(cancelled_signal.wait.remote(), timeout=5)
    except ray.exceptions.GetTimeoutError:
        assert False, "Cancelled signal should have been sent"

    ray.get(running_signal.send.remote(clear=True))
    ray.get(cancelled_signal.send.remote(clear=True))


def test_context_propagation(_skip_if_ff_not_enabled, serve_instance):
    """Test that the context is propagated to the deployment"""

    @serve.deployment(name="context-propagation-deployment")
    class ContextPropagationTest:
        async def __call__(self):
            return ray.serve.context._get_serve_request_context().app_name

        async def Method1(
            self, request: serve_pb2.UserDefinedMessage
        ) -> serve_pb2.UserDefinedResponse:
            return serve_pb2.UserDefinedResponse(
                greeting=ray.serve.context._get_serve_request_context().app_name
            )

    serve.run(
        ContextPropagationTest.bind(),
        name="context-propagation-deployment",
        route_prefix="/context-propagation-deployment",
    )
    http_url = get_application_url("HTTP", app_name="context-propagation-deployment")
    response = httpx.get(http_url)
    assert response.status_code == 200
    assert response.text == "context-propagation-deployment"

    grpc_url = get_application_url(
        "gRPC", app_name="context-propagation-deployment", from_proxy_manager=True
    )
    channel = grpc.insecure_channel(grpc_url)
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage()
    future = stub.Method1.future(request=request)
    assert future.result().greeting == "context-propagation-deployment"


def test_context_propagation_with_child(_skip_if_ff_not_enabled, serve_instance):
    """Test that the context is propagated to the deployment"""

    @serve.deployment(name="child-deployment")
    class ChildDeployment:
        async def __call__(self):
            return ray.serve.context._get_serve_request_context().app_name

    @serve.deployment(name="context-propagation-deployment")
    class ContextPropagationTest:
        def __init__(self, child_deployment: ChildDeployment):
            self.child_deployment = child_deployment

        async def __call__(self):
            return await self.child_deployment.remote()

        async def Method1(
            self, request: serve_pb2.UserDefinedMessage
        ) -> serve_pb2.UserDefinedResponse:
            return serve_pb2.UserDefinedResponse(
                greeting=await self.child_deployment.remote()
            )

    serve.run(
        ContextPropagationTest.bind(ChildDeployment.bind()),
        name="context-propagation-deployment",
        route_prefix="/context-propagation-deployment",
    )
    http_url = get_application_url("HTTP", app_name="context-propagation-deployment")
    response = httpx.get(http_url)
    assert response.status_code == 200
    assert response.text == "context-propagation-deployment"

    grpc_url = get_application_url(
        "gRPC", app_name="context-propagation-deployment", from_proxy_manager=True
    )
    channel = grpc.insecure_channel(grpc_url)
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    request = serve_pb2.UserDefinedMessage()
    future = stub.Method1.future(request=request)
    assert future.result().greeting == "context-propagation-deployment"


def test_shutdown_replica_only_after_draining_requests(
    _skip_if_ff_not_enabled, serve_instance
):
    """Test that the replica is shutdown correctly when the deployment is shutdown."""
    signal = SignalActor.remote()

    # In direct ingress mode, graceful_shutdown_timeout_s is automatically bumped to
    # max(graceful_shutdown_timeout_s, RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S)
    # to give external load balancers time to deregister the replica.
    @serve.deployment(name="replica-shutdown-deployment", graceful_shutdown_timeout_s=5)
    class ReplicaShutdownTest:
        async def __call__(self):
            await signal.wait.remote()
            return "ok"

    serve.run(ReplicaShutdownTest.bind(), name="replica-shutdown-deployment")

    http_url = get_application_url("HTTP", app_name="replica-shutdown-deployment")
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(httpx.get, http_url, timeout=10) for _ in range(4)]

        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 4, timeout=10
        )

        serve.delete("replica-shutdown-deployment", _blocking=False)

        # Wait less than graceful_shutdown_timeout_s to ensure requests can complete
        time.sleep(0.5)

        ray.get(signal.send.remote(clear=True))

        for future in futures:
            assert future.result().status_code == 200

    wait_for_condition(
        lambda: "replica-shutdown-deployment" not in serve.status().applications,
        timeout=10,
    )


def test_http_routes_endpoint(_skip_if_ff_not_enabled, serve_instance):
    """Test that the routes endpoint returns pair of routes_prefix and
    app_name of which the replica is serving for.
    """

    @serve.deployment
    class D1:
        def __call__(self, *args):
            return "D1"

    @serve.deployment
    class D2:
        def __call__(self, *args):
            return "D2"

    serve.run(D1.bind(), name="app1", route_prefix="/D1")
    serve.run(D2.bind(), name="app2", route_prefix="/hello/world")

    # Test routes endpoint on the replica running for app1 directly
    url = get_application_url(
        app_name="app1",
        exclude_route_prefix=True,
        from_proxy_manager=True,
    )
    routes = httpx.get(f"{url}/-/routes").json()
    assert routes == {"/D1": "app1"}, routes

    # Test routes endpoint on the replica running for app2 directly
    url = get_application_url(
        app_name="app2",
        exclude_route_prefix=True,
        from_proxy_manager=True,
    )
    routes = httpx.get(f"{url}/-/routes").json()
    assert routes == {"/hello/world": "app2"}, routes

    # Test routes endpoint on the proxy
    url = "http://localhost:8000/-/routes"
    routes = httpx.get(url).json()
    assert routes == {"/D1": "app1", "/hello/world": "app2"}, routes


# TODO: haproxy doesn't support gRPC ListApplications yet so skipping this test
def test_grpc_list_applications_endpoint(
    _skip_if_ff_not_enabled, _skip_if_haproxy_enabled, serve_instance
):
    """Each replica's gRPC `ListApplications` method should only report the
    single application that replica is serving.
    """

    @serve.deployment
    class D1:
        def __call__(self, *args):
            return "D1"

    @serve.deployment
    class D2:
        def __call__(self, *args):
            return "D2"

    serve.run(D1.bind(), name="app1", route_prefix="/D1")
    serve.run(D2.bind(), name="app2", route_prefix="/hello/world")

    # Test gRPC `ListApplications` on the replica running for app1 directly
    url = get_application_url("gRPC", app_name="app1")
    channel = grpc.insecure_channel(url)
    try:
        ping_grpc_list_applications(channel, ["app1"])
    finally:
        channel.close()

    # Test gRPC `ListApplications` on the replica running for app2 directly
    url = get_application_url("gRPC", app_name="app2")
    channel = grpc.insecure_channel(url)
    try:
        ping_grpc_list_applications(channel, ["app2"])
    finally:
        channel.close()

    # Test gRPC `ListApplications` on the proxy
    channel = grpc.insecure_channel("localhost:9000")
    try:
        ping_grpc_list_applications(channel, ["app1", "app2"])
    finally:
        channel.close()


# Copied from test_controller.py
def test_redeploy_start_time(_skip_if_ff_not_enabled, serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = _get_global_client()._controller

    @serve.deployment
    def test(_):
        return "1"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test", SERVE_DEFAULT_APP_NAME))
    )
    deployment_info_1 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test", SERVE_DEFAULT_APP_NAME))
    )
    deployment_info_2 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


# Copied from test_controller.py
def test_deploy_app_custom_exception(_skip_if_ff_not_enabled, serve_instance):
    """Check that controller doesn't deserialize an exception from deploy_app."""

    controller = _get_global_client()._controller

    config = {
        "applications": [
            {
                "name": "broken_app",
                "route_prefix": "/broken",
                "import_path": "ray.serve.tests.test_config_files.broken_app:app",
            }
        ]
    }

    ray.get(controller.apply_config.remote(config=ServeDeploySchema.parse_obj(config)))

    def check_custom_exception() -> bool:
        status = serve.status().applications["broken_app"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        assert "custom exception info" in status.message
        return True

    wait_for_condition(check_custom_exception, timeout=10)


# Copied from test_controller.py
@pytest.mark.parametrize(
    "policy_name", [None, DEFAULT_AUTOSCALING_POLICY_NAME, default_autoscaling_policy]
)
def test_get_serve_instance_details_json_serializable(
    _skip_if_ff_not_enabled, serve_instance, policy_name
):
    """Test the result from get_serve_instance_details is json serializable."""

    controller = _get_global_client()._controller

    autoscaling_config = {
        "min_replicas": 1,
        "max_replicas": 10,
        "_policy": {"name": policy_name},
    }
    if policy_name is None:
        autoscaling_config.pop("_policy")

    @serve.deployment(autoscaling_config=autoscaling_config)
    def autoscaling_app():
        return "1"

    serve.run(autoscaling_app.bind())
    details = ray.get(controller.get_serve_instance_details.remote())
    details_json = json.dumps(details)
    controller_details = ray.get(controller.get_actor_details.remote())
    node_id = controller_details.node_id
    node_ip = controller_details.node_ip
    node_instance_id = controller_details.node_instance_id
    proxy_details = ray.get(controller.get_proxy_details.remote(node_id=node_id))
    deployment_timestamp = ray.get(
        controller.get_deployment_timestamps.remote(app_name="default")
    )
    deployment_details = ray.get(
        controller.get_deployment_details.remote("default", "autoscaling_app")
    )
    replica = deployment_details.replicas[0]

    expected_json = json.dumps(
        {
            "controller_info": {
                "node_id": node_id,
                "node_ip": node_ip,
                "node_instance_id": node_instance_id,
                "actor_id": controller_details.actor_id,
                "actor_name": controller_details.actor_name,
                "worker_id": controller_details.worker_id,
                "log_file_path": controller_details.log_file_path,
            },
            "proxy_location": "HeadOnly",
            "http_options": {"host": "0.0.0.0"},
            "grpc_options": {
                "port": 9000,
                "grpc_servicer_functions": TEST_GRPC_SERVICER_FUNCTIONS,
            },
            "proxies": {
                node_id: {
                    "node_id": node_id,
                    "node_ip": node_ip,
                    "node_instance_id": node_instance_id,
                    "actor_id": proxy_details.actor_id,
                    "actor_name": proxy_details.actor_name,
                    "worker_id": proxy_details.worker_id,
                    "log_file_path": proxy_details.log_file_path,
                    "status": proxy_details.status,
                }
            },
            "applications": {
                "default": {
                    "name": "default",
                    "route_prefix": "/",
                    "docs_path": None,
                    "status": "RUNNING",
                    "message": "",
                    "last_deployed_time_s": deployment_timestamp,
                    "deployed_app_config": None,
                    "source": "imperative",
                    "deployments": {
                        "autoscaling_app": {
                            "name": "autoscaling_app",
                            "status": "HEALTHY",
                            "status_trigger": "CONFIG_UPDATE_COMPLETED",
                            "message": "",
                            "deployment_config": {
                                "name": "autoscaling_app",
                                "max_ongoing_requests": 5,
                                "max_queued_requests": -1,
                                "user_config": None,
                                "autoscaling_config": {
                                    "min_replicas": 1,
                                    "initial_replicas": None,
                                    "max_replicas": 10,
                                    "target_ongoing_requests": 2.0,
                                    "metrics_interval_s": 10.0,
                                    "look_back_period_s": 30.0,
                                    "smoothing_factor": 1.0,
                                    "upscale_smoothing_factor": None,
                                    "downscale_smoothing_factor": None,
                                    "upscaling_factor": None,
                                    "downscaling_factor": None,
                                    "downscale_delay_s": 600.0,
                                    "downscale_to_zero_delay_s": None,
                                    "upscale_delay_s": 30.0,
                                    "aggregation_function": "mean",
                                    "policy": {
                                        "policy_function": "ray.serve.autoscaling_policy:default_autoscaling_policy",
                                        "policy_kwargs": {},
                                    },
                                },
                                "graceful_shutdown_wait_loop_s": 2.0,
                                "graceful_shutdown_timeout_s": 20.0,
                                "health_check_period_s": 10.0,
                                "health_check_timeout_s": 30.0,
                                "ray_actor_options": {
                                    "num_cpus": 1.0,
                                },
                                "request_router_config": {
                                    "request_router_class": "ray.serve._private.request_router:PowerOfTwoChoicesRequestRouter",
                                    "request_router_kwargs": {},
                                    "request_routing_stats_period_s": 10.0,
                                    "request_routing_stats_timeout_s": 30.0,
                                },
                            },
                            "target_num_replicas": 1,
                            "required_resources": {"CPU": 1},
                            "replicas": [
                                {
                                    "node_id": node_id,
                                    "node_ip": node_ip,
                                    "node_instance_id": node_instance_id,
                                    "actor_id": replica.actor_id,
                                    "actor_name": replica.actor_name,
                                    "worker_id": replica.worker_id,
                                    "log_file_path": replica.log_file_path,
                                    "replica_id": replica.replica_id,
                                    "state": "RUNNING",
                                    "pid": replica.pid,
                                    "start_time_s": replica.start_time_s,
                                }
                            ],
                        }
                    },
                    "external_scaler_enabled": False,
                    "deployment_topology": {
                        "app_name": "default",
                        "nodes": {
                            "autoscaling_app": {
                                "name": "autoscaling_app",
                                "app_name": "default",
                                "outbound_deployments": [],
                                "is_ingress": True,
                            },
                        },
                        "ingress_deployment": "autoscaling_app",
                    },
                }
            },
            "target_capacity": None,
            "target_groups": [
                {
                    "targets": [
                        {
                            "ip": node_ip,
                            "port": 8000 if RAY_SERVE_ENABLE_HA_PROXY else 30000,
                            "instance_id": node_instance_id,
                            "name": proxy_details.actor_name
                            if RAY_SERVE_ENABLE_HA_PROXY
                            else replica.actor_name,
                        },
                    ],
                    "route_prefix": "/",
                    "protocol": "HTTP",
                    "app_name": "" if RAY_SERVE_ENABLE_HA_PROXY else "default",
                    "fallback_target": None,
                },
                {
                    "targets": [
                        {
                            "ip": node_ip,
                            "port": 9000 if RAY_SERVE_ENABLE_HA_PROXY else 40000,
                            "instance_id": node_instance_id,
                            "name": proxy_details.actor_name
                            if RAY_SERVE_ENABLE_HA_PROXY
                            else replica.actor_name,
                        },
                    ],
                    "route_prefix": "/",
                    "protocol": "gRPC",
                    "app_name": "" if RAY_SERVE_ENABLE_HA_PROXY else "default",
                    "fallback_target": None,
                },
            ],
        }
    )
    assert details_json == expected_json

    # ensure internal field, serialized_policy_def, is not exposed
    application = details["applications"]["default"]
    deployment = application["deployments"]["autoscaling_app"]
    autoscaling_config = deployment["deployment_config"]["autoscaling_config"]
    assert "_serialized_policy_def" not in autoscaling_config


# Copied from test_controller.py
def test_get_deployment_config(_skip_if_ff_not_enabled, serve_instance):
    """Test getting deployment config."""

    controller = _get_global_client()._controller
    deployment_id = DeploymentID(name="App", app_name="default")
    deployment_config = ray.get(
        controller.get_deployment_config.remote(deployment_id=deployment_id)
    )
    # Before any deployment is created, the config should be None.
    assert deployment_config is None

    @serve.deployment
    class App:
        pass

    serve.run(App.bind())

    deployment_config = ray.get(
        controller.get_deployment_config.remote(deployment_id=deployment_id)
    )
    # After the deployment is created, the config should be DeploymentConfig.
    assert isinstance(deployment_config, DeploymentConfig)


def test_stuck_requests_are_force_killed(_skip_if_ff_not_enabled, serve_instance):
    """This test is really slow, because it waits for the ports to be released from TIME_WAIT state.
    The ports are in TIME_WAIT state because the replicas are force-killed and the ports are not
    released immediately."""
    import socket

    def _can_bind_to_port(port):
        """Check if we can bind to the port (not just if nothing is listening)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", port))
            sock.close()
            return True
        except OSError:
            sock.close()
            return False

    signal = SignalActor.remote()

    @serve.deployment(
        name="stuck-requests-deployment",
        graceful_shutdown_timeout_s=1,
    )
    class StuckRequestsTest:
        async def __call__(self):
            # This request will never complete - it waits forever
            await signal.wait.remote()
            return "ok"

    serve.run(
        StuckRequestsTest.bind(),
        name="stuck-requests-deployment",
        route_prefix="/stuck-requests-deployment",
    )

    # Collect all ports used by the application before deleting it
    http_ports = get_http_ports(route_prefix="/stuck-requests-deployment")
    grpc_ports = get_grpc_ports(route_prefix="/stuck-requests-deployment")

    http_url = get_application_url("HTTP", app_name="stuck-requests-deployment")

    with ThreadPoolExecutor() as executor:
        # Send requests that will hang forever (signal is never sent)
        futures = [executor.submit(httpx.get, http_url, timeout=60) for _ in range(2)]

        # Wait for requests to be received by the replica
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 2, timeout=10
        )

        # Delete the deployment - requests are still stuck
        serve.delete("stuck-requests-deployment", _blocking=False)

        # Verify the application is eventually deleted (replica was force-killed).
        # This should complete within graceful_shutdown_timeout_s (35s) + buffer.
        wait_for_condition(
            lambda: "stuck-requests-deployment" not in serve.status().applications,
            timeout=10,
        )

        # The stuck requests should fail (connection closed or similar)
        for future in futures:
            try:
                result = future.result(timeout=5)
                # If we get a response, it should be an error (not 200)
                assert result.status_code != 200
            except Exception:
                # Expected - request failed due to force-kill
                pass

    # Wait until all ports can be bound (not just until nothing is listening).
    # This ensures the ports are fully released from TIME_WAIT state.
    def all_ports_can_be_bound():
        for port in http_ports + grpc_ports:
            if not _can_bind_to_port(port):
                return False
        return True

    # TIME_WAIT can last up to 60s on Linux, so use a generous timeout
    wait_for_condition(all_ports_can_be_bound, timeout=120)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
