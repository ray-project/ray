import asyncio
import pickle
import sys
from typing import Dict, List, Tuple
from unittest.mock import AsyncMock

import grpc
import pytest

from ray.serve._private.common import DeploymentID, EndpointInfo, RequestMetadata
from ray.serve._private.constants import HEALTHY_MESSAGE
from ray.serve._private.proxy import (
    DRAINING_MESSAGE,
    HTTPProxy,
    ResponseGenerator,
    ResponseStatus,
    gRPCProxy,
)
from ray.serve._private.proxy_request_response import ProxyRequest
from ray.serve._private.proxy_router import (
    NO_REPLICAS_MESSAGE,
    NO_ROUTES_MESSAGE,
    ProxyRouter,
)
from ray.serve._private.test_utils import FakeGrpcContext, MockDeploymentHandle
from ray.serve.generated import serve_pb2
from ray.serve.grpc_util import RayServegRPCContext

ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE = "Router is not ready for traffic"


class FakeRef:
    def __init__(self, messages=()):
        self.called = False
        self.messages = messages

    def _on_completed(self, func):
        pass

    def is_nil(self):
        return False

    def __await__(self):
        future = asyncio.Future()
        future.set_result(self)
        result = yield from future
        if self.called:
            return pickle.dumps(self.messages)
        self.called = True
        return result

    def _to_object_ref(self, *args, **kwargs):
        return self

    def cancel(self):
        pass


class FakeGrpcHandle:
    def __init__(self, streaming: bool, grpc_context: RayServegRPCContext):
        self.deployment_id = DeploymentID(
            name="fake_deployment_name", app_name="fake_app_name"
        )
        self.streaming = streaming
        self.grpc_context = grpc_context

    async def remote(self, *args, **kwargs):
        def unary_call():
            return "hello world"

        def streaming_call():
            for i in range(10):
                yield f"hello world: {i}"

        return (
            self.grpc_context,
            unary_call() if not self.streaming else streaming_call(),
        )

    def options(self, *args, **kwargs):
        return self

    @property
    def deployment_name(self) -> str:
        return self.deployment_id.name

    @property
    def app_name(self) -> str:
        return self.deployment_id.app_name


class FakeProxyRouter(ProxyRouter):
    def __init__(self, *args, **kwargs):
        self.route = None
        self.handle = None
        self.app_is_cross_language = None
        self._ready_for_traffic = False
        self.route_patterns = {}
        self._route_pattern_apps = {}

    def update_routes(self, endpoints: Dict[DeploymentID, EndpointInfo]):
        self.endpoints = endpoints

    def get_handle_for_endpoint(self, *args, **kwargs):
        if (
            self.route is None
            and self.handle is None
            and self.app_is_cross_language is None
        ):
            return None

        return self.route, self.handle, self.app_is_cross_language

    def match_route(self, *args, **kwargs):
        if (
            self.route is None
            and self.handle is None
            and self.app_is_cross_language is None
        ):
            return None

        return self.route, self.handle, self.app_is_cross_language

    def set_ready_for_traffic(self):
        self._ready_for_traffic = True

    def ready_for_traffic(self, is_head: bool) -> bool:
        if self._ready_for_traffic:
            return True, ""

        return False, ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE


class FakeProxyRequest(ProxyRequest):
    def __init__(
        self,
        app_name: str = "",
        method: str = "",
        request_type: str = "",
        path: str = "",
        route_path: str = "",
        is_health_request: bool = False,
        is_route_request: bool = False,
    ):
        self._app_name = app_name
        self._method = method
        self._request_type = request_type
        self._path = path
        self._route_path = route_path
        self._is_route_request = is_route_request
        self._is_health_request = is_health_request

    @property
    def app_name(self) -> str:
        return self._app_name

    @property
    def method(self) -> str:
        return self._method

    @property
    def request_type(self) -> str:
        return self._request_type

    @property
    def path(self) -> str:
        return self._path

    @property
    def route_path(self) -> str:
        return self._route_path

    @property
    def is_route_request(self) -> bool:
        return self._is_route_request

    @property
    def is_health_request(self) -> bool:
        return self._is_health_request


class FakeHTTPHandle:
    def __init__(self, messages):
        self.deployment_id = DeploymentID(
            name="fake_deployment_name", app_name="fake_app_name"
        )
        self.messages = messages

    async def remote(self, *args, **kwargs):
        return pickle.dumps(self.messages)

    def options(self, *args, **kwargs):
        return self

    @property
    def deployment_name(self) -> str:
        return self.deployment_id.name

    @property
    def app_name(self) -> str:
        return self.deployment_id.app_name


class FakeHttpReceive:
    def __init__(self, messages=None):
        self.messages = messages or []

    async def __call__(self):
        while True:
            if self.messages:
                return self.messages.pop()
            await asyncio.sleep(0.1)


class FakeHttpSend:
    def __init__(self):
        self.messages = []

    async def __call__(self, message):
        self.messages.append(message)


async def _consume_proxy_generator(
    gen: ResponseGenerator,
) -> Tuple[ResponseStatus, List]:
    status = None
    messages = []
    async for message in gen:
        if isinstance(message, ResponseStatus):
            status = message
        else:
            messages.append(message)

    assert status is not None
    return status, messages


class TestgRPCProxy:
    """Test methods implemented on gRPCProxy"""

    def create_grpc_proxy(self, is_head: bool = False):
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return gRPCProxy(
            node_id=node_id,
            node_ip_address=node_ip_address,
            is_head=is_head,
            proxy_router=FakeProxyRouter(),
        )

    @pytest.mark.asyncio
    async def test_not_found_response(self):
        """Test gRPCProxy returns the correct not found response."""
        grpc_proxy = self.create_grpc_proxy()
        grpc_proxy.proxy_router.update_routes({})

        # Application name not provided.
        status, _ = await _consume_proxy_generator(
            grpc_proxy.proxy_request(
                FakeProxyRequest(request_type="grpc", app_name=""),
            )
        )

        assert status.code == grpc.StatusCode.NOT_FOUND
        assert "Application metadata not set" in status.message
        assert status.is_error is True

        # Application name is provided but wasn't found.
        status, _ = await _consume_proxy_generator(
            grpc_proxy.proxy_request(
                FakeProxyRequest(request_type="grpc", app_name="foobar"),
            )
        )

        assert status.code == grpc.StatusCode.NOT_FOUND
        assert "Application 'foobar' not found" in status.message
        assert status.is_error is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_draining", [False, True])
    @pytest.mark.parametrize("router_ready_for_traffic", [False, True])
    async def test_routes_response(
        self, is_draining: bool, router_ready_for_traffic: bool
    ):
        """Test responses to the routes method.

        The response should be an OK success unless:
            - the route table hasn't been updated yet.
            - the proxy is draining.
        """
        grpc_proxy = self.create_grpc_proxy()
        grpc_proxy.proxy_router.update_routes(
            {DeploymentID(name="deployment", app_name="app"): EndpointInfo("/route")},
        )
        if is_draining:
            grpc_proxy.update_draining(True)
        if router_ready_for_traffic:
            grpc_proxy.proxy_router.set_ready_for_traffic()

        status, [response_bytes] = await _consume_proxy_generator(
            grpc_proxy.proxy_request(
                FakeProxyRequest(
                    request_type="grpc",
                    is_route_request=True,
                ),
            )
        )

        assert isinstance(status, ResponseStatus)
        response = serve_pb2.ListApplicationsResponse()
        response.ParseFromString(response_bytes)

        if not is_draining and router_ready_for_traffic:
            assert status.code == grpc.StatusCode.OK
            assert status.message == HEALTHY_MESSAGE
            assert status.is_error is False
            assert response.application_names == ["app"]
        elif is_draining:
            assert status.code == grpc.StatusCode.UNAVAILABLE
            assert status.message == DRAINING_MESSAGE
            assert status.is_error is True
            assert response.application_names == ["app"]
        else:
            assert status.code == grpc.StatusCode.UNAVAILABLE
            assert status.message == ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE
            assert status.is_error is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_draining", [False, True])
    @pytest.mark.parametrize("router_ready_for_traffic", [False, True])
    async def test_health_response(
        self, is_draining: bool, router_ready_for_traffic: bool
    ):
        """Test responses to the health method.

        The response should be an OK success unless:
            - the route table hasn't been updated yet.
            - the proxy is draining.
        """
        grpc_proxy = self.create_grpc_proxy()
        if is_draining:
            grpc_proxy.update_draining(True)
        if router_ready_for_traffic:
            grpc_proxy.proxy_router.set_ready_for_traffic()

        status, [response_bytes] = await _consume_proxy_generator(
            grpc_proxy.proxy_request(
                FakeProxyRequest(
                    request_type="grpc",
                    is_health_request=True,
                ),
            )
        )

        assert isinstance(status, ResponseStatus)
        response = serve_pb2.HealthzResponse()
        response.ParseFromString(response_bytes)

        if not is_draining and router_ready_for_traffic:
            assert status.code == grpc.StatusCode.OK
            assert status.message == HEALTHY_MESSAGE
            assert status.is_error is False
            assert response.message == HEALTHY_MESSAGE
        elif is_draining:
            assert status.code == grpc.StatusCode.UNAVAILABLE
            assert status.message == DRAINING_MESSAGE
            assert status.is_error is True
            assert response.message == DRAINING_MESSAGE
        else:
            assert status.code == grpc.StatusCode.UNAVAILABLE
            assert status.message == ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE
            assert status.is_error is True
            assert response.message == ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE

    @pytest.mark.asyncio
    async def test_service_handler_factory(self):
        """Test gRPCProxy service_handler_factory returns the correct entrypoints."""

        # Ensure gRPC unary call uses the correct entry point.
        grpc_proxy = self.create_grpc_proxy()
        request_proto = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        unary_entrypoint = grpc_proxy.service_handler_factory(
            service_method="service_method", stream=False
        )
        assert unary_entrypoint.__name__ == "unary_unary"

        # Ensure the unary entry point returns the correct result and sets the
        # code and details on the grpc context object.
        grpc_proxy.proxy_router.route = "route"
        context = FakeGrpcContext()
        serve_grpc_context = RayServegRPCContext(context)
        grpc_proxy.proxy_router.handle = FakeGrpcHandle(
            streaming=False,
            grpc_context=serve_grpc_context,
        )
        grpc_proxy.proxy_router.app_is_cross_language = False
        result = await unary_entrypoint(request_proto=request_proto, context=context)
        assert result == "hello world"
        assert context.code() == grpc.StatusCode.OK
        assert context.details() == ""

        # Ensure gRPC streaming call uses the correct entry point.
        streaming_entrypoint = grpc_proxy.service_handler_factory(
            service_method="service_method", stream=True
        )
        assert streaming_entrypoint.__name__ == "unary_stream"

        # Ensure the streaming entry point returns the correct result and sets the
        # code and details on the grpc context object.
        grpc_proxy.proxy_router.route = "route"
        context = FakeGrpcContext()
        serve_grpc_context = RayServegRPCContext(context)
        grpc_proxy.proxy_router.handle = FakeGrpcHandle(
            streaming=True,
            grpc_context=serve_grpc_context,
        )
        grpc_proxy.proxy_router.app_is_cross_language = False
        result = await unary_entrypoint(request_proto=request_proto, context=context)
        assert list(result) == [f"hello world: {i}" for i in range(10)]
        assert context.code() == grpc.StatusCode.OK
        assert context.details() == ""


class TestHTTPProxy:
    """Test methods implemented on HTTPProxy"""

    def _check_asgi_messages(
        self, messages: List[Dict], *, status_code: int, body: str
    ):
        assert messages[0]["headers"] is not None
        assert messages[0]["status"] == status_code
        assert messages[1]["body"].decode("utf-8") == body

    def create_http_proxy(self, is_head: bool = False):
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return HTTPProxy(
            node_id=node_id,
            node_ip_address=node_ip_address,
            is_head=is_head,
            proxy_router=FakeProxyRouter(),
            self_actor_name="fake-proxy-name",
        )

    @pytest.mark.asyncio
    async def test_not_found_response(self):
        """Test the response returned when a route is not found."""
        http_proxy = self.create_http_proxy()
        http_proxy.proxy_router.update_routes({})

        status, messages = await _consume_proxy_generator(
            http_proxy.proxy_request(
                FakeProxyRequest(request_type="http", path="/not-found"),
            )
        )
        assert status.code == 404
        assert status.is_error is True
        self._check_asgi_messages(
            messages,
            status_code=404,
            body=(
                "Path '/not-found' not found. "
                "Ping http://.../-/routes for available routes."
            ),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_draining", [False, True])
    @pytest.mark.parametrize("router_ready_for_traffic", [False, True])
    async def test_routes_response(
        self, is_draining: bool, router_ready_for_traffic: bool
    ):
        """Test responses to the routes method.

        The response should be a 200 success unless:
            - the route table hasn't been updated yet.
            - the proxy is draining.
        """
        http_proxy = self.create_http_proxy()
        http_proxy.proxy_router.update_routes(
            {DeploymentID(name="deployment", app_name="app"): EndpointInfo("/route")},
        )
        if is_draining:
            http_proxy.update_draining(True)
        if router_ready_for_traffic:
            http_proxy.proxy_router.set_ready_for_traffic()

        status, messages = await _consume_proxy_generator(
            http_proxy.proxy_request(
                FakeProxyRequest(
                    request_type="http",
                    is_route_request=True,
                ),
            )
        )

        if not is_draining and router_ready_for_traffic:
            assert status.code == 200
            assert status.is_error is False
            assert status.message == HEALTHY_MESSAGE
            self._check_asgi_messages(
                messages, status_code=200, body='{"/route":"app"}'
            )
        elif is_draining:
            assert status.code == 503
            assert status.is_error is True
            assert status.message == DRAINING_MESSAGE
            self._check_asgi_messages(messages, status_code=503, body=DRAINING_MESSAGE)
        else:
            assert status.code == 503
            assert status.is_error is True
            assert status.message == ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE
            self._check_asgi_messages(
                messages, status_code=503, body=ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_draining", [False, True])
    @pytest.mark.parametrize("router_ready_for_traffic", [False, True])
    async def test_health_response(
        self, is_draining: bool, router_ready_for_traffic: bool
    ):
        """Test responses to the health check method.

        The response should be a 200 success unless:
            - the route table hasn't been updated yet.
            - the proxy is draining.
        """
        http_proxy = self.create_http_proxy()
        if is_draining:
            http_proxy.update_draining(True)
        if router_ready_for_traffic:
            http_proxy.proxy_router.set_ready_for_traffic()

        status, messages = await _consume_proxy_generator(
            http_proxy.proxy_request(
                FakeProxyRequest(
                    request_type="http",
                    is_health_request=True,
                ),
            )
        )
        if not is_draining and router_ready_for_traffic:
            assert status.code == 200
            assert status.is_error is False
            assert status.message == HEALTHY_MESSAGE
            self._check_asgi_messages(messages, status_code=200, body=HEALTHY_MESSAGE)
        elif is_draining:
            assert status.code == 503
            assert status.is_error is True
            assert status.message == DRAINING_MESSAGE
            self._check_asgi_messages(messages, status_code=503, body=DRAINING_MESSAGE)
        else:
            assert status.code == 503
            assert status.is_error is True
            assert status.message == ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE
            self._check_asgi_messages(
                messages, status_code=503, body=ROUTER_NOT_READY_FOR_TRAFFIC_MESSAGE
            )

    @pytest.mark.asyncio
    async def test_receive_asgi_messages(self):
        """Test HTTPProxy receive_asgi_messages received correct message."""
        http_proxy = self.create_http_proxy()
        internal_request_id = "fake-internal-request-id"
        request_metadata = RequestMetadata(
            request_id="fake-request-id",
            internal_request_id="fake-internal-request-id",
        )
        queue = AsyncMock()
        http_proxy.asgi_receive_queues[internal_request_id] = queue

        await http_proxy.receive_asgi_messages(request_metadata=request_metadata)
        queue.wait_for_message.assert_called_once()
        queue.get_messages_nowait.assert_called_once()

        with pytest.raises(KeyError):
            request_metadata.internal_request_id = "non-existent-internal-request-id"
            await http_proxy.receive_asgi_messages(request_metadata=request_metadata)

    @pytest.mark.asyncio
    async def test_call(self):
        """Test HTTPProxy __call__ calls proxy_request."""
        expected_messages = [
            {"type": "http.response.start", "status": "200"},
            {"type": "http.response.body"},
        ]

        http_proxy = self.create_http_proxy()
        http_proxy.proxy_router.route = "route"
        http_proxy.proxy_router.handle = FakeHTTPHandle(messages=expected_messages)
        http_proxy.proxy_router.app_is_cross_language = False

        receive = FakeHttpReceive()
        scope = {
            "type": "http",
            "headers": [
                (
                    b"x-request-id",
                    b"fake_request_id",
                ),
            ],
        }
        send = FakeHttpSend()

        # Ensure before calling __call__, send.messages should be empty.
        assert send.messages == []
        await http_proxy(
            scope=scope,
            receive=receive,
            send=send,
        )
        # Ensure after calling __call__, send.messages should be expected messages.
        assert send.messages == expected_messages

    @pytest.mark.asyncio
    async def test_proxy_asgi_receive(self):
        """Test HTTPProxy proxy_asgi_receive receives messages."""
        http_proxy = self.create_http_proxy()
        receive = AsyncMock()
        receive.side_effect = [
            {"type": "http.request"},
            {"type": "http.request"},
            {"type": "http.disconnect"},
        ]
        queue = AsyncMock()
        await http_proxy.proxy_asgi_receive(
            receive=receive,
            queue=queue,
        )

        queue.close.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "disconnect",
        [
            "client",
            "server_with_disconnect_message",
            "server_without_disconnect_message",
        ],
    )
    async def test_websocket_call(self, disconnect: str):
        """Test HTTPProxy websocket __call__ calls proxy_request."""

        if disconnect == "client":
            receive = FakeHttpReceive(
                [{"type": "websocket.disconnect", "code": "1000"}]
            )
            expected_messages = [
                {"type": "websocket.accept"},
                {"type": "websocket.send"},
            ]
        elif disconnect == "server_with_disconnect_message":
            receive = FakeHttpReceive()
            expected_messages = [
                {"type": "websocket.accept"},
                {"type": "websocket.send"},
                {"type": "websocket.disconnect", "code": "1000"},
            ]
        else:
            receive = FakeHttpReceive()
            expected_messages = [
                {"type": "websocket.accept"},
                {"type": "websocket.send"},
            ]

        http_proxy = self.create_http_proxy()
        http_proxy.proxy_router.route = "route"
        http_proxy.proxy_router.handle = FakeHTTPHandle(messages=expected_messages)
        http_proxy.proxy_router.app_is_cross_language = False

        scope = {
            "type": "websocket",
            "headers": [
                (
                    b"x-request-id",
                    b"fake_request_id",
                ),
            ],
        }
        send = FakeHttpSend()

        # Ensure before calling __call__, send.messages should be empty.
        assert send.messages == []
        await http_proxy(
            scope=scope,
            receive=receive,
            send=send,
        )
        # Ensure after calling __call__, send.messages should be expected messages.
        assert send.messages == expected_messages


@pytest.mark.asyncio
async def test_head_http_unhealthy_until_route_table_updated():
    """Health endpoint should error until `update_routes` has been called."""

    def get_handle_override(endpoint, info):
        return MockDeploymentHandle(endpoint.name, endpoint.app_name)

    http_proxy = HTTPProxy(
        node_id="fake-node-id",
        node_ip_address="fake-node-ip-address",
        # proxy is on head node
        is_head=True,
        proxy_router=ProxyRouter(get_handle_override),
        self_actor_name="fake-proxy-name",
    )
    proxy_request = FakeProxyRequest(
        request_type="http",
        is_health_request=True,
    )

    status, messages = await _consume_proxy_generator(
        http_proxy.proxy_request(proxy_request)
    )
    assert status.code == 503
    assert status.is_error is True
    assert status.message == NO_ROUTES_MESSAGE
    assert messages[0]["headers"] is not None
    assert messages[0]["status"] == 503
    assert messages[1]["body"].decode("utf-8") == NO_ROUTES_MESSAGE

    # Update route table, response should no longer error
    http_proxy.proxy_router.update_routes(
        {DeploymentID("a", "b"): EndpointInfo("/route")}
    )

    status, messages = await _consume_proxy_generator(
        http_proxy.proxy_request(proxy_request)
    )
    assert status.code == 200
    assert status.is_error is False
    assert status.message == HEALTHY_MESSAGE
    assert messages[0]["headers"] is not None
    assert messages[0]["status"] == 200
    assert messages[1]["body"].decode("utf-8") == HEALTHY_MESSAGE


@pytest.mark.asyncio
async def test_worker_http_unhealthy_until_replicas_populated():
    """Health endpoint should error until handle's running replicas is populated."""

    handle = MockDeploymentHandle("a", "b")
    http_proxy = HTTPProxy(
        node_id="fake-node-id",
        node_ip_address="fake-node-ip-address",
        # proxy is on worker node
        is_head=False,
        proxy_router=ProxyRouter(lambda *args: handle),
        self_actor_name="fake-proxy-name",
    )
    proxy_request = FakeProxyRequest(
        request_type="http",
        is_health_request=True,
    )

    status, messages = await _consume_proxy_generator(
        http_proxy.proxy_request(proxy_request)
    )
    assert status.code == 503
    assert status.is_error is True
    assert status.message == NO_ROUTES_MESSAGE
    assert messages[0]["headers"] is not None
    assert messages[0]["status"] == 503
    assert messages[1]["body"].decode("utf-8") == NO_ROUTES_MESSAGE

    # Update route table, response should still error because running
    # replicas is not yet populated.
    http_proxy.proxy_router.update_routes(
        {DeploymentID("a", "b"): EndpointInfo("/route")}
    )

    status, messages = await _consume_proxy_generator(
        http_proxy.proxy_request(proxy_request)
    )
    assert status.code == 503
    assert status.is_error is True
    assert status.message == NO_REPLICAS_MESSAGE
    assert messages[0]["headers"] is not None
    assert messages[0]["status"] == 503
    assert messages[1]["body"].decode("utf-8") == NO_REPLICAS_MESSAGE

    # Populate running replicas. Now response should be healthy and return 200.
    handle.set_running_replicas_populated(True)

    status, messages = await _consume_proxy_generator(
        http_proxy.proxy_request(proxy_request)
    )
    assert status.code == 200
    assert status.is_error is False
    assert status.message == HEALTHY_MESSAGE
    assert messages[0]["headers"] is not None
    assert messages[0]["status"] == 200
    assert messages[1]["body"].decode("utf-8") == HEALTHY_MESSAGE


class TestProxyRouterMatchRoutePattern:
    """Test ProxyRouter.match_route_pattern functionality."""

    @pytest.fixture
    def mock_get_handle(self):
        def _get_handle(endpoint: DeploymentID, info: EndpointInfo):
            return MockDeploymentHandle(deployment_name=endpoint.name)

        return _get_handle

    def test_match_route_pattern_no_patterns(self, mock_get_handle):
        """Test that match_route_pattern returns route_prefix when no patterns exist."""
        router = ProxyRouter(mock_get_handle)
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api", route_patterns=None
                )
            }
        )

        scope = {"type": "http", "path": "/api/users/123", "method": "GET"}
        result = router.match_route_pattern("/api", scope)
        assert result == "/api"

    def test_match_route_pattern_with_patterns(self, mock_get_handle):
        """Test that match_route_pattern matches specific route patterns."""
        router = ProxyRouter(mock_get_handle)
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api",
                    route_patterns=[
                        "/api/",
                        "/api/users/{user_id}",
                        "/api/items/{item_id}/details",
                    ],
                )
            }
        )

        # Test matching parameterized route
        scope = {"type": "http", "path": "/api/users/123", "method": "GET"}
        result = router.match_route_pattern("/api", scope)
        assert result == "/api/users/{user_id}"

        # Test matching nested parameterized route
        scope = {"type": "http", "path": "/api/items/abc/details", "method": "GET"}
        result = router.match_route_pattern("/api", scope)
        assert result == "/api/items/{item_id}/details"

        # Test matching root
        scope = {"type": "http", "path": "/api/", "method": "GET"}
        result = router.match_route_pattern("/api", scope)
        assert result == "/api/"

    def test_match_route_pattern_caching(self, mock_get_handle):
        """Test that mock Starlette apps are cached for performance."""
        router = ProxyRouter(mock_get_handle)
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api",
                    route_patterns=["/api/users/{user_id}"],
                )
            }
        )

        scope = {"type": "http", "path": "/api/users/123", "method": "GET"}

        # First call should create and cache the mock app
        assert "/api" not in router._route_pattern_apps
        result1 = router.match_route_pattern("/api", scope)
        assert result1 == "/api/users/{user_id}"
        assert "/api" in router._route_pattern_apps

        # Second call should use cached app
        cached_app = router._route_pattern_apps["/api"]
        result2 = router.match_route_pattern("/api", scope)
        assert result2 == "/api/users/{user_id}"
        assert router._route_pattern_apps["/api"] is cached_app

    def test_match_route_pattern_cache_invalidation(self, mock_get_handle):
        """Test that cache is cleared when routes are updated."""
        router = ProxyRouter(mock_get_handle)
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api",
                    route_patterns=["/api/users/{user_id}"],
                )
            }
        )

        scope = {"type": "http", "path": "/api/users/123", "method": "GET"}
        router.match_route_pattern("/api", scope)
        assert "/api" in router._route_pattern_apps

        # Update routes should clear cache
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api",
                    route_patterns=["/api/items/{item_id}"],
                )
            }
        )
        assert len(router._route_pattern_apps) == 0

    def test_match_route_pattern_empty_patterns(self, mock_get_handle):
        """Test that empty pattern list returns route_prefix."""
        router = ProxyRouter(mock_get_handle)
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api", route_patterns=[]
                )
            }
        )

        scope = {"type": "http", "path": "/api/users/123", "method": "GET"}
        result = router.match_route_pattern("/api", scope)
        assert result == "/api"

    def test_match_route_pattern_no_match_fallback(self, mock_get_handle):
        """Test that unmatched requests fall back to route_prefix."""
        router = ProxyRouter(mock_get_handle)
        router.update_routes(
            {
                DeploymentID("api", "default"): EndpointInfo(
                    route="/api",
                    route_patterns=["/api/users/{user_id}"],
                )
            }
        )

        # Request to path not in patterns
        scope = {"type": "http", "path": "/api/admin/settings", "method": "GET"}
        result = router.match_route_pattern("/api", scope)
        # Should fall back to prefix since no pattern matches
        assert result == "/api"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
