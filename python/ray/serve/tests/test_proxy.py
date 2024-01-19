import asyncio
import pickle
import sys
from typing import Dict
from unittest.mock import AsyncMock

import grpc
import pytest

import ray
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.common import DeploymentID, EndpointInfo, EndpointTag
from ray.serve._private.constants import (
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_NAMESPACE,
)
from ray.serve._private.proxy import (
    DRAINED_MESSAGE,
    HEALTH_CHECK_SUCCESS_MESSAGE,
    HTTPProxy,
    ResponseStatus,
    gRPCProxy,
)
from ray.serve._private.proxy_request_response import ProxyRequest
from ray.serve._private.proxy_router import ProxyRouter
from ray.serve._private.test_utils import FakeGrpcContext
from ray.serve.generated import serve_pb2
from ray.serve.grpc_util import RayServegRPCContext


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


class FakeActorHandler:
    def __init__(self, actor_id):
        self._actor_id = actor_id

    @property
    def listen_for_change(self):
        class FakeListenForChangeActorMethod:
            def remote(self, snapshot_ids):
                return FakeRef()

        return FakeListenForChangeActorMethod()

    @property
    def receive_asgi_messages(self):
        class FakeReceiveASGIMessagesActorMethod:
            def remote(self, request_id):
                return FakeRef()

        return FakeReceiveASGIMessagesActorMethod()


class FakeGrpcHandle:
    def __init__(self, streaming: bool, grpc_context: RayServegRPCContext):
        self.deployment_id = DeploymentID("fak_deployment_name", "fake_app_name")
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


class FakeProxyRouter(ProxyRouter):
    def __init__(self, *args, **kwargs):
        self.route = None
        self.handle = None
        self.app_is_cross_language = None

    def update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]):
        pass

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


class FakeProxyRequest(ProxyRequest):
    def __init__(self):
        self._request_type = ""
        self._method = ""
        self._route_path = ""
        self._is_route_request = False
        self._is_health_request = False
        self.app_name = ""
        self.path = ""

    @property
    def request_type(self) -> str:
        return self._request_type

    @property
    def method(self) -> str:
        return self._method

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
        self.deployment_id = DeploymentID("fak_deployment_name", "fake_app_name")
        self.messages = messages

    async def remote(self, *args, **kwargs):
        return pickle.dumps(self.messages)

    def options(self, *args, **kwargs):
        return self


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


class TestgRPCProxy:
    """Test methods implemented on gRPCProxy"""

    def create_grpc_proxy(self):
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return gRPCProxy(
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=FakeProxyRouter,
            controller_actor=FakeActorHandler("fake_controller_actor"),
        )

    @pytest.mark.asyncio
    async def test_not_found(self):
        """Test gRPCProxy set up the correct not found response."""
        grpc_proxy = self.create_grpc_proxy()

        # Application name isn't provided.
        proxy_request = FakeProxyRequest()
        proxy_request.app_name = ""
        gen = grpc_proxy.not_found(proxy_request)
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.NOT_FOUND
        assert "Application metadata not set" in status.message
        assert status.is_error is True

        # Application name is provided but wasn't found.
        proxy_request.app_name = "foobar"
        gen = grpc_proxy.not_found(proxy_request)
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.NOT_FOUND
        assert "Application 'foobar' not found" in status.message
        assert status.is_error is True

    @pytest.mark.asyncio
    async def test_draining_response(self):
        """Test gRPCProxy set up the correct draining response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = FakeProxyRequest()
        proxy_request.app_name = ""

        gen = grpc_proxy.draining_response(proxy_request)
        message = await gen.__anext__()
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(message, bytes)
        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.UNAVAILABLE
        assert status.message == DRAINED_MESSAGE
        assert status.is_error is True

    @pytest.mark.asyncio
    async def test_routes_response(self):
        """Test gRPCProxy set up the correct routes response."""
        grpc_proxy = self.create_grpc_proxy()
        endpoint = EndpointTag("endpoint", "app1")
        route_info = {"/route": endpoint}
        grpc_proxy.route_info = route_info
        proxy_request = FakeProxyRequest()

        gen = grpc_proxy.routes_response(proxy_request)
        message = await gen.__anext__()
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(message, bytes)
        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.OK
        assert status.message == HEALTH_CHECK_SUCCESS_MESSAGE
        assert status.is_error is False
        response_proto = serve_pb2.ListApplicationsResponse()
        response_proto.ParseFromString(message)
        assert response_proto.application_names == [endpoint.app]

    @pytest.mark.asyncio
    async def test_health_response(self):
        """Test gRPCProxy set up the correct health response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = FakeProxyRequest()
        gen = grpc_proxy.health_response(proxy_request)
        message = await gen.__anext__()
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(message, bytes)
        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.OK
        assert status.message == HEALTH_CHECK_SUCCESS_MESSAGE
        assert status.is_error is False
        response_proto = serve_pb2.HealthzResponse()
        response_proto.ParseFromString(message)
        assert response_proto.message == HEALTH_CHECK_SUCCESS_MESSAGE

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

    def create_http_proxy(self):
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return HTTPProxy(
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=FakeProxyRouter,
            controller_actor=FakeActorHandler("fake_controller_actor"),
            proxy_actor=FakeActorHandler("fake_proxy_actor"),
        )

    @pytest.mark.asyncio
    async def test_not_found(self):
        """Test HTTPProxy set up the correct not found response."""
        http_proxy = self.create_http_proxy()
        proxy_request = FakeProxyRequest()
        gen = http_proxy.not_found(proxy_request)
        status = None
        messages = []
        async for message in gen:
            if isinstance(message, ResponseStatus):
                status = message
            else:
                messages.append(message)

        not_found_message = "Please ping http://.../-/routes for route table."
        assert any([message.get("headers") is not None for message in messages])
        assert any(
            [not_found_message in str(message.get("body")) for message in messages]
        )
        assert isinstance(status, ResponseStatus)
        assert status.code == 404
        assert status.is_error is True

    @pytest.mark.asyncio
    async def test_draining_response(self):
        """Test HTTPProxy set up the correct draining response."""
        http_proxy = self.create_http_proxy()
        proxy_request = FakeProxyRequest()
        gen = http_proxy.draining_response(proxy_request)
        status = None
        messages = []
        async for message in gen:
            if isinstance(message, ResponseStatus):
                status = message
            else:
                messages.append(message)

        assert any([message.get("headers") is not None for message in messages])
        assert any(
            [DRAINED_MESSAGE in str(message.get("body")) for message in messages]
        )
        assert isinstance(status, ResponseStatus)
        assert status.code == 503
        assert status.is_error is True

    @pytest.mark.asyncio
    async def test_timeout_response(self):
        """Test HTTPProxy set up the correct timeout response."""
        http_proxy = self.create_http_proxy()
        proxy_request = FakeProxyRequest()
        request_id = "fake_request_id"
        gen = http_proxy.timeout_response(
            proxy_request=proxy_request,
            request_id=request_id,
        )
        status = None
        messages = []
        async for message in gen:
            if isinstance(message, ResponseStatus):
                status = message
            else:
                messages.append(message)

        assert any([message.get("headers") is not None for message in messages])
        assert any(
            [
                f"Request {request_id} timed out after" in str(message.get("body"))
                for message in messages
            ]
        )
        assert isinstance(status, ResponseStatus)
        assert status.code == 408
        assert status.is_error is True

    @pytest.mark.asyncio
    async def test_routes_response(self):
        """Test HTTPProxy set up the correct routes response."""
        http_proxy = self.create_http_proxy()
        endpoint = EndpointTag("endpoint", "app1")
        route_info = {"/route": endpoint}
        http_proxy.route_info = route_info
        proxy_request = FakeProxyRequest()
        gen = http_proxy.routes_response(proxy_request=proxy_request)
        status = None
        messages = []
        async for message in gen:
            if isinstance(message, ResponseStatus):
                status = message
            else:
                messages.append(message)

        assert any([message.get("headers") is not None for message in messages])
        assert any(
            ['{"/route":"app1"}' in str(message.get("body")) for message in messages]
        )
        assert isinstance(status, ResponseStatus)
        assert status.code == 200
        assert status.is_error is False

    @pytest.mark.asyncio
    async def test_health_response(self):
        """Test HTTPProxy set up the correct health response."""
        http_proxy = self.create_http_proxy()
        proxy_request = FakeProxyRequest()
        gen = http_proxy.health_response(proxy_request=proxy_request)
        status = None
        messages = []
        async for message in gen:
            if isinstance(message, ResponseStatus):
                status = message
            else:
                messages.append(message)

        assert any([message.get("headers") is not None for message in messages])
        assert any(
            [
                HEALTH_CHECK_SUCCESS_MESSAGE in str(message.get("body"))
                for message in messages
            ]
        )
        assert isinstance(status, ResponseStatus)
        assert status.code == 200
        assert status.is_error is False

    @pytest.mark.asyncio
    async def test_receive_asgi_messages(self):
        """Test HTTPProxy receive_asgi_messages received correct message."""
        http_proxy = self.create_http_proxy()
        request_id = "fake-request-id"
        queue = AsyncMock()
        http_proxy.asgi_receive_queues[request_id] = queue

        await http_proxy.receive_asgi_messages(request_id=request_id)
        queue.wait_for_message.assert_called_once()
        queue.get_messages_nowait.assert_called_once()

        with pytest.raises(KeyError):
            await http_proxy.receive_asgi_messages(request_id="non-existent-request-id")

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


class TestTimeoutKeepAliveConfig:
    """Test setting keep_alive_timeout_s in config and env."""

    def get_proxy_actor(self) -> ActorHandle:
        proxy_actor_name = None
        for actor in ray._private.state.actors().values():
            if actor["ActorClassName"] == "ProxyActor":
                proxy_actor_name = actor["Name"]
        return ray.get_actor(proxy_actor_name, namespace=SERVE_NAMESPACE)

    def test_default_keep_alive_timeout_s(self, ray_shutdown):
        """Test when no keep_alive_timeout_s is set.

        When the keep_alive_timeout_s is not set, the uvicorn keep alive is 5.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._uvicorn_keep_alive.remote())
            == DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S
        )

    def test_set_keep_alive_timeout_in_http_configs(self, ray_shutdown):
        """Test when keep_alive_timeout_s is in http configs.

        When the keep_alive_timeout_s is set in http configs, the uvicorn keep alive
        is set correctly.
        """
        keep_alive_timeout_s = 222
        serve.start(http_options={"keep_alive_timeout_s": keep_alive_timeout_s})
        proxy_actor = self.get_proxy_actor()
        assert ray.get(proxy_actor._uvicorn_keep_alive.remote()) == keep_alive_timeout_s

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_keep_alive_timeout_in_env(self, ray_instance, ray_shutdown):
        """Test when keep_alive_timeout_s is in env.

        When the keep_alive_timeout_s is set in env, the uvicorn keep alive
        is set correctly.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert ray.get(proxy_actor._uvicorn_keep_alive.remote()) == 333

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_timeout_keep_alive_in_both_config_and_env(
        self, ray_instance, ray_shutdown
    ):
        """Test when keep_alive_timeout_s is in both http configs and env.

        When the keep_alive_timeout_s is set in env, the uvicorn keep alive
        is set to the one in env.
        """
        keep_alive_timeout_s = 222
        serve.start(http_options={"keep_alive_timeout_s": keep_alive_timeout_s})
        proxy_actor = self.get_proxy_actor()
        assert ray.get(proxy_actor._uvicorn_keep_alive.remote()) == 333


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
