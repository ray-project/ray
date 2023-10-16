import asyncio
import pickle
import sys
from typing import AsyncGenerator, Dict
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

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
from ray.serve.generated import serve_pb2


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


class FakeActor:
    def remote(self, snapshot_ids):
        return FakeRef()


class FakeActorHandler:
    def __init__(self, actor_id):
        self._actor_id = actor_id

    @property
    def listen_for_change(self):
        return FakeActor()

    def remote(self, *args, **kwargs):
        return FakeRef()


class FakeHandle:
    def __init__(self, streaming: bool):
        self.deployment_id = DeploymentID("fak_deployment_name", "fake_app_name")
        self.streaming = streaming

    async def remote(self, *args, **kwargs):
        def unary_call():
            return "hello world"

        def streaming_call():
            for i in range(10):
                yield f"hello world: {i}"
        return unary_call() if not self.streaming else streaming_call()

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
        if self.route is None and self.handle is None and self.app_is_cross_language is None:
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


class FakeGrpcContext:
    def __init__(self):
        self.code = None
        self.details = None
        self._trailing_metadata = None
        self._invocation_metadata = []

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details

    def set_trailing_metadata(self, trailing_metadata):
        self._trailing_metadata = trailing_metadata

    def invocation_metadata(self):
        return self._invocation_metadata


class TestgRPCProxy:
    """Test methods implemented on gRPCProxy"""

    def create_grpc_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return gRPCProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=FakeProxyRouter,
            controller_actor=FakeActorHandler("fake_controller_actor"),
            proxy_actor=FakeActorHandler("fake_proxy_actor"),
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

        # Application name is provided but wasn't found.
        proxy_request.app_name = "foobar"
        gen = grpc_proxy.not_found(proxy_request)
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.NOT_FOUND
        assert "Application 'foobar' not found" in status.message

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
        response_proto = serve_pb2.ListApplicationsResponse()
        response_proto.ParseFromString(message)
        assert response_proto.application_names == [endpoint.app]

    @pytest.mark.asyncio
    async def test_health_response(self):
        """Test gRPCProxy set up the correct health response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = FakeProxyRequest()
        # health_status = grpc.StatusCode.OK
        gen = grpc_proxy.health_response(proxy_request)
        message = await gen.__anext__()
        status = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        assert isinstance(message, bytes)
        assert isinstance(status, ResponseStatus)
        assert status.code == grpc.StatusCode.OK
        assert status.message == HEALTH_CHECK_SUCCESS_MESSAGE
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
        grpc_proxy.proxy_router.handle = FakeHandle(streaming=False)
        grpc_proxy.proxy_router.app_is_cross_language = False
        context = FakeGrpcContext()
        result = await unary_entrypoint(request_proto=request_proto, context=context)
        assert result == "hello world"
        assert context.code == grpc.StatusCode.OK
        assert context.details == ""

        # Ensure gRPC streaming call uses the correct entry point.
        streaming_entrypoint = grpc_proxy.service_handler_factory(
            service_method="service_method", stream=True
        )
        assert streaming_entrypoint.__name__ == "unary_stream"

        # Ensure the streaming entry point returns the correct result and sets the
        # code and details on the grpc context object.
        grpc_proxy.proxy_router.route = "route"
        grpc_proxy.proxy_router.handle = FakeHandle(streaming=True)
        grpc_proxy.proxy_router.app_is_cross_language = False
        context = FakeGrpcContext()
        result = await unary_entrypoint(request_proto=request_proto, context=context)
        assert list(result) == [f"hello world: {i}" for i in range(10)]
        assert context.code == grpc.StatusCode.OK
        assert context.details == ""


class TestHTTPProxy:
    """Test methods implemented on HTTPProxy"""

    def create_http_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=MagicMock(),
            controller_actor=FakeActorHandler("fake_controller_actor"),
            proxy_actor=FakeActorHandler("fake_proxy_actor"),
        )

    @pytest.mark.asyncio
    @patch("ray.serve._private.proxy.Response")
    async def test_not_found(self, mocked_util):
        """Test HTTPProxy set up the correct not found response."""
        mocked_response = AsyncMock()
        mocked_util.return_value = mocked_response
        http_proxy = self.create_http_proxy()
        not_found_status = 404
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.not_found(proxy_request=proxy_request)

        assert response.status_code == str(not_found_status)
        mocked_util.assert_called_with(ANY, status_code=not_found_status)
        mocked_response.send.assert_called_once()

    @pytest.mark.asyncio
    @patch("ray.serve._private.proxy.Response")
    async def test_draining_response(self, mocked_util):
        """Test HTTPProxy set up the correct draining response."""
        mocked_response = AsyncMock()
        mocked_util.return_value = mocked_response
        http_proxy = self.create_http_proxy()
        draining_status = 503
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.draining_response(proxy_request=proxy_request)
        assert response.status_code == str(draining_status)
        mocked_util.assert_called_with(ANY, status_code=draining_status)
        mocked_response.send.assert_called_once()

    @pytest.mark.asyncio
    @patch("ray.serve._private.proxy.Response")
    async def test_timeout_response(self, mocked_util):
        mocked_response = AsyncMock()
        """Test HTTPProxy set up the correct timeout response."""
        mocked_util.return_value = mocked_response
        http_proxy = self.create_http_proxy()
        timeout_status = 408
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.timeout_response(
            proxy_request=proxy_request,
            request_id="fake-request-id",
        )
        assert response.status_code == str(timeout_status)
        mocked_util.assert_called_with(ANY, status_code=timeout_status)
        mocked_response.send.assert_called_once()

    @pytest.mark.asyncio
    @patch("ray.serve._private.proxy.starlette.responses.JSONResponse")
    async def test_routes_response(self, mock_json_response):
        """Test HTTPProxy set up the correct routes response."""
        mocked_response = AsyncMock()
        mock_json_response.return_value = mocked_response
        http_proxy = self.create_http_proxy()
        routes_status = 200
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.routes_response(proxy_request=proxy_request)
        assert response.status_code == str(routes_status)
        mock_json_response.assert_called_once()
        mocked_response.assert_called_once()

    @pytest.mark.asyncio
    @patch("ray.serve._private.proxy.starlette.responses.PlainTextResponse")
    async def test_health_response(self, mock_plain_text_response):
        """Test HTTPProxy set up the correct health response."""
        mocked_response = AsyncMock()
        mock_plain_text_response.return_value = mocked_response
        http_proxy = self.create_http_proxy()
        health_status = 200
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.health_response(proxy_request=proxy_request)
        assert response.status_code == str(health_status)
        mock_plain_text_response.assert_called_once()
        mocked_response.assert_called_once()

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
        http_proxy = self.create_http_proxy()
        mocked_proxy_request = AsyncMock()
        with patch.object(http_proxy, "proxy_request", mocked_proxy_request):
            await http_proxy(
                scope=AsyncMock(),
                receive=AsyncMock(),
                send=AsyncMock(),
            )
        mocked_proxy_request.assert_called_once()

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
