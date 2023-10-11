import asyncio
import pickle
import sys
from typing import AsyncGenerator
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import grpc
import pytest

import ray
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.common import EndpointTag, RequestProtocol
from ray.serve._private.constants import (
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_NAMESPACE,
)
from ray.serve._private.proxy import (
    HEALTH_CHECK_SUCCESS_MESSAGE,
    GenericProxy,
    HTTPProxy,
    gRPCProxy,
)
from ray.serve._private.proxy_request_response import ASGIProxyRequest, ProxyResponse
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


class FakeRefGenerator:
    def __init__(self, messages=None):
        self.called = False
        self.messages = messages

    def _next_async(self, *args, **kwargs):
        if not self.called:
            self.called = True
            return FakeRef(messages=self.messages)
        raise StopAsyncIteration


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
            proxy_router_class=MagicMock(),
            controller_actor=FakeActorHandler("fake_controller_actor"),
            proxy_actor=FakeActorHandler("fake_proxy_actor"),
        )

    def test_subclass_from_generic_proxy(self):
        """Test gRPCProxy is a subclass from GenericProxy."""
        grpc_proxy = self.create_grpc_proxy()

        assert isinstance(grpc_proxy, GenericProxy)

    def test_protocol(self):
        """Test gRPCProxy set up the correct protocol property."""
        grpc_proxy = self.create_grpc_proxy()

        assert isinstance(grpc_proxy.protocol, RequestProtocol)
        assert grpc_proxy.protocol == "gRPC"

    def test_success_status_code(self):
        """Test gRPCProxy set up the correct success status code."""
        grpc_proxy = self.create_grpc_proxy()

        assert isinstance(grpc_proxy.success_status_code, str)
        assert grpc_proxy.success_status_code == str(grpc.StatusCode.OK)

    @pytest.mark.asyncio
    async def test_not_found(self):
        """Test gRPCProxy set up the correct not found response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = AsyncMock()
        not_found_status = grpc.StatusCode.NOT_FOUND
        response = await grpc_proxy.not_found(proxy_request=proxy_request)

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(not_found_status)
        proxy_request.send_status_code.assert_called_with(status_code=not_found_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_draining_response(self):
        """Test gRPCProxy set up the correct draining response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = AsyncMock()
        draining_status = grpc.StatusCode.UNAVAILABLE
        response = await grpc_proxy.draining_response(proxy_request=proxy_request)

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(draining_status)
        proxy_request.send_status_code.assert_called_with(status_code=draining_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_response(self):
        """Test gRPCProxy set up the correct timeout response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = AsyncMock()
        timeout_status = grpc.StatusCode.CANCELLED
        response = await grpc_proxy.timeout_response(
            proxy_request=proxy_request, request_id="fake-request-id"
        )

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(timeout_status)
        proxy_request.send_status_code.assert_called_with(status_code=timeout_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_routes_response(self):
        """Test gRPCProxy set up the correct routes response."""
        grpc_proxy = self.create_grpc_proxy()
        endpoint = EndpointTag("endpoint", "app1")
        route_info = {"/route": endpoint}
        grpc_proxy.route_info = route_info
        proxy_request = AsyncMock()
        routes_status = grpc.StatusCode.OK
        response = await grpc_proxy.routes_response(proxy_request=proxy_request)

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(routes_status)
        response_proto = serve_pb2.ListApplicationsResponse()
        response_proto.ParseFromString(response.response)
        assert response_proto.application_names == [endpoint.app]
        proxy_request.send_status_code.assert_called_with(status_code=routes_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_response(self):
        """Test gRPCProxy set up the correct health response."""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = AsyncMock()
        health_status = grpc.StatusCode.OK
        response = await grpc_proxy.health_response(proxy_request=proxy_request)

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(grpc.StatusCode.OK)
        response_proto = serve_pb2.HealthzResponse()
        response_proto.ParseFromString(response.response)
        assert response_proto.message == HEALTH_CHECK_SUCCESS_MESSAGE
        proxy_request.send_status_code.assert_called_with(status_code=health_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_handler_factory(self):
        """Test gRPCProxy service_handler_factory returns the correct entrypoints."""
        grpc_proxy = self.create_grpc_proxy()
        request_proto = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        unary_entrypoint = grpc_proxy.service_handler_factory(
            service_method="service_method", stream=False
        )
        assert unary_entrypoint.__name__ == "unary_unary"
        mocked_proxy_request_unary = AsyncMock()
        with patch.object(grpc_proxy, "proxy_request", mocked_proxy_request_unary):
            await unary_entrypoint(request_proto=request_proto, context=MagicMock())
        mocked_proxy_request_unary.assert_called_once()

        streaming_entrypoint = grpc_proxy.service_handler_factory(
            service_method="service_method", stream=True
        )
        assert streaming_entrypoint.__name__ == "unary_stream"
        mocked_proxy_request_stream = AsyncMock()
        with patch.object(grpc_proxy, "proxy_request", mocked_proxy_request_stream):
            response = streaming_entrypoint(
                request_proto=request_proto, context=MagicMock()
            )
            assert isinstance(response, AsyncGenerator)
            while True:
                try:
                    obj_ref = await response.__anext__()
                    _ = await obj_ref
                except StopAsyncIteration:
                    break
        mocked_proxy_request_stream.assert_called_once()


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

    def test_subclass_from_generic_proxy(self):
        """Test HTTPProxy is a subclass from GenericProxy."""
        http_proxy = self.create_http_proxy()

        assert isinstance(http_proxy, GenericProxy)

    def test_protocol(self):
        """Test HTTPProxy set up the correct success status code."""
        http_proxy = self.create_http_proxy()

        assert isinstance(http_proxy.protocol, RequestProtocol)
        assert http_proxy.protocol == "HTTP"

    def test_success_status_code(self):
        """Test HTTPProxy set up the correct success status code."""
        http_proxy = self.create_http_proxy()

        assert isinstance(http_proxy.success_status_code, str)
        assert http_proxy.success_status_code == "200"

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

        assert isinstance(response, ProxyResponse)
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
        assert isinstance(response, ProxyResponse)
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
        assert isinstance(response, ProxyResponse)
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
        assert isinstance(response, ProxyResponse)
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
        assert isinstance(response, ProxyResponse)
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
