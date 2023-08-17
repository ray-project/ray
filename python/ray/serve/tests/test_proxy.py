import sys
from unittest.mock import patch, MagicMock

import grpc
import pytest
import ray
from ray import serve
from ray.actor import ActorHandle
from ray.serve._private.constants import (
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    SERVE_NAMESPACE,
)
from ray.serve._private.http_proxy import GenericProxy, gRPCProxy, HTTPProxy
from ray.serve._private.proxy_request_response import (
    ASGIProxyRequest,
    gRPCProxyRequest,
    ProxyResponse,
)
from ray.serve._private.common import EndpointTag, RequestProtocol
from ray.serve._private.constants import RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING
from ray.serve.generated import serve_pb2

from unittest.mock import AsyncMock


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
@patch("ray.serve._private.http_proxy.ray.get_runtime_context", MagicMock())
@patch("ray.serve._private.http_proxy.ray.get_actor", MagicMock())
class TestgRPCProxy:
    def create_grpc_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return gRPCProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=MagicMock(),
        )

    def test_subclass_from_generic_proxy(self):
        """Test gRPCProxy is a subclass from GenericProxy"""
        grpc_proxy = self.create_grpc_proxy()
        assert isinstance(grpc_proxy, GenericProxy)

    def test_protocol(self):
        """Test gRPCProxy set up the correct protocol property"""
        grpc_proxy = self.create_grpc_proxy()
        assert isinstance(grpc_proxy.protocol, RequestProtocol)
        assert grpc_proxy.protocol == "gRPC"

    def test_success_status_code(self):
        """Test gRPCProxy set up the correct success status code"""
        grpc_proxy = self.create_grpc_proxy()
        assert isinstance(grpc_proxy.success_status_code, str)
        assert grpc_proxy.success_status_code == str(grpc.StatusCode.OK)

    @pytest.mark.asyncio
    async def test_not_found(self):
        """Test gRPCProxy set up the correct not found response"""
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
        """Test gRPCProxy set up the correct draining response"""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = AsyncMock()
        draining_status = grpc.StatusCode.ABORTED
        response = await grpc_proxy.draining_response(proxy_request=proxy_request)

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(draining_status)
        proxy_request.send_status_code.assert_called_with(status_code=draining_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_response(self):
        """Test gRPCProxy set up the correct timeout response"""
        grpc_proxy = self.create_grpc_proxy()
        proxy_request = AsyncMock()
        timeout_status = grpc.StatusCode.ABORTED
        response = await grpc_proxy.timeout_response(
            proxy_request=proxy_request, request_id="fake-request-id"
        )

        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(timeout_status)
        proxy_request.send_status_code.assert_called_with(status_code=timeout_status)
        proxy_request.send_details.assert_called_once()

    @pytest.mark.asyncio
    async def test_routes_response(self):
        """Test gRPCProxy set up the correct routes response"""
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
        assert response_proto.application_names == [str(endpoint)]

    @pytest.mark.asyncio
    async def test_health_response(self):
        """Test gRPCProxy set up the correct health response"""
        grpc_proxy = self.create_grpc_proxy()
        request_proto = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        proxy_request = gRPCProxyRequest(
            request_proto=request_proto,
            context=MagicMock(),
            service_method="service_method",
            stream=False,
        )
        response = await grpc_proxy.health_response(proxy_request=proxy_request)
        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(grpc.StatusCode.OK)

    @pytest.mark.asyncio
    async def test_service_handler_factory(self):
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

    @pytest.mark.asyncio
    async def test_send_request_to_replica_unary(self):
        grpc_proxy = self.create_grpc_proxy()
        mocked_send_request_to_replica_streaming = AsyncMock()
        with patch.object(
            grpc_proxy,
            "send_request_to_replica_streaming",
            mocked_send_request_to_replica_streaming,
        ):
            await grpc_proxy.send_request_to_replica_unary(
                handle=MagicMock(), proxy_request=MagicMock()
            )
        mocked_send_request_to_replica_streaming.assert_called_once()

    @pytest.mark.asyncio
    async def test_setup_request_context_and_handle(self):
        grpc_proxy = self.create_grpc_proxy()
        handle = MagicMock()
        context = MagicMock()
        request_id = "fake-request-id"
        context.invocation_metadata.return_value = [("request_id", request_id)]
        request_proto = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        proxy_request = gRPCProxyRequest(
            request_proto=request_proto,
            context=context,
            service_method="service_method",
            stream=False,
        )
        (
            returned_context,
            returned_request_id,
        ) = grpc_proxy.setup_request_context_and_handle(
            app_name="fake-app-name",
            handle=handle,
            route_path="/fake-route-path",
            proxy_request=proxy_request,
        )
        handle._set_request_protocol.assert_called_with(RequestProtocol.GRPC)
        assert returned_request_id == request_id

    @pytest.mark.asyncio
    async def test_consume_generator_stream(self):
        grpc_proxy = self.create_grpc_proxy()
        mocked_streaming_generator_helper = AsyncMock()
        with patch.object(
            grpc_proxy, "_streaming_generator_helper", mocked_streaming_generator_helper
        ):
            await grpc_proxy._consume_generator_stream(obj_ref=MagicMock())
        mocked_streaming_generator_helper.assert_called_once()

    @pytest.mark.asyncio
    async def test_consume_generator_unary(self):
        grpc_proxy = self.create_grpc_proxy()
        response_bytes = b"fake-response-bytes"

        async def async_magic():
            return response_bytes

        MagicMock.__await__ = lambda x: async_magic().__await__()
        obj_ref = MagicMock()
        response = await grpc_proxy._consume_generator_unary(obj_ref=obj_ref)
        assert response.response == response_bytes

    @pytest.mark.skipif(
        not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
        reason="Not supported w/o streaming.",
    )
    @pytest.mark.asyncio
    async def test_send_request_to_replica_streaming(self):
        grpc_proxy = self.create_grpc_proxy()
        request_proto = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        proxy_request = gRPCProxyRequest(
            request_proto=request_proto,
            context=MagicMock(),
            service_method="service_method",
            stream=True,
        )
        response = await grpc_proxy.send_request_to_replica_streaming(
            request_id="fake-request-id",
            handle=MagicMock(),
            proxy_request=proxy_request,
        )
        assert isinstance(response, ProxyResponse)
        assert response.status_code == str(grpc.StatusCode.INTERNAL)


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
@patch("ray.serve._private.http_proxy.ray.get_runtime_context", MagicMock())
@patch("ray.serve._private.http_proxy.ray.get_actor", MagicMock())
class TestHTTPProxy:
    def create_http_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=MagicMock(),
        )

    def test_subclass_from_generic_proxy(self):
        http_proxy = self.create_http_proxy()
        assert isinstance(http_proxy, GenericProxy)

    def test_protocol(self):
        http_proxy = self.create_http_proxy()
        assert http_proxy.protocol == "HTTP"

    def test_success_status_code(self):
        http_proxy = self.create_http_proxy()
        assert http_proxy.success_status_code == "200"

    @pytest.mark.asyncio
    async def test_not_found(self):
        http_proxy = self.create_http_proxy()
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.not_found(proxy_request=proxy_request)
        assert isinstance(response, ProxyResponse)
        assert response.status_code == "404"

    @pytest.mark.asyncio
    async def test_draining_response(self):
        http_proxy = self.create_http_proxy()
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.draining_response(proxy_request=proxy_request)
        assert isinstance(response, ProxyResponse)
        assert response.status_code == "503"

    @pytest.mark.asyncio
    async def test_timeout_response(self):
        http_proxy = self.create_http_proxy()
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
        assert response.status_code == "408"

    @pytest.mark.asyncio
    async def test_routes_response(self):
        http_proxy = self.create_http_proxy()
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.routes_response(proxy_request=proxy_request)
        assert isinstance(response, ProxyResponse)
        assert response.status_code == "200"

    @pytest.mark.asyncio
    async def test_health_response(self):
        http_proxy = self.create_http_proxy()
        proxy_request = ASGIProxyRequest(
            scope=AsyncMock(),
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.health_response(proxy_request=proxy_request)
        assert isinstance(response, ProxyResponse)
        assert response.status_code == "200"

    @pytest.mark.asyncio
    async def test_receive_asgi_messages(self):
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
    async def test_consume_and_send_asgi_message_generator(self):
        http_proxy = self.create_http_proxy()
        obj_ref_generator = AsyncMock()
        obj_ref_generator._next_async.side_effect = [
            StopAsyncIteration(),
        ]
        await http_proxy._consume_and_send_asgi_message_generator(
            obj_ref_generator=obj_ref_generator,
            send=AsyncMock(),
        )

    @pytest.mark.asyncio
    async def test_setup_request_context_and_handle(self):
        http_proxy = self.create_http_proxy()
        handle = MagicMock()
        request_id = "fake-request-id"
        scope = {"headers": [(b"x-request-id", request_id.encode())]}
        proxy_request = ASGIProxyRequest(
            scope=scope,
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        (
            returned_handle,
            returned_request_id,
        ) = http_proxy.setup_request_context_and_handle(
            app_name="fake-app-name",
            handle=handle,
            route_path="/fake-route-path",
            proxy_request=proxy_request,
        )
        handle._set_request_protocol.assert_called_with(RequestProtocol.HTTP)
        assert returned_request_id == request_id

    @pytest.mark.skipif(
        not RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
        reason="Not supported w/ streaming.",
    )
    @pytest.mark.asyncio
    async def test_send_request_to_replica_streaming(self):
        http_proxy = self.create_http_proxy()
        request_id = "fake-request-id"
        handle = MagicMock()
        scope = {"headers": [(b"x-request-id", request_id.encode())]}
        proxy_request = ASGIProxyRequest(
            scope=scope,
            receive=AsyncMock(),
            send=AsyncMock(),
        )
        response = await http_proxy.send_request_to_replica_streaming(
            request_id=request_id,
            handle=handle,
            proxy_request=proxy_request,
        )
        assert isinstance(response, ProxyResponse)
        assert response.status_code == "500"


class TestTimeoutKeepAliveConfig:
    """Test setting keep_alive_timeout_s in config and env."""

    def get_proxy_actor(self) -> ActorHandle:
        proxy_actor_name = None
        for actor in ray._private.state.actors().values():
            if actor["ActorClassName"] == "HTTPProxyActor":
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
