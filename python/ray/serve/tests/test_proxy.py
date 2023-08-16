import pytest
import sys
from unittest.mock import patch, MagicMock

from ray.serve._private.http_proxy import GenericProxy, gRPCProxy, HTTPProxy
from ray.serve._private.proxy_request_response import (
    ASGIProxyRequest,
    gRPCProxyRequest,
    ProxyRequest,
    ProxyResponse,
)

if sys.version_info >= (3, 8, 0):
    from unittest.mock import AsyncMock
else:
    from asyncmock import AsyncMock


@patch("ray.get_runtime_context", MagicMock())
@patch("ray.get_actor", MagicMock())
class TestGenericProxy:
    def create_generic_proxy(self):
        controller_name = "fake-controller_name"
        node_id = "fake-node_id"
        node_ip_address = "fake-node_ip_address"
        return GenericProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=MagicMock(),
        )


@patch("ray.get_runtime_context", MagicMock())
@patch("ray.get_actor", MagicMock())
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


@patch("ray.get_runtime_context", MagicMock())
@patch("ray.get_actor", MagicMock())
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

    # @pytest.mark.asyncio
    # @patch("ray.serve._private.http_util.receive_http_body", MagicMock())
    # async def test_send_request_to_replica_unary(self):
    #     http_proxy = self.create_http_proxy()
    #     proxy_request = ASGIProxyRequest(
    #         scope=AsyncMock(),
    #         receive=AsyncMock(),
    #         send=AsyncMock(),
    #     )
    #     response = await http_proxy.send_request_to_replica_unary(
    #         handle=MagicMock(),
    #         proxy_request=proxy_request,
    #     )
    #     assert isinstance(response, ProxyResponse)
    #     # with patch.object(http_proxy, 'proxy_request', mocked_proxy_request):
    #     #     pass
    #     # mocked_proxy_request.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
