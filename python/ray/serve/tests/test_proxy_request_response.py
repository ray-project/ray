import pickle
from typing import Generator
from unittest.mock import MagicMock

import grpc
import pytest
from ray.serve._private.proxy_request_response import (
    ASGIProxyRequest,
    gRPCProxyRequest,
    ProxyRequest,
    ProxyResponse,
)
from ray.serve.generated import serve_pb2
from ray.serve._private.common import gRPCRequest, StreamingHTTPRequest


class TestASGIProxyRequest:
    def create_asgi_serve_request(self, scope: dict) -> ASGIProxyRequest:
        receive = MagicMock()
        send = MagicMock()
        return ASGIProxyRequest(scope=scope, receive=receive, send=send)

    def test_request_type(self):
        """Test calling request_type on an instance of ASGIProxyRequest.

        When the request_type is not passed into the scope, it returns empty string.
        When the request_type is passed into the scope, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.request_type == ""

        request_type = "fake-request_type"
        serve_request = self.create_asgi_serve_request(scope={"type": request_type})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.request_type == request_type

    def test_client(self):
        """Test calling client on an instance of ASGIProxyRequest.

        When the client is not passed into the scope, it returns empty string.
        When the request_type is passed into the scope, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.client == ""

        client = "fake-client"
        serve_request = self.create_asgi_serve_request(scope={"client": client})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.client == client

    def test_method(self):
        """Test calling method on an instance of ASGIProxyRequest.

        When the method is not passed into the scope, it returns "WEBSOCKET". When
        the method is passed into the scope, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.method == "WEBSOCKET"

        method = "fake-method"
        serve_request = self.create_asgi_serve_request(scope={"method": method})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.method == method.upper()

    def test_root_path(self):
        """Test calling root_path on an instance of ASGIProxyRequest.

        When the root_path is not passed into the scope, it returns empty string.
        When calling set_root_path, it correctly sets the root_path. When the
        root_path is passed into the scope, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.root_path == ""

        root_path = "fake-root_path"
        serve_request.set_root_path(root_path)
        assert serve_request.root_path == root_path

        serve_request = self.create_asgi_serve_request(scope={"root_path": root_path})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.root_path == root_path

    def test_path(self):
        """Test calling path on an instance of ASGIProxyRequest.

        When the path is not passed into the scope, it returns empty string.
        When calling set_path, it correctly sets the path. When the
        path is passed into the scope, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.path == ""

        path = "fake-path"
        serve_request.set_path(path)
        assert serve_request.path == path

        serve_request = self.create_asgi_serve_request(scope={"path": path})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.path == path

    def test_headers(self):
        """Test calling headers on an instance of ASGIProxyRequest.

        When the headers are not passed into the scope, it returns empty list.
        When the headers are passed into the scope, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.headers == []

        headers = [(b"fake-header-key", b"fake-header-value")]
        serve_request = self.create_asgi_serve_request(scope={"headers": headers})
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.headers == headers

    def test_request_object(self):
        """Test calling request_object on an instance of ASGIProxyRequest.

        When the request_object is called, it returns a StreamingHTTPRequest object
        with the correct pickled_asgi_scope and http_proxy_handle.
        """
        proxy_handle = MagicMock()
        headers = [(b"fake-header-key", b"fake-header-value")]
        scope = {"headers": headers}
        serve_request = self.create_asgi_serve_request(scope=scope)
        request_object = serve_request.request_object(proxy_handle=proxy_handle)

        assert isinstance(request_object, StreamingHTTPRequest)
        assert pickle.loads(request_object.pickled_asgi_scope) == scope
        assert request_object.http_proxy_handle == proxy_handle

    def test_is_route_request(self):
        """Test calling is_route_request on an instance of ASGIProxyRequest.

        When the is_route_request is called with `/-/routes`, it returns true.
        When the is_route_request is called with other path, it returns false.
        """
        scope = {"path": "/-/routes"}
        serve_request = self.create_asgi_serve_request(scope=scope)
        assert serve_request.is_route_request is True

        scope = {"path": "/foo"}
        serve_request = self.create_asgi_serve_request(scope=scope)
        assert serve_request.is_route_request is False

    def test_is_health_request(self):
        """Test calling is_health_request on an instance of ASGIProxyRequest.

        When the is_health_request is called with `/-/healthz`, it returns true.
        When the is_health_request is called with other path, it returns false.
        """
        scope = {"path": "/-/healthz"}
        serve_request = self.create_asgi_serve_request(scope=scope)
        assert serve_request.is_health_request is True

        scope = {"path": "/foo"}
        serve_request = self.create_asgi_serve_request(scope=scope)
        assert serve_request.is_health_request is False


class TestgRPCProxyRequest:
    def test_calling_list_applications_method(self):
        """Test initialize gRPCProxyRequest with list applications service method.

        When the gRPCProxyRequest is initialized with list application service method,
        calling is_route_request should return true and calling is_health_request
        should return false. `send_status_code()` and `send_details()` should also work
        accordingly to be able to send the into back to the client.
        """
        context = MagicMock()
        service_method = "/ray.serve.RayServeAPIService/ListApplications"
        serve_request = gRPCProxyRequest(
            request_proto=MagicMock(),
            context=context,
            service_method=service_method,
            stream=MagicMock(),
        )
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.is_route_request is True
        assert serve_request.is_health_request is False

        status_code = grpc.StatusCode.OK
        serve_request.send_status_code(status_code=status_code)
        context.set_code.assert_called_with(status_code)

        message = "success"
        serve_request.send_details(message=message)
        context.set_details.assert_called_with(message)

    def test_calling_healthz_method(self):
        """Test initialize gRPCProxyRequest with healthz service method.

        When the gRPCProxyRequest is initialized with healthz service method, calling
        is_route_request should return false and calling is_health_request
        should return true. `send_status_code()` and `send_details()` should
        also work accordingly to be able to send the into back to the client.
        """
        context = MagicMock()
        service_method = "/ray.serve.RayServeAPIService/Healthz"
        serve_request = gRPCProxyRequest(
            request_proto=MagicMock(),
            context=context,
            service_method=service_method,
            stream=MagicMock(),
        )
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.is_route_request is False
        assert serve_request.is_health_request is True

        status_code = grpc.StatusCode.OK
        serve_request.send_status_code(status_code=status_code)
        context.set_code.assert_called_with(status_code)

        message = "success"
        serve_request.send_details(message=message)
        context.set_details.assert_called_with(message)

    def test_calling_user_defined_method(self):
        """Test initialize gRPCProxyRequest with user defined service method.

        When the gRPCProxyRequest is initialized with user defined service method,
        all attributes should be setup accordingly. Calling both is_route_request
        and is_health_request should return false. `send_request_id()` should
        also work accordingly to be able to send the into back to the client.
        `request_object()` generates a gRPCRequest object with the correct attributes.
        """
        request_proto = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        application = "fake-application"
        request_id = "fake-request_id"
        multiplexed_model_id = "fake-multiplexed_model_id"
        metadata = (
            ("foo", "bar"),
            ("application", application),
            ("request_id", request_id),
            ("multiplexed_model_id", multiplexed_model_id),
        )
        context = MagicMock()
        context.invocation_metadata.return_value = metadata
        method_name = "Method1"
        service_method = f"/custom.defined.Service/{method_name}"

        serve_request = gRPCProxyRequest(
            request_proto=request_proto,
            context=context,
            service_method=service_method,
            stream=MagicMock(),
        )
        assert isinstance(serve_request, ProxyRequest)
        assert serve_request.route_path == application
        assert pickle.loads(serve_request.request) == request_proto
        assert serve_request.method_name == method_name.lower()
        assert serve_request.app_name == application
        assert serve_request.request_id == request_id
        assert serve_request.multiplexed_model_id == multiplexed_model_id
        assert serve_request.is_route_request is False
        assert serve_request.is_health_request is False

        serve_request.send_request_id(request_id=request_id)
        context.set_trailing_metadata.assert_called_with([("request_id", request_id)])

        proxy_handle = MagicMock()
        request_object = serve_request.request_object(proxy_handle=proxy_handle)
        assert isinstance(request_object, gRPCRequest)
        assert pickle.loads(request_object.grpc_user_request) == request_proto
        assert request_object.grpc_proxy_handle == proxy_handle


def test_serve_response():
    """Test ProxyResponse.

    When a ProxyResponse object is initialized with status_code, response, and
    streaming_response, the object is able to return the correct values.
    """
    status_code = "200"
    response = b"unary_response"

    def streaming_response(i: int) -> bytes:
        return f"{i} from generator".encode()

    def test_generator() -> Generator[bytes, None, None]:
        for i in range(10):
            yield streaming_response(i=i)

    serve_response = ProxyResponse(
        status_code=status_code,
        response=response,
        streaming_response=test_generator(),
    )

    assert serve_response.status_code == status_code
    assert serve_response.response == response
    for idx, resp in enumerate(serve_response.streaming_response):
        assert resp == streaming_response(idx)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
