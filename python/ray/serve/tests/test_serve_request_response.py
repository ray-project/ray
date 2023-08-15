import pickle
from typing import Generator
from unittest.mock import MagicMock

import grpc
import pytest
from ray.serve._private.serve_request_response import (
    ASGIServeRequest,
    gRPCServeRequest,
    ServeRequest,
    ServeResponse,
)
from ray.serve.generated import serve_pb2


class TestASGIServeRequest:
    def create_asgi_serve_request(self, scope: dict) -> ASGIServeRequest:
        receive = MagicMock()
        send = MagicMock()
        return ASGIServeRequest(scope=scope, receive=receive, send=send)

    def test_request_type(self):
        """Test calling request_type on an instance of ASGIServeRequest.
        When the request_type is not passed into the scope, it returns empty string.
        When the request_type is passed into the scopt, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.request_type == ""

        request_type = "fake-request_type"
        serve_request = self.create_asgi_serve_request(scope={"type": request_type})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.request_type == request_type

    def test_client(self):
        """Test calling client on an instance of ASGIServeRequest.
        When the client is not passed into the scope, it returns empty string.
        When the request_type is passed into the scopt, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.client == ""

        client = "fake-client"
        serve_request = self.create_asgi_serve_request(scope={"client": client})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.client == client

    def test_method(self):
        """Test calling request_type on an instance of ASGIServeRequest.
        When the request_type is not passed into the scope, it returns empty string.
        When the request_type is passed into the scopt, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.method == "WEBSOCKET"

        method = "fake-method"
        serve_request = self.create_asgi_serve_request(scope={"method": method})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.method == method.upper()

    def test_root_path(self):
        """Test calling request_type on an instance of ASGIServeRequest.
        When the request_type is not passed into the scope, it returns empty string.
        When the request_type is passed into the scopt, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.root_path == ""

        root_path = "fake-root_path"
        serve_request.set_root_path(root_path)
        assert serve_request.root_path == root_path

        serve_request = self.create_asgi_serve_request(scope={"root_path": root_path})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.root_path == root_path

    def test_path(self):
        """Test calling request_type on an instance of ASGIServeRequest.
        When the request_type is not passed into the scope, it returns empty string.
        When the request_type is passed into the scopt, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.path == ""

        path = "fake-path"
        serve_request.set_path(path)
        assert serve_request.path == path

        serve_request = self.create_asgi_serve_request(scope={"path": path})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.path == path

    def test_headers(self):
        """Test calling request_type on an instance of ASGIServeRequest.
        When the request_type is not passed into the scope, it returns empty string.
        When the request_type is passed into the scopt, it returns the correct value.
        """
        serve_request = self.create_asgi_serve_request(scope={})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.headers == []

        headers = [(b"fake-header-key", b"fake-header-value")]
        serve_request = self.create_asgi_serve_request(scope={"headers": headers})
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.headers == headers


class TestgRPCServeRequest:
    def test_calling_routes_method(self):
        """Test initialize gRPCServeRequest with routes service method.
        When the gRPCServeRequest is initialized with routes service method, route_path
        should be set to "/-/routes". `send_status_code()` and `send_details()` should
        also work accordingly to be able to send the into back to the client.
        """
        context = MagicMock()
        service_method = "/ray.serve.ServeAPIService/ServeRoutes"
        serve_request = gRPCServeRequest(
            request_proto=MagicMock(),
            context=context,
            match_target=MagicMock(),
            service_method=service_method,
            stream=MagicMock(),
        )
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.route_path == "/-/routes"

        status_code = grpc.StatusCode.OK
        serve_request.send_status_code(status_code=status_code)
        context.set_code.assert_called_with(status_code)

        message = "success"
        serve_request.send_details(message=message)
        context.set_details.assert_called_with(message)

    def test_calling_healthz_method(self):
        """Test initialize gRPCServeRequest with healthz service method.
        When the gRPCServeRequest is initialized with healthz service method, route_path
        should be set to "/-/healthz". `send_status_code()` and `send_details()` should
        also work accordingly to be able to send the into back to the client.
        """
        context = MagicMock()
        service_method = "/ray.serve.ServeAPIService/ServeHealthz"
        serve_request = gRPCServeRequest(
            request_proto=MagicMock(),
            context=context,
            match_target=MagicMock(),
            service_method=service_method,
            stream=MagicMock(),
        )
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.route_path == "/-/healthz"

        status_code = grpc.StatusCode.OK
        serve_request.send_status_code(status_code=status_code)
        context.set_code.assert_called_with(status_code)

        message = "success"
        serve_request.send_details(message=message)
        context.set_details.assert_called_with(message)

    def test_calling_user_defined_method(self):
        """Test initialize gRPCServeRequest with user defined service method.
        When the gRPCServeRequest is initialized with user defined service method,
        all attributes should be setup accordingly. `send_request_id()` should
        also work accordingly to be able to send the into back to the client.
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

        def match_target(app_name: str) -> str:
            if app_name == application:
                return "matched_path"
            return "unmatched_path"

        serve_request = gRPCServeRequest(
            request_proto=request_proto,
            context=context,
            match_target=match_target,
            service_method=service_method,
            stream=MagicMock(),
        )
        assert isinstance(serve_request, ServeRequest)
        assert serve_request.route_path == "matched_path"
        assert pickle.loads(serve_request.request) == request_proto
        assert serve_request.method_name == method_name.lower()
        assert serve_request.app_name == application
        assert serve_request.request_id == request_id
        assert serve_request.multiplexed_model_id == multiplexed_model_id

        serve_request.send_request_id(request_id=request_id)
        context.set_trailing_metadata.assert_called_with([("request_id", request_id)])


def test_serve_response():
    """Test ServeResponse.
    When a ServeResponse object is initialized with status_code, response, and
    streaming_response, the object is able to return the correct values.
    """
    status_code = "200"
    response = b"unary_response"

    def streaming_response(i: int) -> bytes:
        return f"{i} from generator".encode()

    def test_generator() -> Generator[bytes, None, None]:
        for i in range(10):
            yield streaming_response(i=i)

    serve_response = ServeResponse(
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
