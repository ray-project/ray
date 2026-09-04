import pickle
from typing import Callable
from unittest.mock import Mock

import grpc
import pytest
from google.protobuf.any_pb2 import Any as AnyProto

from ray import cloudpickle
from ray.serve._private.default_impl import add_grpc_address
from ray.serve._private.grpc_util import (
    GRPC_MAX_STATUS_DETAILS_LENGTH,
    _truncate_message,
    enable_server_reflection,
    get_grpc_response_status,
    get_service_names,
    gRPCGenericServer,
)
from ray.serve._private.proxy_request_response import gRPCStreamingType
from ray.serve._private.test_utils import FakeGrpcContext
from ray.serve.exceptions import BackPressureError, gRPCStatusError
from ray.serve.grpc_util import RayServegRPCContext


class FakeGrpcServer:
    def __init__(self):
        self.address = None

    def add_insecure_port(self, address):
        self.address = address


def fake_service_handler_factory(
    service_method: str, streaming_type: gRPCStreamingType
) -> Callable:
    def foo() -> bytes:
        return f"{streaming_type.value} call from {service_method}".encode()

    return foo


def add_servicer_to_server(service_name: str, method_name: str, servicer, server):
    rpc_method_handlers = {
        method_name: grpc.unary_unary_rpc_method_handler(
            getattr(servicer, method_name),
            request_deserializer=AnyProto.FromString,
            response_serializer=AnyProto.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        service_name, rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


def test_grpc_server():
    """Test `gRPCGenericServer` did the correct overrides.

    When a add_servicer_to_server function is called on an instance of `gRPCGenericServer`,
    it correctly overrides `response_serializer` to None, and `unary_unary`,
    `unary_stream`, `stream_unary`, and `stream_stream` to be generated from the
    factory function.
    """
    service_name = "ray.serve.ServeAPIService"
    method_name = "ServeRoutes"

    grpc_server = gRPCGenericServer(fake_service_handler_factory)
    dummy_servicer = Mock()

    # Ensure `generic_rpc_handlers` is not populated before calling
    # the add_servicer_to_server function.
    assert grpc_server.generic_rpc_handlers == []

    add_servicer_to_server(service_name, method_name, dummy_servicer, grpc_server)

    # `generic_rpc_handlers` should be populated after add_servicer_to_server is called.
    assert len(grpc_server.generic_rpc_handlers) == 1

    # The populated rpc handler should have the correct service name.
    rpc_handler = grpc_server.generic_rpc_handlers[0][0]
    assert rpc_handler.service_name() == service_name

    # The populated method handlers should have the correct response_serializer,
    # unary_unary, unary_stream, stream_unary, and stream_stream.
    service_method = f"/{service_name}/{method_name}"
    method_handlers = rpc_handler._method_handlers.get(service_method)
    assert method_handlers.response_serializer is None
    assert (
        method_handlers.unary_unary()
        == f"unary_unary call from {service_method}".encode()
    )
    assert (
        method_handlers.unary_stream()
        == f"unary_stream call from {service_method}".encode()
    )
    assert (
        method_handlers.stream_unary()
        == f"stream_unary call from {service_method}".encode()
    )
    assert (
        method_handlers.stream_stream()
        == f"stream_stream call from {service_method}".encode()
    )


def test_grpc_server_passthrough_service():
    """Handlers for a passthrough service are registered unmodified, while
    handlers for other services are still overridden."""
    grpc_server = gRPCGenericServer(fake_service_handler_factory)
    grpc_server.add_passthrough_service("test.PassthroughService")
    dummy_servicer = Mock()

    add_servicer_to_server(
        "test.PassthroughService", "Echo", dummy_servicer, grpc_server
    )
    add_servicer_to_server("test.UserService", "Predict", dummy_servicer, grpc_server)

    passthrough_handler, user_handler = (
        handlers[0] for handlers in grpc_server.generic_rpc_handlers
    )

    passthrough_method = passthrough_handler._method_handlers[
        "/test.PassthroughService/Echo"
    ]
    assert passthrough_method.unary_unary == dummy_servicer.Echo
    assert passthrough_method.response_serializer is not None

    user_method = user_handler._method_handlers["/test.UserService/Predict"]
    assert user_method.unary_unary != dummy_servicer.Predict
    assert user_method.response_serializer is None


def test_get_service_names():
    """Service names are derived from registered method handler keys."""
    grpc_server = gRPCGenericServer(fake_service_handler_factory)
    dummy_servicer = Mock()

    add_servicer_to_server("foo.FooService", "MethodA", dummy_servicer, grpc_server)
    add_servicer_to_server("foo.FooService", "MethodB", dummy_servicer, grpc_server)
    add_servicer_to_server("bar.BarService", "MethodC", dummy_servicer, grpc_server)

    assert get_service_names(grpc_server.generic_rpc_handlers) == {
        "foo.FooService",
        "bar.BarService",
    }


def test_enable_server_reflection():
    """Reflection handlers survive un-clobbered and advertise user-defined
    services, but not Serve's built-in API service."""
    pytest.importorskip("grpc_reflection")
    from grpc_reflection.v1alpha import reflection

    from ray.serve.generated.serve_pb2_grpc import (
        add_RayServeAPIServiceServicer_to_server,
    )

    user_service_name = "test.UserService"
    grpc_server = gRPCGenericServer(fake_service_handler_factory)
    dummy_servicer = Mock()
    add_RayServeAPIServiceServicer_to_server(dummy_servicer, grpc_server)
    add_servicer_to_server(user_service_name, "Predict", dummy_servicer, grpc_server)

    enable_server_reflection(grpc_server)

    handlers_by_service = {
        service_name: handlers[0]
        for handlers in grpc_server.generic_rpc_handlers
        for service_name in get_service_names([handlers])
    }
    reflection_handler = handlers_by_service[reflection.SERVICE_NAME]
    reflection_method = reflection_handler._method_handlers[
        f"/{reflection.SERVICE_NAME}/ServerReflectionInfo"
    ]

    # The reflection handler executes on the server itself, not overridden
    # to route to replicas.
    assert reflection_method.response_serializer is not None
    reflection_servicer = reflection_method.stream_stream.__self__
    advertised_services = reflection_servicer._service_names
    assert user_service_name in advertised_services
    assert reflection.SERVICE_NAME in advertised_services
    assert "ray.serve.RayServeAPIService" not in advertised_services

    # User handlers are still overridden.
    user_method = handlers_by_service[user_service_name]._method_handlers[
        f"/{user_service_name}/Predict"
    ]
    assert user_method.response_serializer is None
    assert user_method.unary_unary != dummy_servicer.Predict


def test_ray_serve_grpc_context_serializable():
    """RayServegRPCContext should be serializable."""
    context = RayServegRPCContext(FakeGrpcContext())
    pickled_context = pickle.dumps(context)
    deserialized_context = pickle.loads(pickled_context)
    assert deserialized_context.__dict__ == context.__dict__

    cloudpickled_context = cloudpickle.dumps(context)
    deserialized_context = pickle.loads(cloudpickled_context)
    assert deserialized_context.__dict__ == context.__dict__


def test_add_grpc_address():
    """Test `add_grpc_address` adds the address to the gRPC server."""
    fake_grpc_server = FakeGrpcServer()
    grpc_address = "fake_address:50051"
    assert fake_grpc_server.address is None
    add_grpc_address(fake_grpc_server, grpc_address)
    assert fake_grpc_server.address == grpc_address


def test_get_grpc_response_status_backpressure_error():
    """Test that BackPressureError returns RESOURCE_EXHAUSTED status."""
    backpressure_error = BackPressureError(
        num_queued_requests=10, max_queued_requests=5
    )

    status = get_grpc_response_status(
        exc=backpressure_error, request_timeout_s=30.0, request_id="test_request_123"
    )

    assert status.code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert status.is_error is True
    assert status.message == backpressure_error.message


def test_get_grpc_response_status_grpc_status_error():
    """Test that gRPCStatusError preserves user-set status code."""
    original_error = RuntimeError("test error")
    user_status_code = grpc.StatusCode.INVALID_ARGUMENT
    user_details = "Invalid argument provided"

    grpc_status_error = gRPCStatusError(
        original_exception=original_error,
        code=user_status_code,
        details=user_details,
    )

    status = get_grpc_response_status(
        exc=grpc_status_error, request_timeout_s=30.0, request_id="test_request_123"
    )

    assert status.code == user_status_code
    assert status.is_error is True
    assert status.message == user_details


def test_get_grpc_response_status_grpc_status_error_no_details():
    """Test that gRPCStatusError without details uses original exception message."""
    original_error = RuntimeError("original error message")
    user_status_code = grpc.StatusCode.RESOURCE_EXHAUSTED

    grpc_status_error = gRPCStatusError(
        original_exception=original_error,
        code=user_status_code,
        details=None,
    )

    status = get_grpc_response_status(
        exc=grpc_status_error, request_timeout_s=30.0, request_id="test_request_123"
    )

    assert status.code == user_status_code
    assert status.is_error is True
    assert "original error message" in status.message


def test_truncate_message_short():
    """Test that short messages are not truncated."""
    short_message = "short error message"
    result = _truncate_message(short_message)
    assert result == short_message


def test_truncate_message_long():
    """Test that long messages are truncated."""
    # Create a message longer than the max length
    long_message = "a" * (GRPC_MAX_STATUS_DETAILS_LENGTH + 1000)
    result = _truncate_message(long_message)

    assert len(result) <= GRPC_MAX_STATUS_DETAILS_LENGTH
    assert result.endswith("... [truncated]")


def test_truncate_message_at_boundary():
    """Test truncation at the exact boundary."""
    # Create a message exactly at the limit
    exact_message = "a" * GRPC_MAX_STATUS_DETAILS_LENGTH
    result = _truncate_message(exact_message)
    assert result == exact_message
    assert len(result) == GRPC_MAX_STATUS_DETAILS_LENGTH


def test_get_grpc_response_status_truncates_long_message():
    """Test that long error messages are truncated in INTERNAL errors."""
    long_message = "a" * (GRPC_MAX_STATUS_DETAILS_LENGTH + 1000)
    long_error = RuntimeError(long_message)

    status = get_grpc_response_status(
        exc=long_error, request_timeout_s=30.0, request_id="test_request_123"
    )

    assert status.code == grpc.StatusCode.INTERNAL
    assert status.is_error is True
    assert len(status.message) <= GRPC_MAX_STATUS_DETAILS_LENGTH
    assert status.message.endswith("... [truncated]")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
