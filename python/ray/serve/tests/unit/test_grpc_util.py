import pickle
from typing import Callable
from unittest.mock import Mock

import grpc
import pytest
from google.protobuf.any_pb2 import Any as AnyProto

from ray import cloudpickle
from ray.serve._private.default_impl import add_grpc_address
from ray.serve._private.grpc_util import (
    get_grpc_response_status,
    gRPCGenericServer,
)
from ray.serve._private.test_utils import FakeGrpcContext
from ray.serve.exceptions import BackPressureError
from ray.serve.grpc_util import RayServegRPCContext


class FakeGrpcServer:
    def __init__(self):
        self.address = None

    def add_insecure_port(self, address):
        self.address = address


def fake_service_handler_factory(service_method: str, stream: bool) -> Callable:
    def foo() -> bytes:
        return f"{'stream' if stream else 'unary'} call from {service_method}".encode()

    return foo


def test_grpc_server():
    """Test `gRPCGenericServer` did the correct overrides.

    When a add_servicer_to_server function is called on an instance of `gRPCGenericServer`,
    it correctly overrides `response_serializer` to None, and `unary_unary` and
    `unary_stream` to be generated from the factory function.
    """
    service_name = "ray.serve.ServeAPIService"
    method_name = "ServeRoutes"

    def add_test_servicer_to_server(servicer, server):
        rpc_method_handlers = {
            method_name: grpc.unary_unary_rpc_method_handler(
                servicer.ServeRoutes,
                request_deserializer=AnyProto.FromString,
                response_serializer=AnyProto.SerializeToString,
            ),
        }
        generic_handler = grpc.method_handlers_generic_handler(
            service_name, rpc_method_handlers
        )
        server.add_generic_rpc_handlers((generic_handler,))

    grpc_server = gRPCGenericServer(fake_service_handler_factory)
    dummy_servicer = Mock()

    # Ensure `generic_rpc_handlers` is not populated before calling
    # the add_servicer_to_server function.
    assert grpc_server.generic_rpc_handlers == []

    add_test_servicer_to_server(dummy_servicer, grpc_server)

    # `generic_rpc_handlers` should be populated after add_servicer_to_server is called.
    assert len(grpc_server.generic_rpc_handlers) == 1

    # The populated rpc handler should have the correct service name.
    rpc_handler = grpc_server.generic_rpc_handlers[0][0]
    assert rpc_handler.service_name() == service_name

    # The populated method handlers should have the correct response_serializer,
    # unary_unary, and unary_stream.
    service_method = f"/{service_name}/{method_name}"
    method_handlers = rpc_handler._method_handlers.get(service_method)
    assert method_handlers.response_serializer is None
    assert method_handlers.unary_unary() == f"unary call from {service_method}".encode()
    assert (
        method_handlers.unary_stream() == f"stream call from {service_method}".encode()
    )


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
