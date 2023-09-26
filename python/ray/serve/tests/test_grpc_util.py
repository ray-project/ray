from typing import Callable

import grpc
import pytest
from google.protobuf.any_pb2 import Any as AnyProto

from ray.serve._private.grpc_util import (
    DummyServicer,
    create_serve_grpc_server,
    gRPCServer,
)


def fake_service_handler_factory(service_method: str, stream: bool) -> Callable:
    def foo() -> bytes:
        return f"{'stream' if stream else 'unary'} call from {service_method}".encode()

    return foo


def test_dummy_servicer_can_take_any_methods():
    """Test an instance of DummyServicer can be called with any method name without
    error.

    When dummy_servicer is called with any custom defined methods, it won't raise error.
    """
    dummy_servicer = DummyServicer()
    dummy_servicer.foo
    dummy_servicer.bar
    dummy_servicer.baz
    dummy_servicer.my_method
    dummy_servicer.Predict


def test_create_serve_grpc_server():
    """Test `create_serve_grpc_server()` creates the correct server.

    The server created by `create_serve_grpc_server()` should be an instance of
    Serve defined `gRPCServer`. Also, the handler factory passed with the function
    should be used to initialize the `gRPCServer`.
    """
    grpc_server = create_serve_grpc_server(
        service_handler_factory=fake_service_handler_factory
    )
    assert isinstance(grpc_server, gRPCServer)
    assert grpc_server.service_handler_factory == fake_service_handler_factory


def test_grpc_server():
    """Test `gRPCServer` did the correct overrides.

    When a add_servicer_to_server function is called on an instance of `gRPCServer`,
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

    grpc_server = gRPCServer(
        thread_pool=None,
        generic_handlers=(),
        interceptors=(),
        options=(),
        maximum_concurrent_rpcs=None,
        compression=None,
        service_handler_factory=fake_service_handler_factory,
    )
    dummy_servicer = DummyServicer()

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
