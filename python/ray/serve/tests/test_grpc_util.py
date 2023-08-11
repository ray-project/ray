import pytest
from typing import Callable


from ray.serve._private.grpc_util import (
    create_serve_grpc_server,
    DummyServicer,
    gRPCServer,
)


@pytest.fixture
def fake_service_handler_factory(*args, **kwargs) -> Callable:
    def foo() -> bytes:
        return b"foo"

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


def test_create_serve_grpc_server(fake_service_handler_factory):
    """

    :param fake_service_handler_factory:
    :return:
    """
    grpc_server = create_serve_grpc_server(
        service_handler_factory=fake_service_handler_factory
    )
    assert isinstance(grpc_server, gRPCServer)
    assert grpc_server.service_handler_factory == fake_service_handler_factory


def test_grpc_server(fake_service_handler_factory):
    server = gRPCServer(
        thread_pool=None,
        generic_handlers=(),
        interceptors=(),
        options=(),
        maximum_concurrent_rpcs=None,
        compression=None,
        service_handler_factory=fake_service_handler_factory,
    )
    assert server.service_handler_factory == fake_service_handler_factory


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
