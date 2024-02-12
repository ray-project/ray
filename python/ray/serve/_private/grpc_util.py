from typing import Sequence

import grpc
from grpc.aio._server import Server

from ray.serve._private.constants import SERVE_GRPC_OPTIONS


class gRPCServer(Server):
    """Custom gRPC server to override gRPC method methods.

    Original implementation see: https://github.com/grpc/grpc/blob/
        60c1701f87cacf359aa1ad785728549eeef1a4b0/src/python/grpcio/grpc/aio/_server.py
    """

    def __init__(self, service_handler_factory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.service_handler_factory = service_handler_factory
        self.generic_rpc_handlers = []

    def add_generic_rpc_handlers(
        self, generic_rpc_handlers: Sequence[grpc.GenericRpcHandler]
    ):
        """Override generic_rpc_handlers before adding to the gRPC server.

        This function will override all user defined handlers to have
            1. None `response_serializer` so the server can pass back the
            raw protobuf bytes to the user.
            2. `unary_unary` is always calling the unary function generated via
            `self.service_handler_factory`
            3. `unary_stream` is always calling the streaming function generated via
            `self.service_handler_factory`
        """
        serve_rpc_handlers = {}
        rpc_handler = generic_rpc_handlers[0]
        for service_method, method_handler in rpc_handler._method_handlers.items():
            serve_method_handler = method_handler._replace(
                response_serializer=None,
                unary_unary=self.service_handler_factory(
                    service_method=service_method,
                    stream=False,
                ),
                unary_stream=self.service_handler_factory(
                    service_method=service_method,
                    stream=True,
                ),
            )
            serve_rpc_handlers[service_method] = serve_method_handler
        generic_rpc_handlers[0]._method_handlers = serve_rpc_handlers
        self.generic_rpc_handlers.append(generic_rpc_handlers)
        super().add_generic_rpc_handlers(generic_rpc_handlers)


def create_serve_grpc_server(service_handler_factory):
    """Custom function to create Serve's gRPC server.

    This function works similar to `grpc.server()`, but it creates a Serve defined
    gRPC server in order to override the `unary_unary` and `unary_stream` methods

    See: https://grpc.github.io/grpc/python/grpc.html#grpc.server
    """
    return gRPCServer(
        thread_pool=None,
        generic_handlers=(),
        interceptors=(),
        options=SERVE_GRPC_OPTIONS,
        maximum_concurrent_rpcs=None,
        compression=None,
        service_handler_factory=service_handler_factory,
    )


class DummyServicer:
    """Dummy servicer for gRPC server to call on.

    This is a dummy class that just pass through when calling on any method.
    User defined servicer function will attempt to add the method on this class to the
    gRPC server, but our gRPC server will override the caller to call gRPCProxy.
    """

    def __getattr__(self, attr):
        # No-op pass through. Just need this to act as the callable.
        pass
