from typing import Any, Dict, List, Optional, Sequence, Tuple

import grpc
from grpc.aio._server import Server


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
        options=(),
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


class RayServegRPCContext:
    """Context manager to set and get gRPC context.

    This class implenents the most of the methods on ServicerContext
    (see: https://grpc.github.io/grpc/python/grpc.html#grpc.ServicerContext) so it's
    serializable and can pass with the request to be used on the deployment.
    """

    def __init__(self, grpc_context: grpc._cython.cygrpc._ServicerContext):
        self._auth_context = grpc_context.auth_context()
        self._code = grpc_context.code()
        self._details = grpc_context.details()
        self._invocation_metadata = [
            (key, value) for key, value in grpc_context.invocation_metadata()
        ]
        self._peer = grpc_context.peer()
        self._peer_identities = grpc_context.peer_identities()
        self._peer_identity_key = grpc_context.peer_identity_key()
        self._trailing_metadata = [
            (key, value) for key, value in grpc_context.trailing_metadata()
        ]
        self._compression = None

    def auth_context(self) -> Dict[str, Any]:
        return self._auth_context

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def invocation_metadata(self) -> List[Tuple[str, str]]:
        return self._invocation_metadata

    def peer(self) -> str:
        return self._peer

    def peer_identities(self) -> Optional[bytes]:
        return self._peer_identities

    def peer_identity_key(self) -> Optional[str]:
        return self._peer_identity_key

    def trailing_metadata(self) -> List[Tuple[str, str]]:
        return self._trailing_metadata

    def set_code(self, code: grpc.StatusCode):
        self._code = code

    def set_compression(self, compression: grpc.Compression):
        self._compression = compression

    def set_details(self, details: str):
        self._details = details

    def set_trailing_metadata(self, trailing_metadata: List[Tuple[str, str]]):
        self._trailing_metadata += trailing_metadata

    def set_on_grpc_context(self, grpc_context: grpc._cython.cygrpc._ServicerContext):
        if self._code:
            grpc_context.set_code(self._code)

        if self._compression:
            grpc_context.set_compression(self._compression)

        if self._details:
            grpc_context.set_details(self._details)

        if self._trailing_metadata:
            grpc_context.set_trailing_metadata(self._trailing_metadata)
