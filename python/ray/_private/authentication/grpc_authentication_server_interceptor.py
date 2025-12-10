"""gRPC server interceptor for token-based authentication."""

import logging
from typing import Awaitable, Callable

import grpc
from grpc import aio as aiogrpc

from ray._private.authentication.authentication_constants import (
    AUTHORIZATION_HEADER_NAME,
)
from ray._private.authentication.authentication_utils import (
    is_token_auth_enabled,
    validate_request_token,
)

logger = logging.getLogger(__name__)


def _authenticate_request(metadata: tuple) -> bool:
    """Authenticate incoming request. currently only supports token authentication.

    Args:
        metadata: gRPC metadata tuple of (key, value) pairs
    Returns:
        True if authentication succeeds or is not required, False otherwise
    """
    if not is_token_auth_enabled():
        return True

    # Extract authorization header from metadata
    auth_header = None
    for key, value in metadata:
        if key.lower() == AUTHORIZATION_HEADER_NAME:
            auth_header = value
            break

    if not auth_header:
        logger.warning("Authentication required but no authorization header provided")
        return False

    # Validate the token format and value
    # validate_request_token returns bool (True if valid, False otherwise)
    return validate_request_token(auth_header)


class AsyncAuthenticationServerInterceptor(aiogrpc.ServerInterceptor):
    """Async gRPC server interceptor that validates authentication tokens.

    This interceptor checks the "authorization" metadata header for a valid
    Bearer token when token authentication is enabled via RAY_AUTH_MODE=token.

    If the token is missing or invalid, the request is rejected with UNAUTHENTICATED status.
    """

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        """Intercept service calls to validate authentication.

        This method is called once per RPC to get the handler. We wrap the handler
        to validate authentication before executing the actual RPC method.
        """
        # Get the actual handler
        handler = await continuation(handler_call_details)

        if handler is None:
            return None

        async def _abort_if_unauthenticated(context):
            """Abort the RPC if authentication fails."""
            if not _authenticate_request(context.invocation_metadata()):
                await context.abort(
                    grpc.StatusCode.UNAUTHENTICATED,
                    "Invalid or missing authentication token",
                )

        # Wrap the RPC behavior with authentication check
        def wrap_unary_response(behavior):
            """Wrap a unary response RPC method to validate authentication first."""
            if behavior is None:
                return None

            async def wrapped(request_or_iterator, context):
                await _abort_if_unauthenticated(context)
                return await behavior(request_or_iterator, context)

            return wrapped

        def wrap_stream_response(behavior):
            """Wrap a streaming response RPC method to validate authentication first."""
            if behavior is None:
                return None

            async def wrapped(request_or_iterator, context):
                await _abort_if_unauthenticated(context)
                async for response in behavior(request_or_iterator, context):
                    yield response

            return wrapped

        # Create a wrapper class that implements RpcMethodHandler interface
        class AuthenticatedHandler:
            """Wrapper handler that validates authentication."""

            def __init__(
                self, original_handler, unary_wrapper_func, stream_wrapper_func
            ):
                self._original = original_handler
                self._wrap_unary = unary_wrapper_func
                self._wrap_stream = stream_wrapper_func

            @property
            def request_streaming(self):
                return self._original.request_streaming

            @property
            def response_streaming(self):
                return self._original.response_streaming

            @property
            def request_deserializer(self):
                return self._original.request_deserializer

            @property
            def response_serializer(self):
                return self._original.response_serializer

            @property
            def unary_unary(self):
                return self._wrap_unary(self._original.unary_unary)

            @property
            def unary_stream(self):
                return self._wrap_stream(self._original.unary_stream)

            @property
            def stream_unary(self):
                return self._wrap_unary(self._original.stream_unary)

            @property
            def stream_stream(self):
                return self._wrap_stream(self._original.stream_stream)

        return AuthenticatedHandler(handler, wrap_unary_response, wrap_stream_response)


class SyncAuthenticationServerInterceptor(grpc.ServerInterceptor):
    """Synchronous gRPC server interceptor that validates authentication tokens.

    This interceptor checks the "authorization" metadata header for a valid
    Bearer token when token authentication is enabled via RAY_AUTH_MODE=token.
    If the token is missing or invalid, the request is rejected with UNAUTHENTICATED status.
    """

    def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], grpc.RpcMethodHandler],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        """Intercept service calls to validate authentication.

        This method is called once per RPC to get the handler. We wrap the handler
        to validate authentication before executing the actual RPC method.
        """
        # Get the actual handler
        handler = continuation(handler_call_details)

        if handler is None:
            return None

        # Wrap the RPC behavior with authentication check
        def wrap_rpc_behavior(behavior):
            """Wrap an RPC method to validate authentication first."""
            if behavior is None:
                return None

            def wrapped(request_or_iterator, context):
                if not _authenticate_request(context.invocation_metadata()):
                    context.abort(
                        grpc.StatusCode.UNAUTHENTICATED,
                        "Invalid or missing authentication token",
                    )
                return behavior(request_or_iterator, context)

            return wrapped

        # Create a wrapper class that implements RpcMethodHandler interface
        class AuthenticatedHandler:
            """Wrapper handler that validates authentication."""

            def __init__(self, original_handler, wrapper_func):
                self._original = original_handler
                self._wrap = wrapper_func

            @property
            def request_streaming(self):
                return self._original.request_streaming

            @property
            def response_streaming(self):
                return self._original.response_streaming

            @property
            def request_deserializer(self):
                return self._original.request_deserializer

            @property
            def response_serializer(self):
                return self._original.response_serializer

            @property
            def unary_unary(self):
                return self._wrap(self._original.unary_unary)

            @property
            def unary_stream(self):
                return self._wrap(self._original.unary_stream)

            @property
            def stream_unary(self):
                return self._wrap(self._original.stream_unary)

            @property
            def stream_stream(self):
                return self._wrap(self._original.stream_stream)

        return AuthenticatedHandler(handler, wrap_rpc_behavior)
