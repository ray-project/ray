"""gRPC server interceptor for token-based authentication."""

import logging
from typing import Awaitable, Callable, Optional

import grpc
from grpc import aio as aiogrpc

from ray._private.authentication.authentication_constants import (
    AUTHORIZATION_HEADER_NAME,
    TOKEN_AUTH_ERROR_MESSAGE,
)
from ray._private.authentication.authentication_utils import (
    is_token_auth_enabled,
    validate_request_token,
)

logger = logging.getLogger(__name__)

_UNAUTHENTICATED_MESSAGE = "Invalid or missing authentication token"


def _authentication_error(metadata: tuple) -> Optional[str]:
    """Return the UNAUTHENTICATED message for a request that fails authentication.

    Args:
        metadata: gRPC metadata tuple of (key, value) pairs
    Returns:
        None if authentication succeeds, otherwise the UNAUTHENTICATED message.
    """
    # Extract authorization header from metadata
    auth_header = None
    for key, value in metadata:
        if key.lower() == AUTHORIZATION_HEADER_NAME:
            auth_header = value
            break

    if not is_token_auth_enabled():
        # Reject clients that send a token so that they opt out too.
        if auth_header is not None:
            logger.debug("Rejecting a request with a token because auth is disabled")
            return TOKEN_AUTH_ERROR_MESSAGE
        return None

    if not auth_header:
        logger.warning("Authentication required but no authorization header provided")
        return _UNAUTHENTICATED_MESSAGE

    if not validate_request_token(auth_header):
        return _UNAUTHENTICATED_MESSAGE
    return None


async def _abort_if_unauthenticated(context) -> None:
    """Abort the RPC if authentication fails."""
    error_message = _authentication_error(context.invocation_metadata())
    if error_message:
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, error_message)


def _wrap_async_unary_response(behavior):
    """Wrap a unary response RPC method to validate authentication first."""
    if behavior is None:
        return None

    async def wrapped(request_or_iterator, context):
        await _abort_if_unauthenticated(context)
        return await behavior(request_or_iterator, context)

    return wrapped


def _wrap_async_stream_response(behavior):
    """Wrap a streaming response RPC method to validate authentication first."""
    if behavior is None:
        return None

    async def wrapped(request_or_iterator, context):
        await _abort_if_unauthenticated(context)
        async for response in behavior(request_or_iterator, context):
            yield response

    return wrapped


def _wrap_sync_behavior(behavior):
    """Wrap an RPC method to validate authentication first."""
    if behavior is None:
        return None

    def wrapped(request_or_iterator, context):
        error_message = _authentication_error(context.invocation_metadata())
        if error_message:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, error_message)
        return behavior(request_or_iterator, context)

    return wrapped


class _AuthenticatedHandler:
    """Wraps an ``RpcMethodHandler`` so every call authenticates first.

    Module level on purpose: ``intercept_service`` runs once per RPC, so a class
    nested in it would leak a fresh type object per request until gc runs.
    """

    def __init__(self, original_handler, unary_wrapper_func, stream_wrapper_func):
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


class AsyncAuthenticationServerInterceptor(aiogrpc.ServerInterceptor):
    """Async gRPC server interceptor that validates authentication tokens.

    With RAY_AUTH_MODE=token, a request without a valid Bearer token in the
    "authorization" metadata header is rejected with UNAUTHENTICATED status. With
    auth disabled, a request that carries that header is rejected with
    UNAUTHENTICATED and the same token auth error a client without a token gets.
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

        return _AuthenticatedHandler(
            handler, _wrap_async_unary_response, _wrap_async_stream_response
        )


class SyncAuthenticationServerInterceptor(grpc.ServerInterceptor):
    """Synchronous gRPC server interceptor that validates authentication tokens.

    With RAY_AUTH_MODE=token, a request without a valid Bearer token in the
    "authorization" metadata header is rejected with UNAUTHENTICATED status. With
    auth disabled, a request that carries that header is rejected with
    UNAUTHENTICATED and the same token auth error a client without a token gets.
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

        # The sync handler wraps every arity the same way.
        return _AuthenticatedHandler(handler, _wrap_sync_behavior, _wrap_sync_behavior)
