"""gRPC client interceptor for token-based authentication."""

import logging
from collections import namedtuple
from typing import Tuple

import grpc
from grpc import aio as aiogrpc

from ray._raylet import AuthenticationTokenLoader

logger = logging.getLogger(__name__)


# Named tuple to hold client call details
_ClientCallDetails = namedtuple(
    "_ClientCallDetails",
    ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"),
)


def _get_authentication_metadata_tuple() -> Tuple[Tuple[str, str], ...]:
    """Get gRPC metadata tuple for authentication. Currently only supported for token authentication.

    Returns:
        tuple: Empty tuple or ((AUTHORIZATION_HEADER_NAME, "Bearer <token>"),)
    """
    token_loader = AuthenticationTokenLoader.instance()
    if not token_loader.has_token():
        return ()

    headers = token_loader.get_token_for_http_header()

    # Convert HTTP header dict to gRPC metadata tuple
    # gRPC expects: (("key", "value"), ...)
    return tuple((k, v) for k, v in headers.items())


class SyncAuthenticationMetadataClientInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Synchronous gRPC client interceptor that adds authentication metadata."""

    def _intercept_call_details(self, client_call_details):
        """Helper method to add authentication metadata to client call details."""
        metadata = list(client_call_details.metadata or [])
        metadata.extend(_get_authentication_metadata_tuple())

        return _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=getattr(client_call_details, "wait_for_ready", None),
            compression=getattr(client_call_details, "compression", None),
        )

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = self._intercept_call_details(client_call_details)
        return continuation(new_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = self._intercept_call_details(client_call_details)
        return continuation(new_details, request)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        new_details = self._intercept_call_details(client_call_details)
        return continuation(new_details, request_iterator)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        new_details = self._intercept_call_details(client_call_details)
        return continuation(new_details, request_iterator)


def _intercept_call_details_async(client_call_details):
    """Helper to add authentication metadata to client call details (async version)."""
    metadata = list(client_call_details.metadata or [])
    metadata.extend(_get_authentication_metadata_tuple())

    return _ClientCallDetails(
        method=client_call_details.method,
        timeout=client_call_details.timeout,
        metadata=metadata,
        credentials=client_call_details.credentials,
        wait_for_ready=getattr(client_call_details, "wait_for_ready", None),
        compression=getattr(client_call_details, "compression", None),
    )


# NOTE: gRPC aio's Channel.__init__ uses if-elif chains to categorize interceptors,
# so a single class inheriting from multiple interceptor types will only be registered
# for the first matching type. We must use separate classes for each RPC type.
# See: https://github.com/grpc/grpc/blob/master/src/python/grpcio/grpc/aio/_channel.py


class _AsyncUnaryUnaryAuthInterceptor(aiogrpc.UnaryUnaryClientInterceptor):
    """Async unary-unary interceptor that adds authentication metadata."""

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = _intercept_call_details_async(client_call_details)
        return await continuation(new_details, request)


class _AsyncUnaryStreamAuthInterceptor(aiogrpc.UnaryStreamClientInterceptor):
    """Async unary-stream interceptor that adds authentication metadata."""

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = _intercept_call_details_async(client_call_details)
        return await continuation(new_details, request)


class _AsyncStreamUnaryAuthInterceptor(aiogrpc.StreamUnaryClientInterceptor):
    """Async stream-unary interceptor that adds authentication metadata."""

    async def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        new_details = _intercept_call_details_async(client_call_details)
        return await continuation(new_details, request_iterator)


class _AsyncStreamStreamAuthInterceptor(aiogrpc.StreamStreamClientInterceptor):
    """Async stream-stream interceptor that adds authentication metadata."""

    async def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        new_details = _intercept_call_details_async(client_call_details)
        return await continuation(new_details, request_iterator)


def get_async_auth_interceptors():
    """Get a list of async authentication interceptors for all RPC types.

    Returns a list of separate interceptor instances, one for each RPC type,
    because gRPC aio channels only register multi-inheritance interceptors
    for the first matching type.

    Returns:
        List of interceptor instances for unary-unary, unary-stream,
        stream-unary, and stream-stream RPCs.
    """
    return [
        _AsyncUnaryUnaryAuthInterceptor(),
        _AsyncUnaryStreamAuthInterceptor(),
        _AsyncStreamUnaryAuthInterceptor(),
        _AsyncStreamStreamAuthInterceptor(),
    ]
