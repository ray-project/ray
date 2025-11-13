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


class AsyncAuthenticationMetadataClientInterceptor(
    aiogrpc.UnaryUnaryClientInterceptor,
    aiogrpc.UnaryStreamClientInterceptor,
    aiogrpc.StreamUnaryClientInterceptor,
    aiogrpc.StreamStreamClientInterceptor,
):
    """Async gRPC client interceptor that adds authentication metadata."""

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

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = self._intercept_call_details(client_call_details)
        return await continuation(new_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = self._intercept_call_details(client_call_details)
        return await continuation(new_details, request)

    async def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        new_details = self._intercept_call_details(client_call_details)
        return await continuation(new_details, request_iterator)

    async def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        new_details = self._intercept_call_details(client_call_details)
        return await continuation(new_details, request_iterator)
