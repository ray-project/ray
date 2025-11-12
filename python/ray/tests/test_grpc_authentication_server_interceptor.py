"""Unit tests for gRPC server authentication interceptor."""

import uuid

import grpc
import pytest
from grpc import aio as aiogrpc

from ray._private.authentication.grpc_authentication_server_interceptor import (
    AsyncAuthenticationServerInterceptor,
)
from ray._private.authentication_test_utils import (
    authentication_env_guard,
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)

# Create a simple test service for testing
from ray.core.generated import reporter_pb2, reporter_pb2_grpc


class TestReporterServicer(reporter_pb2_grpc.ReporterServiceServicer):
    """Simple test servicer for testing authentication."""

    async def HealthCheck(self, request, context):
        """Return a health check response."""
        return reporter_pb2.HealthCheckReply()


@pytest.fixture
async def auth_server_and_port():
    """Create a gRPC server with authentication interceptor."""
    interceptor = AsyncAuthenticationServerInterceptor()
    server = aiogrpc.server(interceptors=[interceptor])

    servicer = TestReporterServicer()
    reporter_pb2_grpc.add_ReporterServiceServicer_to_server(servicer, server)

    port = server.add_insecure_port("[::]:0")
    await server.start()

    yield server, port

    await server.stop(grace=1)


@pytest.fixture
async def no_auth_server_and_port():
    """Create a gRPC server without authentication interceptor."""
    server = aiogrpc.server()

    servicer = TestReporterServicer()
    reporter_pb2_grpc.add_ReporterServiceServicer_to_server(servicer, server)

    port = server.add_insecure_port("[::]:0")
    await server.start()

    yield server, port

    await server.stop(grace=1)


@pytest.mark.asyncio
async def test_server_interceptor_allows_valid_token(auth_server_and_port):
    """Test that server interceptor allows requests with valid tokens."""
    with authentication_env_guard():
        # Set up token authentication
        token = uuid.uuid4().hex
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        # Get server from fixture
        _, port = auth_server_and_port

        # Create client with valid token in metadata
        async with aiogrpc.insecure_channel(f"localhost:{port}") as channel:
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            # Add valid token to metadata
            metadata = (("authorization", f"Bearer {token}"),)

            request = reporter_pb2.HealthCheckRequest()
            response = await stub.HealthCheck(request, metadata=metadata)

            # Should succeed (response exists and is not None)
            assert response is not None


@pytest.mark.asyncio
async def test_server_interceptor_rejects_invalid_token(auth_server_and_port):
    """Test that server interceptor rejects requests with invalid tokens."""
    with authentication_env_guard():
        # Set up token authentication
        correct_token = uuid.uuid4().hex
        set_auth_mode("token")
        set_env_auth_token(correct_token)
        reset_auth_token_state()

        # Get server from fixture
        _, port = auth_server_and_port

        # Create client with invalid token in metadata
        async with aiogrpc.insecure_channel(f"localhost:{port}") as channel:
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            # Add invalid token to metadata
            wrong_token = uuid.uuid4().hex
            metadata = (("authorization", f"Bearer {wrong_token}"),)

            request = reporter_pb2.HealthCheckRequest()

            # Should fail with UNAUTHENTICATED status
            with pytest.raises(grpc.RpcError) as exc_info:
                await stub.HealthCheck(request, metadata=metadata)

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED


@pytest.mark.asyncio
async def test_server_interceptor_rejects_missing_token(auth_server_and_port):
    """Test that server interceptor rejects requests without tokens."""
    with authentication_env_guard():
        # Set up token authentication
        token = uuid.uuid4().hex
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        # Get server from fixture
        _, port = auth_server_and_port

        # Create client without token in metadata
        async with aiogrpc.insecure_channel(f"localhost:{port}") as channel:
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            request = reporter_pb2.HealthCheckRequest()

            # Should fail with UNAUTHENTICATED status
            with pytest.raises(grpc.RpcError) as exc_info:
                await stub.HealthCheck(request)

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED


@pytest.mark.asyncio
async def test_server_interceptor_disabled_auth_allows_all(auth_server_and_port):
    """Test that when auth is disabled, all requests are allowed."""
    with authentication_env_guard():
        # Set auth mode to disabled (or don't set it at all)
        set_auth_mode("disabled")
        reset_auth_token_state()

        # Get server from fixture
        _, port = auth_server_and_port

        # Create client without any token
        async with aiogrpc.insecure_channel(f"localhost:{port}") as channel:
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            request = reporter_pb2.HealthCheckRequest()
            response = await stub.HealthCheck(request)

            # Should succeed even without token
            assert response is not None


@pytest.mark.asyncio
async def test_no_interceptor_allows_all_requests(no_auth_server_and_port):
    """Test that server without interceptor allows all requests."""
    with authentication_env_guard():
        # Even with token auth enabled, server without interceptor allows all
        token = uuid.uuid4().hex
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        _, port = no_auth_server_and_port

        # Create client without token
        async with aiogrpc.insecure_channel(f"localhost:{port}") as channel:
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            request = reporter_pb2.HealthCheckRequest()
            response = await stub.HealthCheck(request)

            # Should succeed (no interceptor means no auth check)
            assert response is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
