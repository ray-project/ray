import grpc
import pytest
from grpc import aio as aiogrpc

from ray._private.authentication.authentication_token_generator import (
    generate_new_authentication_token,
)
from ray._private.authentication_test_utils import (
    authentication_env_guard,
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)
from ray._private.grpc_utils import init_grpc_channel
from ray.core.generated import reporter_pb2, reporter_pb2_grpc


@pytest.mark.asyncio
async def test_async_server_and_client_with_valid_token(create_async_test_server):
    """Test async server + client with matching token succeeds."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        # Create server with auth enabled
        server, port = await create_async_test_server(with_auth=True)

        try:
            # Client with auth interceptor via init_grpc_channel
            channel = init_grpc_channel(
                f"localhost:{port}",
                options=None,
                asynchronous=True,
            )
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()
            response = await stub.HealthCheck(request, timeout=5)
            assert response is not None
        finally:
            await server.stop(grace=1)


@pytest.mark.asyncio
async def test_async_server_and_client_with_invalid_token(create_async_test_server):
    """Test async server + client with mismatched token fails."""
    server_token = generate_new_authentication_token()
    wrong_token = generate_new_authentication_token()

    with authentication_env_guard():
        # Set up server with server_token
        set_auth_mode("token")
        set_env_auth_token(server_token)
        reset_auth_token_state()

        server, port = await create_async_test_server(with_auth=True)

        try:
            # Create client channel and manually add wrong token to metadata
            channel = aiogrpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            # Add invalid token to metadata (not using client interceptor)
            metadata = (("authorization", f"Bearer {wrong_token}"),)
            request = reporter_pb2.HealthCheckRequest()

            # Should fail with UNAUTHENTICATED
            with pytest.raises(grpc.RpcError) as exc_info:
                await stub.HealthCheck(request, metadata=metadata, timeout=5)

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            await server.stop(grace=1)


@pytest.mark.asyncio
async def test_async_server_with_auth_client_without_token(create_async_test_server):
    """Test async server with auth, client without token fails."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        # Set up server with auth enabled
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        server, port = await create_async_test_server(with_auth=True)

        try:
            # Create channel without auth metadata
            channel = aiogrpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()

            # Should fail with UNAUTHENTICATED (no metadata provided)
            with pytest.raises(grpc.RpcError) as exc_info:
                await stub.HealthCheck(request, timeout=5)

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            await server.stop(grace=1)


@pytest.mark.asyncio
async def test_async_server_without_auth(create_async_test_server):
    """Test async server without auth allows unauthenticated requests."""
    with authentication_env_guard():
        # Disable auth mode
        set_auth_mode("disabled")
        reset_auth_token_state()

        # Create server without auth
        server, port = await create_async_test_server(with_auth=False)

        try:
            # Client without auth
            channel = aiogrpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()

            # Should succeed without auth
            response = await stub.HealthCheck(request, timeout=5)
            assert response is not None
        finally:
            await server.stop(grace=1)


@pytest.mark.asyncio
async def test_async_server_with_auth_disabled_allows_all(create_async_test_server):
    """Test async server allows requests when auth mode is disabled."""
    with authentication_env_guard():
        # Disable auth mode globally
        set_auth_mode("disabled")
        reset_auth_token_state()

        # Even though we call create_async_test_server with with_auth=True,
        # the server won't enforce auth because auth mode is disabled
        server, port = await create_async_test_server(with_auth=True)

        try:
            # Client without token
            channel = aiogrpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()

            # Should succeed because auth is disabled
            response = await stub.HealthCheck(request, timeout=5)
            assert response is not None
        finally:
            await server.stop(grace=1)


@pytest.mark.asyncio
async def test_async_streaming_response_with_valid_token(create_async_test_server):
    """Test async server streaming response (unary_stream) works with valid token."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        # Create server with auth enabled
        server, port = await create_async_test_server(with_auth=True)

        try:
            # Client with auth interceptor via init_grpc_channel
            channel = init_grpc_channel(
                f"localhost:{port}",
                options=None,
                asynchronous=True,
            )
            stub = reporter_pb2_grpc.LogServiceStub(channel)
            request = reporter_pb2.StreamLogRequest(log_file_name="test.log")

            # Stream the response - this tests the unary_stream RPC path
            chunks = []
            async for response in stub.StreamLog(request, timeout=5):
                chunks.append(response.data)

            # Verify we got all 3 chunks from the test service
            assert len(chunks) == 3
            assert chunks == [b"chunk0", b"chunk1", b"chunk2"]
        finally:
            await server.stop(grace=1)


@pytest.mark.asyncio
async def test_async_streaming_response_without_token_fails(create_async_test_server):
    """Test async server streaming response fails without token."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        server, port = await create_async_test_server(with_auth=True)

        try:
            # Client without auth token
            channel = aiogrpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.LogServiceStub(channel)
            request = reporter_pb2.StreamLogRequest(log_file_name="test.log")

            # Should fail with UNAUTHENTICATED when trying to iterate
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stub.StreamLog(request, timeout=5):
                    pass

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            await server.stop(grace=1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
