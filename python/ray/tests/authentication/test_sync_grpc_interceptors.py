import grpc
import pytest

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


def test_sync_server_and_client_with_valid_token(create_sync_test_server):
    """Test sync server + client with matching token succeeds."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        # Create server with auth enabled
        server, port = create_sync_test_server(with_auth=True)

        try:
            # Client with auth interceptor via init_grpc_channel
            channel = init_grpc_channel(
                f"localhost:{port}",
                options=None,
                asynchronous=False,
            )
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()
            response = stub.HealthCheck(request, timeout=5)
            assert response is not None
        finally:
            server.stop(grace=1)


def test_sync_server_and_client_with_invalid_token(create_sync_test_server):
    """Test sync server + client with mismatched token fails."""
    server_token = generate_new_authentication_token()
    wrong_token = generate_new_authentication_token()

    with authentication_env_guard():
        # Set up server with server_token
        set_auth_mode("token")
        set_env_auth_token(server_token)
        reset_auth_token_state()

        server, port = create_sync_test_server(with_auth=True)

        try:
            # Create client channel and manually add wrong token to metadata
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)

            # Add invalid token to metadata (not using client interceptor)
            metadata = (("authorization", f"Bearer {wrong_token}"),)
            request = reporter_pb2.HealthCheckRequest()

            # Should fail with UNAUTHENTICATED
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.HealthCheck(request, metadata=metadata, timeout=5)

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            server.stop(grace=1)


def test_sync_server_with_auth_client_without_token(create_sync_test_server):
    """Test server with auth, client without token fails."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        # Set up server with auth enabled
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        server, port = create_sync_test_server(with_auth=True)

        try:
            # Create channel without auth metadata
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()

            # Should fail with UNAUTHENTICATED (no metadata provided)
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.HealthCheck(request, timeout=5)

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            server.stop(grace=1)


def test_sync_server_without_auth(create_sync_test_server):
    """Test server without auth allows unauthenticated requests."""
    with authentication_env_guard():
        # Disable auth mode
        set_auth_mode("disabled")
        reset_auth_token_state()

        # Create server without auth
        server, port = create_sync_test_server(with_auth=False)

        try:
            # Client without auth
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()

            # Should succeed without auth
            response = stub.HealthCheck(request, timeout=5)
            assert response is not None
        finally:
            server.stop(grace=1)


def test_sync_server_with_auth_disabled_allows_all(create_sync_test_server):
    """Test server allows requests when auth mode is disabled."""
    with authentication_env_guard():
        # Disable auth mode globally
        set_auth_mode("disabled")
        reset_auth_token_state()

        # Even though we call create_sync_test_server with with_auth=True,
        # the server won't enforce auth because auth mode is disabled
        server, port = create_sync_test_server(with_auth=True)

        try:
            # Client without token
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            request = reporter_pb2.HealthCheckRequest()

            # Should succeed because auth is disabled
            response = stub.HealthCheck(request, timeout=5)
            assert response is not None
        finally:
            server.stop(grace=1)


def test_sync_streaming_response_with_valid_token(create_sync_test_server):
    """Test sync server streaming response (unary_stream) works with valid token."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        # Create server with auth enabled
        server, port = create_sync_test_server(with_auth=True)

        try:
            # Client with auth interceptor via init_grpc_channel
            channel = init_grpc_channel(
                f"localhost:{port}",
                options=None,
                asynchronous=False,
            )
            stub = reporter_pb2_grpc.LogServiceStub(channel)
            request = reporter_pb2.StreamLogRequest(log_file_name="test.log")

            # Stream the response - this tests the unary_stream RPC path
            chunks = []
            for response in stub.StreamLog(request, timeout=5):
                chunks.append(response.data)

            # Verify we got all 3 chunks from the test service
            assert len(chunks) == 3
            assert chunks == [b"chunk0", b"chunk1", b"chunk2"]
        finally:
            server.stop(grace=1)


def test_sync_streaming_response_without_token_fails(create_sync_test_server):
    """Test sync server streaming response fails without token."""
    token = generate_new_authentication_token()

    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(token)
        reset_auth_token_state()

        server, port = create_sync_test_server(with_auth=True)

        try:
            # Client without auth token
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = reporter_pb2_grpc.LogServiceStub(channel)
            request = reporter_pb2.StreamLogRequest(log_file_name="test.log")

            # Should fail with UNAUTHENTICATED when trying to iterate
            with pytest.raises(grpc.RpcError) as exc_info:
                for _ in stub.StreamLog(request, timeout=5):
                    pass

            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            server.stop(grace=1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
