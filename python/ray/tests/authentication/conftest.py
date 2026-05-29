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
from ray._private.grpc_utils import create_grpc_server_with_interceptors
from ray.core.generated import reporter_pb2, reporter_pb2_grpc


class SyncReporterService(reporter_pb2_grpc.ReporterServiceServicer):
    """Simple synchronous test service for testing auth interceptors."""

    def HealthCheck(self, request, context):
        """Simple health check endpoint."""
        return reporter_pb2.HealthCheckReply()


class AsyncReporterService(reporter_pb2_grpc.ReporterServiceServicer):
    """Simple asynchronous test service for testing auth interceptors."""

    async def HealthCheck(self, request, context):
        """Simple health check endpoint (async version)."""
        return reporter_pb2.HealthCheckReply()


class SyncLogService(reporter_pb2_grpc.LogServiceServicer):
    """Simple synchronous log service for testing streaming auth interceptors."""

    def StreamLog(self, request, context):
        """Streaming log endpoint - yields test data chunks."""
        for i in range(3):
            yield reporter_pb2.StreamLogReply(data=f"chunk{i}".encode())


class AsyncLogService(reporter_pb2_grpc.LogServiceServicer):
    """Simple asynchronous log service for testing streaming auth interceptors."""

    async def StreamLog(self, request, context):
        """Streaming log endpoint (async version) - yields test data chunks."""
        for i in range(3):
            yield reporter_pb2.StreamLogReply(data=f"chunk{i}".encode())


def _create_test_server_base(
    *,
    asynchronous: bool,
    with_auth: bool,
    reporter_servicer_cls,
    log_servicer_cls,
):
    """Internal helper to create sync or async test server with optional auth."""

    if with_auth:
        # Auth is enabled - server will use interceptor
        server = create_grpc_server_with_interceptors(
            max_workers=None if asynchronous else 10,
            thread_name_prefix="test_server",
            options=None,
            asynchronous=asynchronous,
        )
    else:
        # Auth is disabled - create server without helper (no interceptor)
        if asynchronous:
            server = aiogrpc.server(options=None)
        else:
            from concurrent import futures

            server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=10),
                options=None,
            )

    # Add test services
    reporter_servicer = reporter_servicer_cls()
    reporter_pb2_grpc.add_ReporterServiceServicer_to_server(reporter_servicer, server)

    log_servicer = log_servicer_cls()
    reporter_pb2_grpc.add_LogServiceServicer_to_server(log_servicer, server)

    # Bind to ephemeral port
    port = server.add_insecure_port("[::]:0")

    return server, port


@pytest.fixture
def create_sync_test_server():
    """Factory to create synchronous gRPC test server.

    Returns a function that creates a test server and returns (server, port).
    The server must be stopped by the caller.
    """

    def _create(with_auth=True):
        server, port = _create_test_server_base(
            asynchronous=False,
            with_auth=with_auth,
            reporter_servicer_cls=SyncReporterService,
            log_servicer_cls=SyncLogService,
        )
        server.start()
        return server, port

    return _create


@pytest.fixture
def create_async_test_server():
    """Factory to create asynchronous gRPC test server.

    Returns an async function that creates a test server and returns (server, port).
    The server must be stopped by the caller.
    """

    async def _create(with_auth=True):
        server, port = _create_test_server_base(
            asynchronous=True,
            with_auth=with_auth,
            reporter_servicer_cls=AsyncReporterService,
            log_servicer_cls=AsyncLogService,
        )
        await server.start()
        return server, port

    return _create


@pytest.fixture
def setup_auth_environment():
    """Set up authentication environment with test token."""
    test_token = generate_new_authentication_token()
    with authentication_env_guard():
        set_auth_mode("token")
        set_env_auth_token(test_token)
        reset_auth_token_state()
        yield test_token
