import asyncio
import sys
from unittest.mock import MagicMock

import pytest

from ray.serve._private.common import RequestMetadata
from ray.serve._private.replica import Replica


def _make_metadata(*, is_direct_ingress: bool) -> RequestMetadata:
    return RequestMetadata(
        request_id="test-request",
        internal_request_id="test-internal-request",
        is_direct_ingress=is_direct_ingress,
    )


class FakeUvicornServer:
    """Stand-in for `uvicorn.Server` that records when `should_exit` is set."""

    def __init__(self, events):
        # Write through __dict__ to avoid recursing into __setattr__.
        self.__dict__["_events"] = events
        self.__dict__["should_exit"] = False

    def __setattr__(self, name, value):
        if name == "should_exit":
            self._events.append(("http_should_exit", value))
        self.__dict__[name] = value


class FakeGrpcServer:
    """Stand-in for a `grpc.aio` server that records graceful stops."""

    def __init__(self, events, name):
        self._events = events
        self._name = name

    async def stop(self, grace):
        self._events.append((self._name, grace))


def _make_shutdown_fake(
    *,
    initialized: bool = True,
    with_http_server: bool = False,
    http_task=None,
    with_grpc_server: bool = False,
    grpc_task=None,
    internal_grpc_port=12345,
    grace_period_s: float = 1.0,
):
    """Builds a minimal stand-in for `Replica` for `perform_graceful_shutdown`.

    Returns the fake and the ordered list of events it records.
    """
    events = []

    fake = MagicMock()
    fake._shutting_down = False
    fake._quiescing = False
    fake._user_callable_initialized = initialized
    fake._ingress = False
    fake._deployment_config.graceful_shutdown_timeout_s = grace_period_s
    fake._direct_ingress_http_server = (
        FakeUvicornServer(events) if with_http_server else None
    )
    fake._direct_ingress_http_server_task = http_task
    fake._direct_ingress_grpc_server = (
        FakeGrpcServer(events, "direct_ingress_grpc_stop") if with_grpc_server else None
    )
    fake._direct_ingress_grpc_server_task = grpc_task
    fake._internal_grpc_port = internal_grpc_port
    fake._server = FakeGrpcServer(events, "inter_deployment_stop")

    async def drain(min_draining_period_s):
        # Quiescing must only start AFTER the drain: during the drain the
        # replica must keep serving normally.
        assert fake._quiescing is False
        events.append(("drain", min_draining_period_s))

    fake._drain_ongoing_requests = drain

    async def shutdown():
        events.append(("shutdown",))

    fake.shutdown = shutdown

    return fake, events


class TestCanAcceptRequestWhileQuiescing:
    def test_handle_path_rejected_when_quiescing(self):
        fake = MagicMock()
        fake._quiescing = True

        assert (
            Replica._can_accept_request(fake, _make_metadata(is_direct_ingress=False))
            is False
        )

    def test_direct_ingress_not_rejected_when_quiescing(self):
        """Direct ingress requests must NOT be rejected while quiescing.

        The HTTP/gRPC servers are shut down gracefully during quiescing, so
        any direct ingress request that still arrives is served to
        completion; rejecting it would surface a client-visible error
        instead of a safe retry.
        """
        fake = MagicMock()
        fake._quiescing = True
        fake.max_queued_requests = -1
        fake._num_queued_requests = 0

        assert (
            Replica._can_accept_request(fake, _make_metadata(is_direct_ingress=True))
            is True
        )

    def test_handle_path_accepted_when_not_quiescing(self):
        fake = MagicMock()
        fake._quiescing = False
        fake._semaphore.locked.return_value = False

        assert (
            Replica._can_accept_request(fake, _make_metadata(is_direct_ingress=False))
            is True
        )


class TestPerformGracefulShutdown:
    @pytest.mark.asyncio
    async def test_quiesces_and_stops_servers_in_order(self):
        """Drain → quiesce → graceful server stops → shutdown, in order."""
        loop = asyncio.get_running_loop()
        http_task = loop.create_future()
        # Simulate the uvicorn serve task exiting promptly once
        # `should_exit` is set.
        http_task.set_result(None)
        grpc_task = MagicMock()

        fake, events = _make_shutdown_fake(
            with_http_server=True,
            http_task=http_task,
            with_grpc_server=True,
            grpc_task=grpc_task,
            grace_period_s=1.0,
        )

        await Replica.perform_graceful_shutdown(fake)

        assert fake._shutting_down is True
        assert fake._quiescing is True
        assert [e[0] for e in events] == [
            "drain",
            "http_should_exit",
            "direct_ingress_grpc_stop",
            "inter_deployment_stop",
            "shutdown",
        ]
        # The grace passed to each stop is the REMAINING shutdown budget at
        # that step (deadline-based), so it must be positive and within the
        # configured budget.
        for name, *args in events:
            if name.endswith("_stop"):
                assert 0.0 < args[0] <= 1.0
        # The direct ingress gRPC server task is cancelled after the
        # graceful stop completes.
        assert grpc_task.cancel.called

    @pytest.mark.asyncio
    async def test_http_graceful_close_timeout_falls_back_to_cancel(self):
        """If the HTTP server doesn't exit within the grace period, the
        server task is cancelled (the previous abrupt behavior)."""
        loop = asyncio.get_running_loop()
        # A serve task that never exits on its own.
        http_task = loop.create_future()

        fake, events = _make_shutdown_fake(
            with_http_server=True,
            http_task=http_task,
            grace_period_s=0.05,
        )

        await Replica.perform_graceful_shutdown(fake)

        # `asyncio.wait_for` cancels the awaited task on timeout.
        assert http_task.cancelled()
        # Shutdown still completes despite the timeout.
        assert fake._quiescing is True
        assert events[-1] == ("shutdown",)

    @pytest.mark.asyncio
    async def test_abrupt_cancel_when_server_object_missing(self):
        """If only the server task exists (no server object), fall back to
        the abrupt cancel."""
        http_task = MagicMock()
        fake, events = _make_shutdown_fake(http_task=http_task)

        await Replica.perform_graceful_shutdown(fake)

        assert http_task.cancel.called
        assert [e[0] for e in events] == [
            "drain",
            "inter_deployment_stop",
            "shutdown",
        ]

    @pytest.mark.asyncio
    async def test_uninitialized_replica_skips_drain_and_server_stops(self):
        """A replica that never initialized has no servers to stop and never
        served traffic, so only the final shutdown step runs."""
        fake, events = _make_shutdown_fake(
            initialized=False,
            internal_grpc_port=None,
        )

        await Replica.perform_graceful_shutdown(fake)

        assert events == [("shutdown",)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
