import asyncio
import json
import logging
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray._common.formatters import JSONFormatter
from ray.serve._private.backpressure import DrainRateEstimator, DrainRateTracker
from ray.serve._private.common import RequestMetadata
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.replica import Replica
from ray.serve._private.utils import Semaphore
from ray.serve.config import BackpressureConfig


def _make_metadata() -> RequestMetadata:
    return RequestMetadata(
        request_id="test-request",
        internal_request_id="test-internal-request",
        is_direct_ingress=True,
    )


def _make_fake_replica(max_ongoing_requests: int):
    """Minimal stand-in exposing the real slot/queue accounting methods."""
    fake = MagicMock()
    fake._num_queued_requests = 0
    fake._reserved_slots = set()
    fake._semaphore = Semaphore(lambda: max_ongoing_requests)
    fake._drain_rate_tracker = DrainRateTracker(has_pending_work=lambda: True)
    for name in (
        "_start_request",
        "_track_queued_request",
        "release_slot",
        "_can_accept_request",
        "_is_replica_quiescing",
        "_decide_direct_ingress_backpressure",
        "_get_backpressure_drain_counter_for_testing",
        "_direct_ingress_asgi",
        "_prepare_direct_ingress_grpc_request",
        "_has_pending_drain_work",
    ):
        setattr(fake, name, getattr(Replica, name).__get__(fake, Replica))
    return fake


async def _wait_for_semaphore_waiter(semaphore: Semaphore):
    for _ in range(1000):
        await asyncio.sleep(0)
        if semaphore._waiters:
            return
    raise AssertionError("no coroutine blocked on the semaphore")


class TestTrackQueuedRequest:
    def test_release_decrements_exactly_once(self):
        fake = _make_fake_replica(max_ongoing_requests=1)

        with fake._track_queued_request() as release:
            assert fake._num_queued_requests == 1
            release()
            assert fake._num_queued_requests == 0
            # Extra calls are a no-op.
            release()
            assert fake._num_queued_requests == 0

        # Exiting the block must not decrement below zero.
        assert fake._num_queued_requests == 0

    @pytest.mark.asyncio
    async def test_count_released_when_cancelled_while_waiting(self):
        """A request cancelled while blocked on the slot is released on block
        exit, as the direct-ingress handlers wire it up."""
        fake = _make_fake_replica(max_ongoing_requests=1)

        # Hold the only slot so the request blocks while queued.
        await fake._semaphore.acquire()

        async def handler():
            with fake._track_queued_request() as release:
                async with fake._start_request(_make_metadata()):
                    release()

        task = asyncio.ensure_future(handler())
        await _wait_for_semaphore_waiter(fake._semaphore)
        assert fake._num_queued_requests == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert fake._num_queued_requests == 0


class TestReplicaDrainCounter:
    """The replica's drain counter counts requests releasing their slot."""

    @pytest.mark.asyncio
    async def test_completion_counted(self):
        fake = _make_fake_replica(max_ongoing_requests=1)
        async with fake._start_request(_make_metadata()):
            assert fake._get_backpressure_drain_counter_for_testing() == 0
        assert fake._get_backpressure_drain_counter_for_testing() == 1

    @pytest.mark.asyncio
    async def test_user_error_counted(self):
        fake = _make_fake_replica(max_ongoing_requests=1)
        with pytest.raises(ValueError):
            async with fake._start_request(_make_metadata()):
                raise ValueError("user error")
        assert fake._get_backpressure_drain_counter_for_testing() == 1

    @pytest.mark.asyncio
    async def test_cancelled_while_running_counted_once(self):
        fake = _make_fake_replica(max_ongoing_requests=1)
        started = asyncio.Event()

        async def handler():
            async with fake._start_request(_make_metadata()):
                started.set()
                await asyncio.Event().wait()

        task = asyncio.ensure_future(handler())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert fake._get_backpressure_drain_counter_for_testing() == 1

    @pytest.mark.asyncio
    async def test_cancelled_while_queued_not_counted(self):
        fake = _make_fake_replica(max_ongoing_requests=1)
        await fake._semaphore.acquire()

        async def handler():
            with fake._track_queued_request() as release:
                async with fake._start_request(_make_metadata()):
                    release()

        task = asyncio.ensure_future(handler())
        await _wait_for_semaphore_waiter(fake._semaphore)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert fake._get_backpressure_drain_counter_for_testing() == 0

    @pytest.mark.asyncio
    async def test_reserved_slot_counted_only_when_executed(self):
        fake = _make_fake_replica(max_ongoing_requests=2)

        # A reservation released without executing doesn't count.
        await fake._semaphore.acquire()
        fake._reserved_slots.add("released")
        fake.release_slot("released")
        assert fake._get_backpressure_drain_counter_for_testing() == 0

        # A reservation consumed by a request counts once.
        await fake._semaphore.acquire()
        fake._reserved_slots.add("consumed")
        metadata = RequestMetadata(
            request_id="test-request",
            internal_request_id="test-internal-request",
            _reserved_slot_token="consumed",
        )
        async with fake._start_request(metadata):
            pass
        assert fake._get_backpressure_drain_counter_for_testing() == 1


def _make_rejecting_replica(
    backpressure_config: BackpressureConfig, num_queued_requests: int
):
    """A direct-ingress replica whose queue is full."""
    fake = _make_fake_replica(max_ongoing_requests=1)
    fake._quiescing = False
    fake._user_callable_initialized = True
    fake._route_prefix = "/"
    fake.max_queued_requests = 2
    fake._num_queued_requests = num_queued_requests
    fake.backpressure_config = backpressure_config
    return fake


async def _send_direct_ingress_http_request(fake, headers=()):
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "root_path": "",
        "headers": list(headers),
        "client": ("127.0.0.1", 12345),
    }
    messages = []

    async def send(msg):
        messages.append(msg)

    with patch("ray.serve._private.replica.log_backpressure_rejection") as log, patch(
        "ray.serve._private.backpressure.random.random", return_value=0.0
    ):
        await fake._direct_ingress_asgi(scope, MagicMock(), send)

    log.assert_called_once()
    message, decision = log.call_args.args
    start = messages[0]
    assert start["type"] == "http.response.start"
    headers = dict(start["headers"])
    return start["status"], headers.get(b"retry-after"), message, decision


class TestDirectIngressRejection:
    @pytest.mark.asyncio
    async def test_static_policy(self):
        fake = _make_rejecting_replica(
            BackpressureConfig(status_code=429, retry_after_s=10),
            num_queued_requests=3,
        )
        for _ in range(4):
            fake._drain_rate_tracker.record_drain()

        (
            status,
            retry_after,
            message,
            decision,
        ) = await _send_direct_ingress_http_request(fake)
        assert status == 429
        # rand=0.0 is the low end of the +/-10% jitter window.
        assert decision.post_jitter_s == 9
        assert retry_after == b"9"
        assert decision.fallback_rung == "static"
        # The exact queue depth (above the cap of 2) is used and logged.
        assert decision.observed_queue_depth == 3
        assert "num_queued_requests=3" in message
        assert decision.drain_counter == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retry_after_s,expected_rung,expected_header",
        [(5, "static_fallback", b"5"), (None, "none", None)],
    )
    async def test_cold_estimator_falls_back(
        self, retry_after_s, expected_rung, expected_header
    ):
        fake = _make_rejecting_replica(
            BackpressureConfig(
                retry_after_policy="queue_drain_rate", retry_after_s=retry_after_s
            ),
            num_queued_requests=2,
        )
        _, retry_after, _, decision = await _send_direct_ingress_http_request(fake)
        assert decision.fallback_rung == expected_rung
        assert not decision.estimator_warm
        assert retry_after == expected_header

    @pytest.mark.asyncio
    async def test_warm_estimator_computes_retry_after(self):
        fake = _make_rejecting_replica(
            BackpressureConfig(retry_after_policy="queue_drain_rate", retry_after_s=30),
            num_queued_requests=20,
        )
        now_s = [0.0]
        estimator = DrainRateEstimator(warmup_samples=1, clock=lambda: now_s[0])
        fake._drain_rate_tracker = DrainRateTracker(
            has_pending_work=lambda: True, estimator=estimator
        )
        estimator.sample(has_pending_work=True)
        for _ in range(4):
            fake._drain_rate_tracker.record_drain()
        now_s[0] += 1
        estimator.sample(has_pending_work=True)

        _, retry_after, _, decision = await _send_direct_ingress_http_request(fake)
        # 20 queued * 0.25 s per completion = 5s, then the low end of the
        # jitter window: ceil(4.5).
        assert decision.fallback_rung == "computed"
        assert decision.seconds_per_drain == pytest.approx(0.25)
        assert decision.pre_jitter_s == pytest.approx(5.0)
        assert decision.post_jitter_s == 5
        assert retry_after == b"5"
        assert decision.drain_counter == 4

    @pytest.mark.asyncio
    async def test_rejection_record_formats_as_json(self):
        fake = _make_rejecting_replica(
            BackpressureConfig(retry_after_s=10), num_queued_requests=2
        )
        records = []

        class _Handler(logging.Handler):
            def emit(self, record):
                records.append(record)

        serve_logger = logging.getLogger(SERVE_LOGGER_NAME)
        handler = _Handler()
        serve_logger.addHandler(handler)
        messages = []

        async def send(msg):
            messages.append(msg)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-request-id", b"json-request-id")],
        }
        try:
            with patch(
                "ray.serve._private.backpressure.random.random", return_value=0.5
            ):
                await fake._direct_ingress_asgi(scope, MagicMock(), send)
        finally:
            serve_logger.removeHandler(handler)

        [record] = [r for r in records if r.getMessage().startswith("Request dropped")]
        formatted = json.loads(JSONFormatter().format(record))
        assert formatted["levelname"] == "WARNING"
        assert formatted["backpressure_retry_after_policy"] == "static"
        assert formatted["backpressure_retry_after_fallback_rung"] == "static"
        assert formatted["backpressure_observed_queue_depth"] == 2
        assert formatted["backpressure_estimator_warm"] is False
        assert formatted["backpressure_estimator_num_samples"] == 0
        assert formatted["backpressure_request_id"] == "json-request-id"
        assert formatted["backpressure_drain_rate"] is None
        assert formatted["backpressure_retry_after_pre_jitter_s"] == 10
        assert formatted["backpressure_retry_after_post_jitter_s"] == 10
        assert formatted["backpressure_drain_counter"] == 0
        assert dict(messages[0]["headers"])[b"retry-after"] == b"10"

    @pytest.mark.asyncio
    async def test_stall_computes_growing_retry_after(self):
        """Queued work that never completes is a real stall: the computed
        value is the busy time so far per (at least one) drain, not the static
        fallback."""
        fake = _make_rejecting_replica(
            BackpressureConfig(retry_after_policy="queue_drain_rate", retry_after_s=5),
            num_queued_requests=2,
        )
        now_s = [0.0]
        estimator = DrainRateEstimator(warmup_samples=3, clock=lambda: now_s[0])
        fake._drain_rate_tracker = DrainRateTracker(
            has_pending_work=fake._has_pending_drain_work, estimator=estimator
        )
        fake._drain_rate_tracker._sample()
        for _ in range(3):
            now_s[0] += 1
            fake._drain_rate_tracker._sample()
        assert estimator.warm

        _, retry_after, _, decision = await _send_direct_ingress_http_request(fake)
        assert decision.fallback_rung == "computed"
        assert decision.estimator_num_samples == 3
        assert decision.drains_in_window == 0
        # 2 queued * 3 busy seconds = 6s; rand=0.0 gives ceil(6 * 0.9).
        assert decision.pre_jitter_s == 6
        assert retry_after == b"6"

    def test_pending_work_is_queued_or_running(self):
        """A replica's drains are completions, so a running request is pending
        work even with nothing queued."""
        fake = _make_fake_replica(max_ongoing_requests=1)
        fake._metrics_manager.get_num_ongoing_requests.return_value = 0
        assert not fake._has_pending_drain_work()

        fake._num_queued_requests = 1
        assert fake._has_pending_drain_work()

        fake._num_queued_requests = 0
        fake._metrics_manager.get_num_ongoing_requests.return_value = 1
        assert fake._has_pending_drain_work()


class TestDirectIngressRejectionRequestId:
    """Each rejection record names the rejected request explicitly, since the
    request context isn't installed yet when direct ingress rejects."""

    @pytest.mark.asyncio
    async def test_http_incoming_request_id(self):
        fake = _make_rejecting_replica(BackpressureConfig(), num_queued_requests=2)
        _, _, _, decision = await _send_direct_ingress_http_request(
            fake, headers=[(b"x-request-id", b"incoming-request-id")]
        )
        assert decision.request_id == "incoming-request-id"

    @pytest.mark.asyncio
    async def test_http_generated_request_id(self):
        fake = _make_rejecting_replica(BackpressureConfig(), num_queued_requests=2)
        with patch(
            "ray.serve._private.replica.generate_request_id",
            return_value="generated-request-id",
        ):
            _, _, _, decision = await _send_direct_ingress_http_request(fake)
        assert decision.request_id == "generated-request-id"

    def test_grpc_request_id(self):
        fake = _make_rejecting_replica(BackpressureConfig(), num_queued_requests=2)
        context = MagicMock()
        context.invocation_metadata.return_value = [("request_id", "grpc-request-id")]
        context.peer.return_value = "ipv4:127.0.0.1:12345"

        with patch("ray.serve._private.replica.log_backpressure_rejection") as log:
            prepared = fake._prepare_direct_ingress_grpc_request(
                "/ray.serve.UserDefinedService/Method1",
                context,
                request_input=MagicMock(),
                is_streaming=False,
            )

        assert prepared is None
        log.assert_called_once()
        message, decision = log.call_args.args
        assert message.startswith("Request dropped due to backpressure")
        assert decision.request_id == "grpc-request-id"
        assert decision.observed_queue_depth == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
