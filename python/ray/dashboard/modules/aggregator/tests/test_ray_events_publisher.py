import pytest
import sys
import asyncio
from unittest.mock import Mock
from concurrent.futures import ThreadPoolExecutor

from ray._common.test_utils import async_wait_for_condition
from ray.dashboard.modules.aggregator.publisher.ray_event_publisher import (
    RayEventsPublisher,
    NoopPublisher,
)
from ray.dashboard.modules.aggregator.publisher.async_publisher_client import (
    AsyncHttpPublisherClient,
    PublishStats,
    PublisherClientInterface,
)
from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)
from ray.core.generated import events_base_event_pb2
from typing import Optional
from google.protobuf.timestamp_pb2 import Timestamp


class MockPublisherClient(PublisherClientInterface):
    """Test implementation of PublisherClientInterface."""

    def __init__(
        self,
        publish_result: Optional[PublishStats] = None,
        batch_size: int = 1,
        side_effect=None,
    ):
        self.publish_result = publish_result or PublishStats(True, 1, 0)
        self.batch_size = batch_size
        self.publish_calls = []
        self._side_effect = side_effect

    async def publish(self, batch) -> PublishStats:
        self.publish_calls.append(batch)
        if self._side_effect is not None:
            if asyncio.iscoroutinefunction(self._side_effect):
                return await self._side_effect(batch)
            return self._side_effect(batch)
        return self.publish_result

    def count_num_events_in_batch(self, batch) -> int:
        return self.batch_size

    async def close(self) -> None:
        pass


@pytest.fixture
def base_kwargs():
    """Common kwargs for publisher initialization."""
    return {
        "name": "test",
        "max_retries": 2,
        "initial_backoff": 0,
        "max_backoff": 0,
        "jitter_ratio": 0,
    }


class TestRayEventsPublisher:
    """Test the main RayEventsPublisher functionality."""

    @pytest.mark.asyncio
    async def test_publish_with_retries_failure_then_success(self, base_kwargs):
        """Test publish that fails then succeeds."""
        call_count = {"count": 0}

        # fail the first publish call but succeed on retry
        def side_effect(batch):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return PublishStats(False, 0, 0)
            return PublishStats(True, 1, 0)

        client = MockPublisherClient(side_effect=side_effect)
        event_buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=10)
        publisher = RayEventsPublisher(
            name=base_kwargs["name"],
            publish_client=client,
            event_buffer=event_buffer,
            max_retries=base_kwargs["max_retries"],
            initial_backoff=base_kwargs["initial_backoff"],
            max_backoff=base_kwargs["max_backoff"],
            jitter_ratio=base_kwargs["jitter_ratio"],
        )

        task = asyncio.create_task(publisher.run_forever())
        try:
            # ensure consumer is registered
            await publisher.wait_until_running(2.0)
            # Enqueue one event into buffer
            e = events_base_event_pb2.RayEvent(
                event_id=b"1",
                source_type=events_base_event_pb2.RayEvent.SourceType.CORE_WORKER,
                event_type=events_base_event_pb2.RayEvent.EventType.TASK_DEFINITION_EVENT,
                timestamp=Timestamp(seconds=123, nanos=0),
                severity=events_base_event_pb2.RayEvent.Severity.INFO,
                message="hello",
            )
            await event_buffer.add_event(e)

            # wait for two publish attempts (failure then success)
            await async_wait_for_condition(lambda: len(client.publish_calls) == 2)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        metrics = await publisher.get_and_reset_metrics()
        assert metrics["published"] == 1
        assert metrics["failed"] == 0

    @pytest.mark.asyncio
    async def test_publish_with_retries_max_retries_exceeded(self, base_kwargs):
        """Test publish that fails all retries and records failed events."""
        client = MockPublisherClient(publish_result=PublishStats(False, 0, 0))
        event_buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=10)
        publisher = RayEventsPublisher(
            name=base_kwargs["name"],
            publish_client=client,
            event_buffer=event_buffer,
            max_retries=2,  # override to finite retries
            initial_backoff=0,
            max_backoff=0,
            jitter_ratio=0,
        )

        task = asyncio.create_task(publisher.run_forever())
        try:
            # ensure consumer is registered
            await publisher.wait_until_running(2.0)
            e = events_base_event_pb2.RayEvent(
                event_id=b"1",
                source_type=events_base_event_pb2.RayEvent.SourceType.CORE_WORKER,
                event_type=events_base_event_pb2.RayEvent.EventType.TASK_DEFINITION_EVENT,
                timestamp=Timestamp(seconds=123, nanos=0),
                severity=events_base_event_pb2.RayEvent.Severity.INFO,
                message="hello",
            )
            await event_buffer.add_event(e)

            # wait for publish attempts (initial + 2 retries)
            await async_wait_for_condition(lambda: len(client.publish_calls) == 3)
            assert len(client.publish_calls) == 3
            metrics = await publisher.get_and_reset_metrics()
            print(metrics)
            assert metrics["failed"] == 1  # batch_size = 1
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


class TestAsyncHttpPublisherClient:
    """Test HTTP publisher client implementation."""

    @pytest.fixture
    def client_kwargs(self):
        """HTTP client specific kwargs."""
        return {
            "endpoint": "http://example.com/events",
            "events_filter_fn": lambda x: True,  # Allow all events
            "timeout": 5.0,
        }

    @pytest.fixture
    def http_client(self, client_kwargs):
        """Create HTTP client for testing."""
        return AsyncHttpPublisherClient(
            endpoint=client_kwargs["endpoint"],
            executor=ThreadPoolExecutor(max_workers=1),
            events_filter_fn=client_kwargs["events_filter_fn"],
            timeout=client_kwargs["timeout"],
        )

    class _FakeResponse:
        def __init__(self, raise_exc: Optional[Exception] = None):
            self._raise_exc = raise_exc

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            if self._raise_exc:
                raise self._raise_exc

    class _FakeSession:
        def __init__(self, response: "TestAsyncHttpPublisherClient._FakeResponse"):
            self._response = response
            self.post_calls = []

        def post(self, url, json):
            self.post_calls.append((url, json))
            return self._response

        async def close(self):
            return

    @pytest.mark.asyncio
    async def test_publish_empty_batch(self, http_client):
        """Test publishing empty batch."""
        result = await http_client.publish([])
        assert result.is_publish_successful is True
        assert result.num_events_published == 0
        assert result.num_events_filtered_out == 0

    @pytest.mark.asyncio
    async def test_publish_all_filtered_out(self):
        """When all events are filtered, should succeed with filtered count set to num events and no HTTP call."""
        client = AsyncHttpPublisherClient(
            endpoint="http://example.com/events",
            executor=ThreadPoolExecutor(max_workers=1),
            events_filter_fn=lambda x: False,  # Filter out all events
            timeout=5.0,
        )

        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(2)]
        result = await client.publish(events)

        assert result.is_publish_successful is True
        assert result.num_events_published == 0
        assert result.num_events_filtered_out == 2

    @pytest.mark.asyncio
    async def test_publish_success(self, client_kwargs):
        """Test successful HTTP publish."""
        fake_resp = self._FakeResponse()
        events = [
            events_base_event_pb2.RayEvent(
                event_id=b"1",
                source_type=events_base_event_pb2.RayEvent.SourceType.CORE_WORKER,
                event_type=events_base_event_pb2.RayEvent.EventType.TASK_DEFINITION_EVENT,
                timestamp=Timestamp(seconds=123, nanos=0),
                severity=events_base_event_pb2.RayEvent.Severity.INFO,
                message="hello",
            ),
            events_base_event_pb2.RayEvent(
                event_id=b"2",
                source_type=events_base_event_pb2.RayEvent.SourceType.CORE_WORKER,
                event_type=events_base_event_pb2.RayEvent.EventType.TASK_DEFINITION_EVENT,
                timestamp=Timestamp(seconds=124, nanos=0),
                severity=events_base_event_pb2.RayEvent.Severity.INFO,
                message="world",
            ),
        ]

        client = AsyncHttpPublisherClient(
            endpoint=client_kwargs["endpoint"],
            executor=ThreadPoolExecutor(max_workers=1),
            events_filter_fn=client_kwargs["events_filter_fn"],
            timeout=client_kwargs["timeout"],
        )
        client.set_session(self._FakeSession(fake_resp))

        result = await client.publish(events)

        assert result.is_publish_successful is True
        assert result.num_events_published == 2
        assert result.num_events_filtered_out == 0

    @pytest.mark.asyncio
    async def test_publish_http_error(self, client_kwargs):
        """Test HTTP error handling."""
        fake_resp = self._FakeResponse(raise_exc=Exception("HTTP error"))
        events = [
            events_base_event_pb2.RayEvent(
                event_id=b"1",
                source_type=events_base_event_pb2.RayEvent.SourceType.CORE_WORKER,
                event_type=events_base_event_pb2.RayEvent.EventType.TASK_DEFINITION_EVENT,
                timestamp=Timestamp(seconds=123, nanos=0),
                severity=events_base_event_pb2.RayEvent.Severity.INFO,
                message="hello",
            )
        ]

        client = AsyncHttpPublisherClient(
            endpoint=client_kwargs["endpoint"],
            executor=ThreadPoolExecutor(max_workers=1),
            events_filter_fn=client_kwargs["events_filter_fn"],
            timeout=client_kwargs["timeout"],
        )
        client.set_session(self._FakeSession(fake_resp))

        result = await client.publish(events)

        assert result.is_publish_successful is False
        assert result.num_events_published == 0

    @pytest.mark.asyncio
    async def test_count_events_in_batch(self, http_client):
        """Test counting events in batch."""
        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(3)]
        count = http_client.count_num_events_in_batch(events)
        assert count == 3


class TestNoopPublisher:
    """Test no-op publisher implementation."""

    @pytest.mark.asyncio
    async def test_all_methods_noop(self):
        """Test that run_forever can be cancelled and metrics return expected values."""
        publisher = NoopPublisher()

        # Start and cancel run_forever
        task = asyncio.create_task(publisher.run_forever())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        metrics = await publisher.get_and_reset_metrics()
        assert metrics == {
            "published": 0,
            "filtered_out": 0,
            "failed": 0,
            "queue_dropped": 0,
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
