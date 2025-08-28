import pytest
import pytest_asyncio
import sys
import asyncio
from unittest.mock import Mock

from ray.dashboard.modules.aggregator.publisher.ray_event_publisher import (
    RayEventsPublisher,
    AsyncHttpPublisherClient,
    NoopPublisher,
    PublishStats,
    PublisherClientInterface,
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
        "queue_max_size": 10,
        "max_retries": 2,
        "initial_backoff": 0,
        "max_backoff": 0,
        "jitter_ratio": 0,
    }


@pytest_asyncio.fixture
async def mock_publisher(base_kwargs):
    """Create a mock publisher for testing inside an active event loop."""
    client = MockPublisherClient()
    try:
        publisher = RayEventsPublisher(publish_client=client, **base_kwargs)
        yield publisher
    finally:
        publisher.shutdown()


class TestRayEventsPublisher:
    """Test the main RayEventsPublisher functionality."""

    @pytest.mark.asyncio
    async def test_has_capacity_and_enqueue(self, mock_publisher):
        """Test that full queue drops oldest batch."""
        publisher = mock_publisher
        publisher.start()
        # Fill queue
        for i in range(10):
            publisher.enqueue_batch(f"batch_{i}")

        # Add one more - should drop oldest
        publisher.enqueue_batch("new_batch")

        metrics = publisher.get_and_reset_metrics()
        print(metrics)
        assert metrics["queue_dropped"] == 1  # batch_size = 1

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
        publisher = RayEventsPublisher(publish_client=client, **base_kwargs)
        publisher.start()

        publisher.enqueue_batch("test_batch")
        await publisher.shutdown()  # process batch and shutdown

        assert len(client.publish_calls) == 2
        metrics = publisher.get_and_reset_metrics()
        assert metrics["published"] == 1
        assert metrics["failed"] == 0

    @pytest.mark.asyncio
    async def test_publish_with_retries_max_retries_exceeded(self, base_kwargs):
        """Test publish that fails all retries."""
        client = MockPublisherClient(publish_result=PublishStats(False, 0, 0))
        publisher = RayEventsPublisher(publish_client=client, **base_kwargs)
        publisher.start()

        publisher.enqueue_batch("test_batch")
        await publisher.shutdown()  # process batch and shutdown

        # Should try max_retries + 1 times (initial + 2 retries)
        assert len(client.publish_calls) == 3
        metrics = publisher.get_and_reset_metrics()
        assert metrics["failed"] == 1  # batch_size = 1


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
        return AsyncHttpPublisherClient(**client_kwargs)

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
        assert result.publish_status is True
        assert result.num_events_published == 0
        assert result.num_events_filtered_out == 0

    @pytest.mark.asyncio
    async def test_publish_all_filtered_out(self):
        """When all events are filtered, should succeed with filtered count set to num events and no HTTP call."""
        client = AsyncHttpPublisherClient(
            endpoint="http://example.com/events",
            events_filter_fn=lambda x: False,  # Filter out all events
            timeout=5.0,
        )

        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(2)]
        result = await client.publish(events)

        assert result.publish_status is True
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

        client = AsyncHttpPublisherClient(**client_kwargs)
        client.set_session(self._FakeSession(fake_resp))

        result = await client.publish(events)

        assert result.publish_status is True
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

        client = AsyncHttpPublisherClient(**client_kwargs)
        client.set_session(self._FakeSession(fake_resp))

        result = await client.publish(events)

        assert result.publish_status is False
        assert result.num_events_published == 0

    @pytest.mark.asyncio
    async def test_count_events_in_batch(self, http_client):
        """Test counting events in batch."""
        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(3)]
        count = http_client.count_num_events_in_batch(events)
        assert count == 3


class TestNoopPublisher:
    """Test no-op publisher implementation."""

    def test_all_methods_noop(self):
        """Test that all methods are no-ops and return expected values."""
        publisher = NoopPublisher()

        # All methods should work without error
        publisher.start()
        # shutdown is async in interface, but noop implementation returns immediately
        asyncio.run(publisher.shutdown())

        # These should return expected values
        assert publisher.can_accept_events() is True
        publisher.enqueue_batch("anything")
        metrics = publisher.get_and_reset_metrics()
        assert metrics == {
            "published": 0,
            "filtered_out": 0,
            "failed": 0,
            "queue_dropped": 0,
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
