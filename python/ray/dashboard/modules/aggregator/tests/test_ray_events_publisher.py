import pytest
import pytest_asyncio
import sys
import asyncio
from unittest.mock import Mock, patch, AsyncMock

from ray.dashboard.modules.aggregator.ray_events_publisher import (
    RayEventsPublisherBase,
    ExternalSvcPublisher,
    NoopPublisher,
    PublishStats,
)
from ray.core.generated import events_base_event_pb2
from typing import Optional
from google.protobuf.timestamp_pb2 import Timestamp


class MockPublisher(RayEventsPublisherBase):
    """Test implementation of RayEventsPublisherBase."""

    def __init__(
        self,
        publish_result: Optional[PublishStats] = None,
        item_size: int = 1,
        side_effect=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.publish_result = publish_result or PublishStats(True, 1, 0)
        self.item_size = item_size
        self.publish_calls = []
        self._side_effect = side_effect

    async def _async_publish(self, item) -> PublishStats:
        self.publish_calls.append(item)
        if self._side_effect is not None:
            if asyncio.iscoroutinefunction(self._side_effect):
                return await self._side_effect(item)
            return self._side_effect(item)
        return self.publish_result

    def _count_num_events(self, item) -> int:
        return self.item_size


@pytest.fixture
def base_kwargs():
    """Common kwargs for publisher initialization."""
    return {
        "name": "test",
        "queue_max_size": 10,
        "max_retries": 2,
        "initial_backoff": 0.01,
        "max_backoff": 0.1,
        "jitter_ratio": 0.1,
    }


@pytest_asyncio.fixture
async def mock_publisher(base_kwargs):
    """Create a mock publisher for testing inside an active event loop."""
    publisher = MockPublisher(**base_kwargs)
    publisher.start()
    try:
        yield publisher
    finally:
        await publisher.shutdown()


class TestRayEventsPublisherBase:
    """Test the abstract base class functionality."""

    @pytest.mark.asyncio
    def test_has_capacity_and_enqueue(self, mock_publisher):
        """Test that full queue drops oldest item."""
        # Fill queue
        for i in range(10):
            mock_publisher.publish_events(f"item_{i}")

        # Add one more - should drop oldest
        mock_publisher.publish_events("new_item")
        metrics = mock_publisher.get_and_reset_metrics()
        assert metrics["queue_dropped"] == 1  # item_size = 1

    @pytest.mark.asyncio
    async def test_start_and_stop_publishers(self, mock_publisher):
        """Test that start and async shutdown work."""
        mock_publisher.start()
        await mock_publisher.shutdown()  # should not block or error

    @pytest.mark.asyncio
    async def test_publish_with_retries_failure_then_success(self, base_kwargs):
        """Test publish that fails then succeeds."""
        call_count = {"count": 0}

        # fail the first publish call but succed on retry
        def side_effect(item):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return PublishStats(False, 0, 0)
            return PublishStats(True, 1, 0)

        pub = MockPublisher(side_effect=side_effect, **base_kwargs)
        await pub._async_publish_with_retries("test_item")

        assert len(pub.publish_calls) == 2
        metrics = pub.get_and_reset_metrics()
        assert metrics["published"] == 1
        assert metrics["failed"] == 0

    @pytest.mark.asyncio
    async def test_publish_with_retries_max_retries_exceeded(self, base_kwargs):
        """Test publish that fails all retries."""
        pub = MockPublisher(
            publish_result=PublishStats(False, 0, 0),
            **base_kwargs,
        )
        await pub._async_publish_with_retries("test_item")

        # Should try max_retries + 1 times (initial + 2 retries)
        assert len(pub.publish_calls) == 3
        metrics = pub.get_and_reset_metrics()
        assert metrics["failed"] == 1  # item_size = 1


class TestExternalSvcPublisher:
    """Test external HTTP publisher implementation."""

    @pytest.fixture
    def external_publisher_kwargs(self, base_kwargs):
        """External publisher specific kwargs (without injected session)."""
        ext_kwargs = base_kwargs.copy()
        # Remove 'name' as ExternalPublisher doesn't take it directly
        ext_kwargs.pop("name", None)
        ext_kwargs.update(
            {
                "endpoint": "http://example.com/events",
                "events_filter_fn": lambda x: True,  # Allow all events
                "timeout": 5.0,
            }
        )
        return ext_kwargs

    @pytest.fixture
    def external_publisher(self, external_publisher_kwargs):
        """Create external publisher for testing."""
        kwargs = external_publisher_kwargs.copy()
        return ExternalSvcPublisher(**kwargs)

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
        def __init__(self, response: "TestExternalPublisher._FakeResponse"):
            self._response = response
            self.post_calls = []

        def post(self, url, json):
            self.post_calls.append((url, json))
            return self._response

        async def close(self):
            return

    @pytest.mark.asyncio
    async def test_publish_empty_batch(self, external_publisher):
        external_publisher.start()
        external_publisher.publish_events([])
        await external_publisher.shutdown()
        metrics = external_publisher.get_and_reset_metrics()
        assert metrics["published"] == 0
        assert metrics["failed"] == 0

    @pytest.mark.asyncio
    async def test_publish_all_filtered_out(self, base_kwargs):
        """When all events are filtered, should succeed with filtered count set to num events and no HTTP call."""
        kwargs = base_kwargs.copy()
        kwargs.pop("name", None)
        kwargs.update(
            {
                "endpoint": "http://example.com/events",
                "events_filter_fn": lambda x: False,  # Filter out all events
                "timeout": 5.0,
            }
        )
        publisher = ExternalSvcPublisher(**kwargs)

        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(2)]
        publisher.start()
        publisher.publish_events(events)
        await publisher.shutdown()

        metrics = publisher.get_and_reset_metrics()
        assert metrics["published"] == 0
        assert metrics["filtered_out"] == 2

    @pytest.mark.asyncio
    async def test_publish_success(
        self,
        external_publisher_kwargs,
    ):
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
        publisher = ExternalSvcPublisher(**external_publisher_kwargs)
        publisher.set_session(self._FakeSession(fake_resp))
        publisher.start()
        publisher.publish_events(events)
        await publisher.shutdown()

        metrics = publisher.get_and_reset_metrics()
        assert metrics["published"] == 2
        assert metrics["filtered_out"] == 0

    @pytest.mark.asyncio
    async def test_publish_http_error(
        self,
        external_publisher_kwargs,
    ):
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

        publisher = ExternalSvcPublisher(**external_publisher_kwargs)
        publisher.set_session(self._FakeSession(fake_resp))
        publisher.start()
        publisher.publish_events(events)
        await publisher.shutdown()

        metrics = publisher.get_and_reset_metrics()
        assert metrics["failed"] == len(events)


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
        assert publisher.has_capacity() is True
        publisher.publish_events("anything")
        metrics = publisher.get_and_reset_metrics()
        assert metrics == {
            "published": 0,
            "filtered_out": 0,
            "failed": 0,
            "queue_dropped": 0,
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
