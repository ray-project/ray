import pytest
import sys
import threading
from unittest.mock import Mock, patch
from typing import Tuple

from ray.dashboard.modules.aggregator.ray_events_publisher import (
    RayEventsPublisherBase,
    GCSPublisher,
    ExternalPublisher,
    NoopPublisher,
)
from ray.core.generated import (
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
)


class MockPublisher(RayEventsPublisherBase):
    """Test implementation of RayEventsPublisherBase."""

    def __init__(self, publish_result=(True, 1, 0), item_size=1, **kwargs):
        super().__init__(**kwargs)
        self.publish_result = publish_result
        self.item_size = item_size
        self.publish_calls = []

    def _publish(self, item) -> Tuple[bool, int, int]:
        self.publish_calls.append(item)
        return self.publish_result

    def _estimate_item_size(self, item) -> int:
        return self.item_size


@pytest.fixture
def base_kwargs():
    """Common kwargs for publisher initialization."""
    return {
        "name": "test",
        "queue_max_size": 10,
        "num_workers": 1,
        "stop_event": threading.Event(),
        "max_enqueue_interval_seconds": 0.1,
        "max_retries": 2,
        "initial_backoff": 0.01,
        "max_backoff": 0.1,
        "jitter_ratio": 0.1,
    }


@pytest.fixture
def mock_publisher(base_kwargs):
    """Create a mock publisher for testing."""
    return MockPublisher(**base_kwargs)


class TestRayEventsPublisherBase:
    """Test the abstract base class functionality."""

    def test_has_capacity_and_enqueue(self, mock_publisher, base_kwargs):
        """Test that full queue drops oldest item."""
        # Fill queue
        for i in range(10):
            mock_publisher.enqueue(f"item_{i}")

        # Add one more - should drop oldest
        mock_publisher.enqueue("new_item")
        stats = mock_publisher.get_and_reset_stats()
        assert stats["queue_dropped"] == 1  # item_size = 1

    def test_start_and_stop_workers(self, mock_publisher, base_kwargs):
        """Test that start and stop work."""
        mock_publisher.start()
        # Signal stop and join via the provided stop_event
        base_kwargs["stop_event"].set()
        mock_publisher.join()  # test exits without blocking

    def test_publish_with_retries_failure_then_success(
        self, mock_publisher, base_kwargs
    ):
        """Test publish that fails then succeeds."""
        call_count = 0

        def side_effect(item):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (False, 0, 0)
            return (True, 1, 0)

        mock_publisher._publish = Mock(side_effect=side_effect)
        mock_publisher._publish_with_retries("test_item")

        assert mock_publisher._publish.call_count == 2
        stats = mock_publisher.get_and_reset_stats()
        assert stats["published"] == 1
        assert stats["failed"] == 0

    def test_publish_with_retries_max_retries_exceeded(
        self, mock_publisher, base_kwargs
    ):
        """Test publish that fails all retries."""
        mock_publisher.publish_result = (False, 0, 0)
        mock_publisher._publish_with_retries("test_item")

        # Should try max_retries + 1 times (initial + 2 retries)
        assert len(mock_publisher.publish_calls) == 3
        stats = mock_publisher.get_and_reset_stats()
        assert stats["failed"] == 1  # item_size = 1


class TestGCSPublisher:
    """Test GCS publisher implementation."""

    @pytest.fixture
    def gcs_stub(self):
        return Mock()

    @pytest.fixture
    def gcs_publisher(self, base_kwargs, gcs_stub):
        """Create GCS publisher for testing."""
        kwargs = base_kwargs.copy()
        # Remove 'name' as GCSPublisher doesn't take it directly
        kwargs.pop("name", None)
        kwargs.update({"timeout": 5.0, "gcs_event_stub": gcs_stub})
        return GCSPublisher(**kwargs)

    def test_publish_empty_batch_via_worker(self, gcs_publisher, base_kwargs, gcs_stub):
        """Empty batches should be treated as success with no GCS call."""
        gcs_publisher.start()
        gcs_publisher.enqueue(([], None))
        base_kwargs["stop_event"].set()
        gcs_publisher.join()
        stats = gcs_publisher.get_and_reset_stats()
        assert stats["published"] == 0
        assert stats["failed"] == 0
        gcs_stub.AddEvents.assert_not_called()

    @patch.object(GCSPublisher, "_create_ray_events_data")
    def test_publish_success_via_worker(
        self, mock_create_data, gcs_publisher, base_kwargs, gcs_stub
    ):
        """Test successful event publishing through the worker."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status.code = 0
        gcs_stub.AddEvents.return_value = mock_response

        # Mock the _create_ray_events_data method to return a real message
        real_events_data = events_event_aggregator_service_pb2.RayEventsData()
        mock_create_data.return_value = real_events_data

        # Enqueue events and run
        mock_event1 = Mock(spec=events_base_event_pb2.RayEvent)
        mock_event2 = Mock(spec=events_base_event_pb2.RayEvent)
        events = [mock_event1, mock_event2]
        gcs_publisher.start()
        gcs_publisher.enqueue((events, None))
        base_kwargs["stop_event"].set()
        gcs_publisher.join()

        gcs_stub.AddEvents.assert_called_once()
        mock_create_data.assert_called_once_with(events, None)
        stats = gcs_publisher.get_and_reset_stats()
        assert stats["published"] == 2
        assert stats["failed"] == 0

    def test_publish_gcs_error_via_worker(self, gcs_publisher, base_kwargs, gcs_stub):
        """Test GCS error response through the worker with retries then failure stat."""
        mock_response = Mock()
        mock_response.status.code = 1
        mock_response.status.message = "Error"
        gcs_stub.AddEvents.return_value = mock_response

        events = [Mock(spec=events_base_event_pb2.RayEvent)]
        gcs_publisher.start()
        gcs_publisher.enqueue((events, None))
        base_kwargs["stop_event"].set()
        gcs_publisher.join()

        stats = gcs_publisher.get_and_reset_stats()
        assert stats["failed"] == len(events)

    def test_publish_exception_via_worker(self, gcs_publisher, base_kwargs, gcs_stub):
        """Test exception during publishing through the worker triggers failure stat."""
        gcs_stub.AddEvents.side_effect = Exception("Network error")

        events = [Mock(spec=events_base_event_pb2.RayEvent)]
        gcs_publisher.start()
        gcs_publisher.enqueue((events, None))
        base_kwargs["stop_event"].set()
        gcs_publisher.join()

        stats = gcs_publisher.get_and_reset_stats()
        assert stats["failed"] == len(events)


class TestExternalPublisher:
    """Test external HTTP publisher implementation."""

    @pytest.fixture
    def external_publisher_kwargs(self, base_kwargs):
        """External publisher specific kwargs (without session)."""
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
    def http_session(self):
        return Mock()

    @pytest.fixture
    def external_publisher(self, external_publisher_kwargs, http_session):
        """Create external publisher for testing."""
        kwargs = external_publisher_kwargs.copy()
        kwargs["http_session"] = http_session
        return ExternalPublisher(**kwargs)

    def test_publish_empty_batch_via_worker(
        self, external_publisher, base_kwargs, http_session
    ):
        """Empty batches should count as success with no HTTP call."""
        external_publisher.start()
        external_publisher.enqueue([])
        base_kwargs["stop_event"].set()
        external_publisher.join()
        stats = external_publisher.get_and_reset_stats()
        assert stats["published"] == 0
        assert stats["failed"] == 0
        http_session.post.assert_not_called()

    def test_publish_all_filtered_out_via_worker(self, base_kwargs):
        """When all events are filtered, should succeed with filtered count and no HTTP call."""
        kwargs = base_kwargs.copy()
        kwargs.pop("name", None)
        kwargs.update(
            {
                "http_session": Mock(),
                "endpoint": "http://example.com/events",
                "events_filter_fn": lambda x: False,  # Filter out all events
                "timeout": 5.0,
            }
        )
        publisher = ExternalPublisher(**kwargs)

        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(2)]
        publisher.start()
        publisher.enqueue(events)
        base_kwargs["stop_event"].set()
        publisher.join()

        stats = publisher.get_and_reset_metrics()
        assert stats["published"] == 0
        assert stats["filtered_out"] == 2
        kwargs["http_session"].post.assert_not_called()

    @patch("json.loads")
    @patch("ray.dashboard.modules.aggregator.ray_events_publisher.MessageToJson")
    def test_publish_success_via_worker(
        self,
        mock_msg_to_json,
        mock_json_loads,
        external_publisher,
        http_session,
        base_kwargs,
    ):
        """Test successful HTTP publishing through the worker."""
        mock_msg_to_json.return_value = '{"event": "data"}'
        mock_json_loads.return_value = {"event": "data"}
        http_session.post.return_value = Mock()

        events = [Mock(spec=events_base_event_pb2.RayEvent) for _ in range(2)]
        external_publisher.start()
        external_publisher.enqueue(events)
        base_kwargs["stop_event"].set()
        external_publisher.join()

        http_session.post.assert_called_once_with(
            "http://example.com/events",
            json=[{"event": "data"}, {"event": "data"}],
            timeout=5.0,
        )
        stats = external_publisher.get_and_reset_stats()
        assert stats["published"] == 2
        assert stats["filtered_out"] == 0

    @patch("json.loads")
    @patch("ray.dashboard.modules.aggregator.ray_events_publisher.MessageToJson")
    def test_publish_http_error_via_worker(
        self,
        mock_msg_to_json,
        mock_json_loads,
        external_publisher,
        http_session,
        base_kwargs,
    ):
        """Test HTTP error during publishing through the worker triggers failure stat."""
        mock_msg_to_json.return_value = '{"event": "data"}'
        mock_json_loads.return_value = {"event": "data"}
        http_session.post.side_effect = Exception("HTTP error")

        events = [Mock(spec=events_base_event_pb2.RayEvent)]
        external_publisher.start()
        external_publisher.enqueue(events)
        base_kwargs["stop_event"].set()
        external_publisher.join()

        stats = external_publisher.get_and_reset_stats()
        assert stats["failed"] == len(events)


class TestNoopPublisher:
    """Test no-op publisher implementation."""

    def test_all_methods_noop(self):
        """Test that all methods are no-ops and return expected values."""
        publisher = NoopPublisher()

        # All methods should work without error
        publisher.start()
        publisher.join()

        # These should return expected values
        assert publisher.has_capacity() is True
        publisher.enqueue("anything")
        stats = publisher.get_and_reset_stats()
        assert stats == {
            "published": 0,
            "filtered_out": 0,
            "failed": 0,
            "queue_dropped": 0,
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
