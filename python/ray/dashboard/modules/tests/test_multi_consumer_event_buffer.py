import asyncio
import pytest
import time
from unittest.mock import MagicMock
from google.protobuf.timestamp_pb2 import Timestamp

from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)


def _create_test_event(
    event_id: bytes = b"test",
    event_type_enum=RayEvent.EventType.TASK_DEFINITION_EVENT,
    message: str = "test message",
):
    """Helper function to create a test RayEvent."""
    event = RayEvent()
    event.event_id = event_id
    event.source_type = RayEvent.SourceType.CORE_WORKER
    event.event_type = event_type_enum
    event.severity = RayEvent.Severity.INFO
    event.message = message
    event.session_name = "test_session"

    # Set timestamp
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    event.timestamp.CopyFrom(timestamp)

    return event


class TestMultiConsumerEventBuffer:
    @pytest.mark.asyncio
    async def test_add_and_consume_event_basic(self):
        """Test basic event addition."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)
        consumer_id = await buffer.register_consumer()
        assert await buffer.size() == 0

        event = _create_test_event(b"event1")
        await buffer.add_event(event)

        assert await buffer.size() == 1

        batch = await buffer.wait_for_batch(consumer_id, timeout_seconds=0)
        assert len(batch) == 1
        assert batch[0] == event

    @pytest.mark.asyncio
    async def test_add_event_buffer_overflow(self):
        """Test buffer overflow behavior and eviction logic."""
        buffer = MultiConsumerEventBuffer(max_size=3, max_batch_size=2)
        consumer_id = await buffer.register_consumer()

        # Add events to fill buffer
        events = []
        event_types = [
            RayEvent.EventType.TASK_DEFINITION_EVENT,
            RayEvent.EventType.TASK_EXECUTION_EVENT,
            RayEvent.EventType.ACTOR_TASK_DEFINITION_EVENT,
        ]
        for i in range(3):
            event = _create_test_event(f"event{i}".encode(), event_types[i])
            events.append(event)
            await buffer.add_event(event)

        assert await buffer.size() == 3

        # Add one more event to trigger eviction
        overflow_event = _create_test_event(
            b"overflow", RayEvent.EventType.TASK_PROFILE_EVENT
        )
        await buffer.add_event(overflow_event)

        assert await buffer.size() == 3  # Still max size
        first_event_type_name = RayEvent.EventType.Name(event_types[0])
        evicted_events_count = await buffer.get_and_reset_evicted_events_count(
            consumer_id
        )
        assert first_event_type_name in evicted_events_count
        assert evicted_events_count[first_event_type_name] == 1

    @pytest.mark.asyncio
    async def test_wait_for_batch_multiple_events_immediate(self):
        """Test waiting for batch when multiple events are immediately available."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=3)
        consumer_id = await buffer.register_consumer()

        # Add multiple events
        events = []
        for i in range(5):
            event = _create_test_event(f"event{i}".encode())
            events.append(event)
            await buffer.add_event(event)

        # Should get max_batch_size events immediately
        batch = await buffer.wait_for_batch(consumer_id, timeout_seconds=0.1)
        assert len(batch) == 3  # max_batch_size
        assert batch == events[:3]
        # should now get the leftover events (< max_batch_size)
        batch = await buffer.wait_for_batch(consumer_id, timeout_seconds=0.1)
        assert len(batch) == 2
        assert batch == events[3:]

    @pytest.mark.asyncio
    async def test_wait_for_batch_unknown_consumer(self):
        """Test error handling for unknown consumer."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)

        with pytest.raises(KeyError, match="unknown consumer"):
            await buffer.wait_for_batch("nonexistent_consumer", timeout_seconds=0)

    @pytest.mark.asyncio
    async def test_multiple_consumers_independent_cursors(self):
        """Test that multiple consumers have independent cursors."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=2)
        consumer_id_1 = await buffer.register_consumer()
        consumer_id_2 = await buffer.register_consumer()

        # Add events
        events = []
        for i in range(10):
            event = _create_test_event(f"event{i}".encode())
            events.append(event)
            await buffer.add_event(event)

        # Consumer 1 reads first batch
        batch1 = await buffer.wait_for_batch(consumer_id_1, timeout_seconds=0.1)
        assert batch1 == events[:2]

        # Consumer 2 reads from beginning
        batch2 = await buffer.wait_for_batch(consumer_id_2, timeout_seconds=0.1)
        assert batch2 == events[:2]

        # consumer 1 reads another batch
        batch3 = await buffer.wait_for_batch(consumer_id_1, timeout_seconds=0.1)
        assert batch3 == events[2:4]

        # more events are added leading to events not consumed by consumer 2 getting evicted
        # 4 events get evicted, consumer 1 has processed all 4 evicted events previously
        # but consumer 2 has only processed 2 out of the 4 evicted events
        for i in range(4):
            event = _create_test_event(f"event{i + 10}".encode())
            events.append(event)
            await buffer.add_event(event)

        # no events are evicted for consumer 1
        eviction_stats1 = await buffer.get_and_reset_evicted_events_count(consumer_id_1)
        assert eviction_stats1 == {}

        # 2 unprocessed events are evicted for consumer 2
        eviction_stats2 = await buffer.get_and_reset_evicted_events_count(consumer_id_2)
        assert "TASK_DEFINITION_EVENT" in eviction_stats2
        assert eviction_stats2["TASK_DEFINITION_EVENT"] == 2

        # consumer 1 will read the next 2 events, not affected by the evictions
        # consumer 1's cursor is adjusted internally to account for the evicted events
        batch4 = await buffer.wait_for_batch(consumer_id_1, timeout_seconds=0.1)
        assert batch4 == events[4:6]

        # consumer 2 will read 2 events, skipping the evicted events
        batch5 = await buffer.wait_for_batch(consumer_id_2, timeout_seconds=0.1)
        assert batch5 == events[4:6]  # events[2:4] are lost

    @pytest.mark.asyncio
    async def test_get_and_reset_evicted_events_count_unknown_consumer(self):
        """Test error handling for unknown consumer in evicted events count."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)

        with pytest.raises(KeyError, match="unknown consumer"):
            await buffer.get_and_reset_evicted_events_count("nonexistent_consumer")

    @pytest.mark.asyncio
    async def test_wait_for_batch_blocks_until_event_available(self):
        """Test that wait_for_batch blocks until at least one event is available."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)
        consumer_id = await buffer.register_consumer()

        # Start waiting for batch (should block)
        async def wait_for_batch():
            return await buffer.wait_for_batch(consumer_id, timeout_seconds=2.0)

        wait_task = asyncio.create_task(wait_for_batch())

        # Wait a bit to ensure the task is waiting
        await asyncio.sleep(4.0)
        assert not wait_task.done()

        # Add an event
        event = _create_test_event(b"event1")
        await buffer.add_event(event)

        # Now the task should complete
        batch = await wait_task
        assert len(batch) == 1
        assert batch[0] == event
