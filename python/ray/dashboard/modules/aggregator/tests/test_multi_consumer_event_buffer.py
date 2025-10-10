import asyncio
import random
import sys

import pytest
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
        consumer_name = "test_consumer"
        await buffer.register_consumer(consumer_name)
        assert await buffer.size() == 0

        event = _create_test_event(b"event1")
        await buffer.add_event(event)

        assert await buffer.size() == 1

        batch = await buffer.wait_for_batch(consumer_name, timeout_seconds=0)
        assert len(batch) == 1
        assert batch[0] == event

    @pytest.mark.asyncio
    async def test_add_event_buffer_overflow(self):
        """Test buffer overflow behavior and eviction logic."""
        buffer = MultiConsumerEventBuffer(max_size=3, max_batch_size=2)
        consumer_name = "test_consumer"
        await buffer.register_consumer(consumer_name)

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

    @pytest.mark.asyncio
    async def test_wait_for_batch_multiple_events(self):
        """Test waiting for batch when multiple events are immediately available and when when not all events are available."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=3)
        consumer_name = "test_consumer"
        await buffer.register_consumer(consumer_name)

        # Add multiple events
        events = []
        for i in range(5):
            event = _create_test_event(f"event{i}".encode())
            events.append(event)
            await buffer.add_event(event)

        # Should get max_batch_size events immediately
        batch = await buffer.wait_for_batch(consumer_name, timeout_seconds=0.1)
        assert len(batch) == 3  # max_batch_size
        assert batch == events[:3]
        # should now get the leftover events (< max_batch_size)
        batch = await buffer.wait_for_batch(consumer_name, timeout_seconds=0.1)
        assert len(batch) == 2
        assert batch == events[3:]

    @pytest.mark.asyncio
    async def test_wait_for_batch_unknown_consumer(self):
        """Test error handling for unknown consumer."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)

        with pytest.raises(KeyError, match="unknown consumer"):
            await buffer.wait_for_batch("nonexistent_consumer", timeout_seconds=0)

    @pytest.mark.asyncio
    async def test_register_consumer_duplicate(self):
        """Test error handling for duplicate consumer registration."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)
        consumer_name = "test_consumer"
        await buffer.register_consumer(consumer_name)
        with pytest.raises(
            ValueError, match="consumer 'test_consumer' already registered"
        ):
            await buffer.register_consumer(consumer_name)

    @pytest.mark.asyncio
    async def test_multiple_consumers_independent_cursors(self):
        """Test that multiple consumers have independent cursors."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=2)
        consumer_name_1 = "test_consumer_1"
        consumer_name_2 = "test_consumer_2"
        await buffer.register_consumer(consumer_name_1)
        await buffer.register_consumer(consumer_name_2)

        # Add events
        events = []
        for i in range(10):
            event = _create_test_event(f"event{i}".encode())
            events.append(event)
            await buffer.add_event(event)

        # Consumer 1 reads first batch
        batch1 = await buffer.wait_for_batch(consumer_name_1, timeout_seconds=0.1)
        assert batch1 == events[:2]

        # Consumer 2 reads from beginning
        batch2 = await buffer.wait_for_batch(consumer_name_2, timeout_seconds=0.1)
        assert batch2 == events[:2]

        # consumer 1 reads another batch
        batch3 = await buffer.wait_for_batch(consumer_name_1, timeout_seconds=0.1)
        assert batch3 == events[2:4]

        # more events are added leading to events not consumed by consumer 2 getting evicted
        # 4 events get evicted, consumer 1 has processed all 4 evicted events previously
        # but consumer 2 has only processed 2 out of the 4 evicted events
        for i in range(4):
            event = _create_test_event(f"event{i + 10}".encode())
            events.append(event)
            await buffer.add_event(event)

        # Just ensure buffer remains at max size
        assert await buffer.size() == 10

        # consumer 1 will read the next 2 events, not affected by the evictions
        # consumer 1's cursor is adjusted internally to account for the evicted events
        batch4 = await buffer.wait_for_batch(consumer_name_1, timeout_seconds=0.1)
        assert batch4 == events[4:6]

        # consumer 2 will read 2 events, skipping the evicted events
        batch5 = await buffer.wait_for_batch(consumer_name_2, timeout_seconds=0.1)
        assert batch5 == events[4:6]  # events[2:4] are lost

    @pytest.mark.asyncio
    async def test_wait_for_batch_blocks_until_event_available(self):
        """Test that wait_for_batch blocks until at least one event is available."""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=5)
        consumer_name = "test_consumer"
        await buffer.register_consumer(consumer_name)

        # Start waiting for batch (should block)
        async def wait_for_batch():
            return await buffer.wait_for_batch(consumer_name, timeout_seconds=2.0)

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

    @pytest.mark.asyncio
    async def test_concurrent_producer_consumer_random_sleeps_with_overall_timeout(
        self,
    ):
        """Producer with random sleeps and consumer reading until all events are received.

        Uses an overall asyncio timeout to ensure the test fails if it hangs
        before consuming all events.
        """
        total_events = 40
        max_batch_size = 2
        buffer = MultiConsumerEventBuffer(max_size=100, max_batch_size=max_batch_size)
        consumer_name = "test_consumer"
        await buffer.register_consumer(consumer_name)

        produced_events = []
        consumed_events = []

        random.seed(0)

        async def producer():
            for i in range(total_events):
                event = _create_test_event(f"e{i}".encode())
                produced_events.append(event)
                await buffer.add_event(event)
                await asyncio.sleep(random.uniform(0.0, 0.02))

        async def consumer():
            while len(consumed_events) < total_events:
                batch = await buffer.wait_for_batch(consumer_name, timeout_seconds=0.1)
                consumed_events.extend(batch)

        # The test should fail if this times out before all events are consumed
        await asyncio.wait_for(asyncio.gather(producer(), consumer()), timeout=5.0)

        assert len(consumed_events) == total_events
        assert consumed_events == produced_events

    @pytest.mark.asyncio
    async def test_events_are_evicted_once_consumed_by_all_consumers(self):
        """Test events are evicted from the buffer once they are consumed by all consumers"""
        buffer = MultiConsumerEventBuffer(max_size=10, max_batch_size=2)
        consumer_name_1 = "test_consumer_1"
        consumer_name_2 = "test_consumer_2"
        await buffer.register_consumer(consumer_name_1)
        await buffer.register_consumer(consumer_name_2)

        # Add events
        events = []
        for i in range(10):
            event = _create_test_event(f"event{i}".encode())
            events.append(event)
            await buffer.add_event(event)

        assert await buffer.size() == 10
        # Consumer 1 reads first batch
        batch1 = await buffer.wait_for_batch(consumer_name_1, timeout_seconds=0.1)
        assert batch1 == events[:2]

        # buffer size does not change as consumer 2 is yet to consume these events
        assert await buffer.size() == 10

        # Consumer 2 reads from beginning
        batch2 = await buffer.wait_for_batch(consumer_name_2, timeout_seconds=0.1)
        assert batch2 == events[:2]

        # size reduces by 2 as both consumers have consumed 2 events
        assert await buffer.size() == 8


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
