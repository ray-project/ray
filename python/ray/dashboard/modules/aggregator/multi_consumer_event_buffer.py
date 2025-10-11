import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional

from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)
from ray.core.generated import (
    events_base_event_pb2,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.dashboard.modules.aggregator.constants import (
    AGGREGATOR_AGENT_METRIC_PREFIX,
    CONSUMER_TAG_KEY,
)


@dataclass
class _ConsumerState:
    # Index of the next event to be consumed by this consumer
    cursor_index: int


class MultiConsumerEventBuffer:
    """A buffer which allows adding one event at a time and consuming events in batches.
    Supports multiple consumers, each with their own cursor index. Tracks the number of events evicted for each consumer.

    Buffer is not thread-safe but is asyncio-friendly. All operations must be called from within the same event loop.

    Arguments:
        max_size: Maximum number of events to store in the buffer.
        max_batch_size: Maximum number of events to return in a batch when calling wait_for_batch.
        common_metric_tags: Tags to add to all metrics.
    """

    def __init__(
        self,
        max_size: int,
        max_batch_size: int,
        common_metric_tags: Optional[Dict[str, str]] = None,
    ):
        self._buffer = deque(maxlen=max_size)
        self._max_size = max_size
        self._lock = asyncio.Lock()
        self._has_new_events_to_consume = asyncio.Condition(self._lock)
        self._consumers: Dict[str, _ConsumerState] = {}

        self._max_batch_size = max_batch_size

        self._common_metrics_tags = common_metric_tags or {}
        self._metric_recorder = OpenTelemetryMetricRecorder()
        self.evicted_events_metric_name = (
            f"{AGGREGATOR_AGENT_METRIC_PREFIX}_queue_dropped_events"
        )
        self._metric_recorder.register_counter_metric(
            self.evicted_events_metric_name,
            "Total number of events dropped because the publish/buffer queue was full.",
        )

    async def add_event(self, event: events_base_event_pb2.RayEvent) -> None:
        """Add an event to the buffer.

        If the buffer is full, the oldest event is dropped.
        """
        async with self._lock:
            dropped_event = None
            if len(self._buffer) >= self._max_size:
                dropped_event = self._buffer.popleft()
            self._buffer.append(event)

            if dropped_event is not None:
                for consumer_name, consumer_state in self._consumers.items():
                    # Update consumer cursor index and evicted events metric if an event was dropped
                    if consumer_state.cursor_index == 0:
                        # The dropped event was the next event this consumer would have consumed, publish eviction metric
                        self._metric_recorder.set_metric_value(
                            self.evicted_events_metric_name,
                            {
                                **self._common_metrics_tags,
                                CONSUMER_TAG_KEY: consumer_name,
                                "event_type": RayEvent.EventType.Name(
                                    dropped_event.event_type
                                ),
                            },
                            1,
                        )
                    else:
                        # The dropped event was already consumed by the consumer, so we need to adjust the cursor
                        consumer_state.cursor_index -= 1

            # Signal the consumers that there are new events to consume
            self._has_new_events_to_consume.notify_all()

    def _evict_old_events(self) -> None:
        """Clean the buffer by removing events from the buffer who have index lower than
        all the cursor indexes of all consumers and updating the cursor index of all
        consumers.
        """
        if not self._consumers:
            return

        min_cursor_index = min(
            consumer_state.cursor_index for consumer_state in self._consumers.values()
        )
        for _ in range(min_cursor_index):
            self._buffer.popleft()

        # update the cursor index of all consumers
        for consumer_state in self._consumers.values():
            consumer_state.cursor_index -= min_cursor_index

    async def wait_for_batch(
        self, consumer_name: str, timeout_seconds: float = 1.0
    ) -> List[events_base_event_pb2.RayEvent]:
        """Wait for batch respecting self.max_batch_size and timeout_seconds.

        Returns a batch of up to self.max_batch_size items. Waits for up to
        timeout_seconds after receiving the first event that will be in
        the next batch. After the timeout, returns as many items as are ready.

        Always returns a batch with at least one item - will block
        indefinitely until an item comes in.

        Arguments:
            consumer_name: name of the consumer consuming the batch
            timeout_seconds: maximum time to wait for a batch

        Returns:
            A list of up to max_batch_size events ready for consumption.
            The list always contains at least one event.
        """
        max_batch = self._max_batch_size
        batch = []
        async with self._has_new_events_to_consume:
            consumer_state = self._consumers.get(consumer_name)
            if consumer_state is None:
                raise KeyError(f"unknown consumer '{consumer_name}'")

            # Phase 1: read the first event, wait indefinitely until there is at least one event to consume
            while consumer_state.cursor_index >= len(self._buffer):
                await self._has_new_events_to_consume.wait()

            # Add the first event to the batch
            event = self._buffer[consumer_state.cursor_index]
            consumer_state.cursor_index += 1
            batch.append(event)

            # Phase 2: add items to the batch up to timeout or until full
            deadline = time.monotonic() + max(0.0, float(timeout_seconds))
            while len(batch) < max_batch:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break

                # Drain whatever is available
                while len(batch) < max_batch and consumer_state.cursor_index < len(
                    self._buffer
                ):
                    batch.append(self._buffer[consumer_state.cursor_index])
                    consumer_state.cursor_index += 1

                if len(batch) >= max_batch:
                    break

                # There is still room in the batch, but no new events to consume; wait until notified or timeout
                try:
                    await asyncio.wait_for(
                        self._has_new_events_to_consume.wait(), remaining
                    )
                except asyncio.TimeoutError:
                    # Timeout, return the current batch
                    break

            self._evict_old_events()
        return batch

    async def register_consumer(self, consumer_name: str) -> None:
        """Register a new consumer with a name.

        Arguments:
            consumer_name: A unique name for the consumer.

        """
        async with self._lock:
            if self._consumers.get(consumer_name) is not None:
                raise ValueError(f"consumer '{consumer_name}' already registered")

            self._consumers[consumer_name] = _ConsumerState(cursor_index=0)

    async def size(self) -> int:
        """Get total number of events in the buffer. Does not take consumer cursors into account."""
        return len(self._buffer)
