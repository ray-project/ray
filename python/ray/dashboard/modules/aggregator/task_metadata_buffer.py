import asyncio
from typing import Dict, Optional

from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)
from ray.core.generated import events_event_aggregator_service_pb2
from ray.dashboard.modules.aggregator.constants import AGGREGATOR_AGENT_METRIC_PREFIX


class TaskMetadataBuffer:
    """Asyncio-safe buffer for accumulating task event metadata and batching it into a bounded queue.

    This buffer is used to construct TaskEventsMetadata protobuf messages (defined in events_event_aggregator_service.proto).
    """

    def __init__(
        self,
        max_buffer_size: int = 1000,
        max_dropped_attempts_per_metadata_entry: int = 100,
        common_metric_tags: Optional[Dict[str, str]] = None,
    ):
        self._buffer = asyncio.Queue(
            maxsize=max_buffer_size - 1
        )  # -1 to account for the current metadata batch
        self._current_metadata_batch = (
            events_event_aggregator_service_pb2.TaskEventsMetadata()
        )
        self._lock = asyncio.Lock()
        self._max_dropped_attempts = max_dropped_attempts_per_metadata_entry

        self._common_metric_tags = common_metric_tags or {}
        self._metric_recorder = OpenTelemetryMetricRecorder()
        self._dropped_metadata_count_metric_name = f"{AGGREGATOR_AGENT_METRIC_PREFIX}_task_metadata_buffer_dropped_attempts_total"
        self._metric_recorder.register_counter_metric(
            self._dropped_metadata_count_metric_name,
            "Total number of dropped task attempt metadata entries which were dropped due to buffer being full",
        )

    async def merge(
        self,
        new_metadata: Optional[events_event_aggregator_service_pb2.TaskEventsMetadata],
    ) -> None:
        """Merge new task event metadata into the current entry, enqueuing when limits are reached."""
        if new_metadata is None:
            return

        async with self._lock:
            for new_attempt in new_metadata.dropped_task_attempts:
                if (
                    len(self._current_metadata_batch.dropped_task_attempts)
                    >= self._max_dropped_attempts
                ):
                    # Add current metadata to buffer, if buffer is full, drop the oldest entry
                    if self._buffer.full():
                        oldest_entry = self._buffer.get_nowait()
                        self._metric_recorder.set_metric_value(
                            self._dropped_metadata_count_metric_name,
                            self._common_metric_tags,
                            len(oldest_entry.dropped_task_attempts),
                        )

                    # Enqueue current metadata batch and start a new batch
                    metadata_copy = (
                        events_event_aggregator_service_pb2.TaskEventsMetadata()
                    )
                    metadata_copy.CopyFrom(self._current_metadata_batch)
                    self._buffer.put_nowait(metadata_copy)
                    self._current_metadata_batch.Clear()

                # Now add the new attempt
                new_entry = self._current_metadata_batch.dropped_task_attempts.add()
                new_entry.CopyFrom(new_attempt)

    async def get(self) -> events_event_aggregator_service_pb2.TaskEventsMetadata:
        """Return the next buffered metadata entry or a snapshot of the current one and reset state."""
        async with self._lock:
            if self._buffer.empty():
                # create a copy of the current metadata and return it
                current_metadata = (
                    events_event_aggregator_service_pb2.TaskEventsMetadata()
                )
                current_metadata.CopyFrom(self._current_metadata_batch)

                # Reset the current metadata and start merging afresh
                self._current_metadata_batch.Clear()

                return current_metadata

            return self._buffer.get_nowait()
