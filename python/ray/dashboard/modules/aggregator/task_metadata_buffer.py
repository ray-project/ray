import threading
import queue
from typing import Optional

from ray.core.generated import events_event_aggregator_service_pb2


class TaskMetadataBuffer:
    """Thread-safe buffer for accumulating task event metadata and batching it into a bounded queue."""

    def __init__(
        self,
        max_buffer_size: int = 1000,
        max_dropped_attempts_per_metadata_entry: int = 100,
    ):
        self._buffer = queue.Queue(maxsize=max_buffer_size)
        self._metadata = events_event_aggregator_service_pb2.TaskEventsMetadata()
        self._lock = threading.Lock()
        self._max_dropped_attempts = max_dropped_attempts_per_metadata_entry
        self._dropped_metadata_count = 0

    def merge(
        self,
        new_metadata: Optional[events_event_aggregator_service_pb2.TaskEventsMetadata],
    ) -> None:
        """Merge new task event metadata into the current entry, enqueuing when limits are reached."""
        if new_metadata is None:
            return

        with self._lock:
            for new_attempt in new_metadata.dropped_task_attempts:
                if (
                    len(self._metadata.dropped_task_attempts)
                    >= self._max_dropped_attempts
                ):
                    # Flush current metadata to buffer
                    if self._buffer.full():
                        oldest_entry = self._buffer.get_nowait()
                        self._dropped_metadata_count += len(
                            oldest_entry.dropped_task_attempts
                        )

                    # Enqueue a copy so clearing current metadata does not affect queued item
                    metadata_copy = (
                        events_event_aggregator_service_pb2.TaskEventsMetadata()
                    )
                    metadata_copy.CopyFrom(self._metadata)
                    self._buffer.put_nowait(metadata_copy)
                    self._metadata.Clear()

                # Now add the new attempt
                new_entry = self._metadata.dropped_task_attempts.add()
                new_entry.CopyFrom(new_attempt)

    def get(self):
        """Return the next buffered metadata entry or a snapshot of the current one and reset state."""
        with self._lock:
            if self._buffer.empty():
                # create a copy of the current metadata and return it
                current_metadata = (
                    events_event_aggregator_service_pb2.TaskEventsMetadata()
                )
                current_metadata.CopyFrom(self._metadata)

                # Reset the current metadata and start merging afresh
                self._metadata.Clear()

                return current_metadata.dropped_task_attempts

            return self._buffer.get_nowait()

    def get_and_reset_dropped_metadata_count(self) -> int:
        """Return the total number of dropped task attempts entries dropped due to buffer being full and reset the counter."""
        with self._lock:
            dropped_metadata_count = self._dropped_metadata_count
            self._dropped_metadata_count = 0
            return dropped_metadata_count
