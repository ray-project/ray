from abc import ABC, abstractmethod
import logging
import threading
import queue
import random
import json
from typing import Callable, Optional, Tuple, Dict

from google.protobuf.json_format import MessageToJson

from ray.core.generated import (
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
)

logger = logging.getLogger(__name__)


class RayEventsPublisherBase(ABC):
    """Base class for event publishers with internal worker threads and retries.

    Subclasses must implement _publish(item) -> Tuple[bool, int, int]
    and _estimate_item_size(item) -> int.
    """

    def __init__(
        self,
        name: str,
        queue_max_size: int,
        num_workers: int,
        stop_event: threading.Event,
        max_enqueue_interval_seconds: float,
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        jitter_ratio: float,
    ) -> None:
        """Initialize a RayEventsPublisher.

        Args:
            name: Name identifier for this publisher instance
            queue_max_size: Maximum number of items that can be queued
            num_workers: Number of worker threads to spawn
            stop_event: Event to signal workers to stop
            max_enqueue_interval_seconds: Maximum time to wait for new items to be added to the queue
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
        """
        self._name = name
        self._queue: "queue.Queue" = queue.Queue(maxsize=queue_max_size)
        self._num_workers = max(1, int(num_workers))
        self._stop_event = stop_event
        self._max_enqueue_interval_seconds = max_enqueue_interval_seconds
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._workers = []

        # Internal metrics (since last get_and_reset_stats call)
        self._metrics_lock = threading.Lock()
        self._published_since_last: int = 0
        self._filtered_out_since_last: int = 0
        self._failed_since_last: int = 0
        self._queue_dropped_since_last: int = 0

    def start(self) -> None:
        for i in range(self._num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"ray_events_publisher_{self._name}_{i}",
                daemon=False,
            )
            self._workers.append(t)
            t.start()

    def has_capacity(self) -> bool:
        return not self._queue.full()

    def enqueue(self, item) -> None:
        """Adds an item to the publisher's queue, dropping oldest item if full."""
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            # Drop oldest then try once more
            oldest = self._queue.get_nowait()
            drop_count = self._estimate_item_size(oldest)
            with self._metrics_lock:
                self._queue_dropped_since_last += drop_count
            self._queue.put_nowait(item)

    def join(self) -> None:
        """Waits for all worker threads to complete."""
        for t in self._workers:
            t.join()

    def get_and_reset_stats(self) -> Dict[str, int]:
        """Return a snapshot of internal stats since last call and reset them.

        Returns a dict with keys: 'published', 'filtered_out', 'failed', 'queue_dropped'.
        """
        with self._metrics_lock:
            stats = {
                "published": self._published_since_last,
                "filtered_out": self._filtered_out_since_last,
                "failed": self._failed_since_last,
                "queue_dropped": self._queue_dropped_since_last,
            }
            self._published_since_last = 0
            self._filtered_out_since_last = 0
            self._failed_since_last = 0
            self._queue_dropped_since_last = 0
        return stats

    def _worker_loop(self) -> None:
        """Main worker loop that processes items from the queue.

        Continuously pulls items and publishes them until stop event is set.
        """
        should_stop = False
        while True:
            if self._queue.empty() and should_stop:
                break

            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                # We dont break out even if should stop is set, because a new item might be added to the queue
                should_stop = self._stop_event.wait(self._max_enqueue_interval_seconds)
                continue

            self._publish_with_retries(item)

    def _publish_with_retries(self, item) -> None:
        """Attempts to publish an item with exponential backoff retries.

        Will retry failed publishes up to max_retries times with increasing delays.
        """
        attempts = 0
        fail_count = self._estimate_item_size(item)
        while True:
            success, published_count, filtered_count = self._publish(item)
            if success:
                with self._metrics_lock:
                    self._published_since_last += int(published_count)
                    self._filtered_out_since_last += int(filtered_count)
                return
            if attempts >= self._max_retries:
                with self._metrics_lock:
                    self._failed_since_last += int(fail_count)
                return
            self._sleep_with_backoff(attempts)
            attempts += 1

    def _sleep_with_backoff(self, attempt: int) -> None:
        """Sleep with exponential backoff and optional jitter.

        Args:
            attempt: The current attempt number (0-based)
        """
        delay = min(
            self._max_backoff,
            self._initial_backoff * (2**attempt),
        )
        if self._jitter_ratio > 0:
            jitter = delay * self._jitter_ratio
            delay = max(0.0, random.uniform(delay - jitter, delay + jitter))
        self._stop_event.wait(delay)  # returns early if stop requested

    # Subclasses must implement these
    @abstractmethod
    def _publish(self, item) -> Tuple[bool, int, int]:
        pass

    @abstractmethod
    def _estimate_item_size(self, item) -> int:
        pass


class GCSPublisher(RayEventsPublisherBase):
    """Publishes event batches to GCS via the GCS gRPC AddEvents API.

    Queue item: (events_tuple, task_events_metadata)
    """

    def __init__(
        self,
        *,
        gcs_event_stub,
        timeout: float,
        queue_max_size: int,
        num_workers: int,
        stop_event: threading.Event,
        max_enqueue_interval_seconds: float,
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        jitter_ratio: float,
    ) -> None:
        super().__init__(
            name="gcs",
            queue_max_size=queue_max_size,
            num_workers=num_workers,
            stop_event=stop_event,
            max_enqueue_interval_seconds=max_enqueue_interval_seconds,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter_ratio=jitter_ratio,
        )
        self._gcs_event_stub = gcs_event_stub
        self._timeout = timeout

    def _publish(self, item) -> Tuple[bool, int, int]:
        # item: (event_batch, task_events_metadata)
        events, task_events_metadata = item
        if not events and (
            not task_events_metadata
            or len(task_events_metadata.dropped_task_attempts) == 0
        ):
            return True, 0, 0
        try:
            events_data = self._create_ray_events_data(events, task_events_metadata)
            request = events_event_aggregator_service_pb2.AddEventsRequest(
                events_data=events_data
            )
            response = self._gcs_event_stub.AddEvents(request, timeout=self._timeout)
            if response.status.code != 0:
                logger.error(f"GCS AddEvents failed: {response.status.message}")
                return False, 0, 0
            return True, len(events), 0
        except Exception as e:
            logger.error(f"Failed to send events to GCS: {e}")
            return False, 0, 0

    def _estimate_item_size(self, item) -> int:
        try:
            events, _ = item
            return len(events)
        except Exception:
            return 0

    def _create_ray_events_data(
        self,
        event_batch: list[events_base_event_pb2.RayEvent],
        task_events_metadata: Optional[
            events_event_aggregator_service_pb2.TaskEventsMetadata
        ] = None,
    ) -> events_event_aggregator_service_pb2.RayEventsData:
        """
        Helper method to create RayEventsData from event batch and metadata.
        """
        events_data = events_event_aggregator_service_pb2.RayEventsData()
        events_data.events.extend(event_batch)

        if task_events_metadata:
            events_data.task_events_metadata.CopyFrom(task_events_metadata)

        return events_data


class ExternalPublisher(RayEventsPublisherBase):
    """Publishes event batches to an external HTTP endpoint after filtering.

    Queue item: events_tuple
    """

    def __init__(
        self,
        *,
        http_session,
        endpoint: str,
        events_filter_fn: Callable[[object], bool],
        timeout: float,
        queue_max_size: int,
        num_workers: int,
        stop_event: threading.Event,
        max_enqueue_interval_seconds: float,
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        jitter_ratio: float,
    ) -> None:
        super().__init__(
            name="external",
            queue_max_size=queue_max_size,
            num_workers=num_workers,
            stop_event=stop_event,
            max_enqueue_interval_seconds=max_enqueue_interval_seconds,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter_ratio=jitter_ratio,
        )
        self._http_session = http_session
        self._endpoint = endpoint
        self._events_filter_fn = events_filter_fn
        self._timeout = timeout

    def _publish(self, item) -> Tuple[bool, int, int]:
        # item: event_batch
        events = item
        if not events:
            return True, 0, 0
        filtered = [e for e in events if self._events_filter_fn(e)]
        filtered_out = len(events) - len(filtered)
        if not filtered:
            # All filtered out -> success but nothing published
            return True, 0, filtered_out
        # Convert protobuf objects to JSON dictionaries for HTTP POST
        filtered_json = [json.loads(MessageToJson(e)) for e in filtered]
        try:
            resp = self._http_session.post(
                self._endpoint,
                json=filtered_json,
                timeout=self._timeout,
            )
            resp.raise_for_status()
            return True, len(filtered), filtered_out
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)
            return False, 0, 0

    def _estimate_item_size(self, item) -> int:
        try:
            return len(item)
        except Exception:
            return 0


class NoopPublisher:
    """A no-op publisher that adheres to the minimal interface used by AggregatorAgent.

    Used when a destination is disabled. It always has capacity, accepts enqueues,
    and has no worker threads to start or join.
    """

    def start(self) -> None:
        return

    def join(self) -> None:
        return

    def has_capacity(self) -> bool:
        return True

    def enqueue(self, item) -> bool:
        return True

    def get_and_reset_stats(self) -> Dict[str, int]:
        return {"published": 0, "filtered_out": 0, "failed": 0, "queue_dropped": 0}
