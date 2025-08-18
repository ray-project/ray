from abc import ABC, abstractmethod
import asyncio
import logging
import random
import json
from typing import Callable, Optional, Tuple, Dict
import threading

import aiohttp
from google.protobuf.json_format import MessageToJson

from ray.core.generated import (
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
)

logger = logging.getLogger(__name__)

class PublishResult:
    """A Data class that represents the result of publishing an item."""

    def __init__(self, publish_status: bool, events_published: int, events_filtered_out: int):
        self.publish_status = publish_status
        self.events_published = events_published
        self.events_filtered_out = events_filtered_out


class RayEventsPublisherBase(ABC):
    """Base class for event publishers with internal async worker and retries.

    Subclasses must implement _async_publish(item) -> PublishResult
    and _estimate_item_size(item) -> int.
    """

    def __init__(
        self,
        name: str,
        queue_max_size: int,
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        jitter_ratio: float,
    ) -> None:
        """Initialize a RayEventsPublisher.

        Args:
            name: Name identifier for this publisher instance
            queue_max_size: Maximum number of items that can be queued
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
        """
        self._name = name
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._publish_worker_task = None

        # Internal metrics (since last get_and_reset_stats call)
        self._metrics_lock = threading.Lock()
        self._metric_events_published_since_last: int = 0
        self._metric_events_filtered_out_since_last: int = 0
        self._metric_num_publish_failures_since_last: int = 0
        self._metric_queue_dropped_since_last: int = 0

    def start(self) -> None:
        """Start async worker task."""
        self._publish_worker_task = asyncio.create_task(
            self._async_worker_loop(),
            name=f"ray_events_publisher_{self._name}"
        )

    def has_capacity(self) -> bool:
        return not self._queue.full()

    def enqueue(self, item) -> None:
        """Adds an item to the publisher's queue, dropping oldest item if full."""
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            # Drop oldest then try once more
            oldest = self._queue.get_nowait()
            drop_count = self._estimate_item_size(oldest)
            with self._metrics_lock:
                self._metric_queue_dropped_since_last += drop_count
            self._queue.put_nowait(item)

    async def shutdown(self) -> None:
        """Send sentinel to stop worker and wait for completion."""
        if self._publish_worker_task:
            # Send sentinel value to stop worker
            self.enqueue(None)
            # Wait for worker to complete
            await self._publish_worker_task

    def get_and_reset_metrics(self) -> Dict[str, int]:
        """Return a snapshot of internal stats since last call and reset them.

        Returns a dict with keys: 'published', 'filtered_out', 'failed', 'queue_dropped'.
        """
        with self._metrics_lock:
            publisher_metrics = {
                "published": self._metric_events_published_since_last,
                "filtered_out": self._metric_events_filtered_out_since_last,
                "failed": self._metric_num_publish_failures_since_last,
                "queue_dropped": self._metric_queue_dropped_since_last,
            }
            self._metric_events_published_since_last = 0
            self._metric_events_filtered_out_since_last = 0
            self._metric_num_publish_failures_since_last = 0
            self._metric_queue_dropped_since_last = 0
        return publisher_metrics

    async def _async_worker_loop(self) -> None:
        """Main async worker loop that processes items from the queue.

        Continuously pulls items and publishes them until sentinel value is received.
        """
        while True:
            item = await self._queue.get()
            
            # Check for sentinel value (None) indicating shutdown
            if item is None:
                break
                
            await self._async_publish_with_retries(item)

    async def _async_publish_with_retries(self, item) -> None:
        """Attempts to publish an item with exponential backoff retries.

        Will retry failed publishes up to max_retries times with increasing delays.
        """
        attempts = 0
        batch_size = self._estimate_item_size(item)
        while True:
            result = await self._async_publish(item)
            if result.publish_status:
                with self._metrics_lock:
                    self._metric_events_published_since_last += int(result.events_published)
                    self._metric_events_filtered_out_since_last += int(result.events_filtered_out)
                return
            if attempts >= self._max_retries:
                with self._metrics_lock:
                    self._metric_num_publish_failures_since_last += int(batch_size)
                return
            await self._async_sleep_with_backoff(attempts)
            attempts += 1

    async def _async_sleep_with_backoff(self, attempt: int) -> None:
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
        await asyncio.sleep(delay)

    # Subclasses must implement these
    @abstractmethod
    async def _async_publish(self, item) -> PublishResult:
        """Publishes an item to the destination.

        Returns:
            PublishResult: (success, published_count, filtered_count)
        """
        pass

    @abstractmethod
    def _estimate_item_size(self, item) -> int:
        """
        Estimates the size of an item to track number of items that failed to publish.
        Used to increment failure metrics when publishing fails or queue is full.
        """
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
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        jitter_ratio: float,
    ) -> None:
        super().__init__(
            name="gcs",
            queue_max_size=queue_max_size,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter_ratio=jitter_ratio,
        )
        self._gcs_event_stub = gcs_event_stub
        self._timeout = timeout

    async def _async_publish(self, item) -> PublishResult:
        # item: (event_batch, task_events_metadata)
        events, task_events_metadata = item
        if not events and (
            not task_events_metadata
            or len(task_events_metadata.dropped_task_attempts) == 0
        ):
            return PublishResult(True, 0, 0)
        try:
            events_data = self._create_ray_events_data(events, task_events_metadata)
            request = events_event_aggregator_service_pb2.AddEventsRequest(
                events_data=events_data
            )
            response = await self._gcs_event_stub.AddEvents(request, timeout=self._timeout)
            if response.status.code != 0:
                logger.error(f"GCS AddEvents failed: {response.status.message}")
                return PublishResult(False, 0, 0)
            return PublishResult(True, len(events), 0)
        except Exception as e:
            logger.error(f"Failed to send events to GCS: {e}")
            return PublishResult(False, 0, 0)

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
        endpoint: str,
        events_filter_fn: Callable[[object], bool],
        timeout: float,
        queue_max_size: int,
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        jitter_ratio: float,
    ) -> None:
        super().__init__(
            name="external",
            queue_max_size=queue_max_size,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter_ratio=jitter_ratio,
        )
        self._endpoint = endpoint
        self._events_filter_fn = events_filter_fn
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = None

    def start(self) -> None:
        """Start async worker task."""
        super().start()

    async def shutdown(self) -> None:
        """Shutdown worker and close HTTP session."""
        # Wait for worker to complete (processes all queued items)
        await super().shutdown()
        
        # Now safe to close session since worker is done
        if self._session:
            await self._session.close()
            self._session = None

    async def _async_publish(self, item) -> PublishResult:
        # item: event_batch
        events = item
        if not events:
            return PublishResult(True, 0, 0)
        filtered = [e for e in events if self._events_filter_fn(e)]
        filtered_out = len(events) - len(filtered)
        if not filtered:
            # All filtered out -> success but nothing published
            return PublishResult(True, 0, filtered_out)
        # Convert protobuf objects to JSON dictionaries for HTTP POST
        filtered_json = [json.loads(MessageToJson(e)) for e in filtered]
        try:
            # Create session on first use (lazy initialization)
            if not self._session:
                self._session = aiohttp.ClientSession(timeout=self._timeout)
            
            async with self._session.post(
                self._endpoint,
                json=filtered_json,
            ) as resp:
                resp.raise_for_status()
                return PublishResult(True, len(filtered), filtered_out)
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)
            return PublishResult(False, 0, 0)

    def _estimate_item_size(self, item) -> int:
        try:
            return len(item)
        except Exception:
            return 0


class NoopPublisher:
    """A no-op publisher that adheres to the minimal interface used by AggregatorAgent.

    Used when a destination is disabled. It always has capacity, accepts enqueues,
    and has no worker tasks to start or shutdown.
    """

    def start(self) -> None:
        return

    async def shutdown(self) -> None:
        return

    def has_capacity(self) -> bool:
        return True

    def enqueue(self, item) -> None:
        return

    def get_and_reset_metrics(self) -> Dict[str, int]:
        return {"published": 0, "filtered_out": 0, "failed": 0, "queue_dropped": 0}
