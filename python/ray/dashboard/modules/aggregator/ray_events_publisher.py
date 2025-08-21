from abc import ABC, abstractmethod
import asyncio
import logging
import random
import json
from typing import Callable, Dict
import threading

import aiohttp
from google.protobuf.json_format import MessageToJson
from ray._private import ray_constants

from ray.core.generated import (
    events_base_event_pb2,
)

logger = logging.getLogger(__name__)

# Environment variables for the aggregator agent
env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT"
# timeout for the publisher to publish events to the destination
PUBLISHER_TIMEOUT_SECONDS = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISHER_TIMEOUT_SECONDS", 5
)
# maximum number of events that can be queued for publishing to the destination
PUBLISHER_QUEUE_MAX_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISH_DEST_QUEUE_MAX_SIZE", 50
)
# maximum number of retries for publishing events to the destination
PUBLISHER_MAX_RETRIES = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISH_MAX_RETRIES", 5
)
# initial backoff time for publishing events to the destination
PUBLISHER_INITIAL_BACKOFF_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_PUBLISH_INITIAL_BACKOFF_SECONDS", 0.01
)
# maximum backoff time for publishing events to the destination
PUBLISHER_MAX_BACKOFF_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_PUBLISH_MAX_BACKOFF_SECONDS", 5.0
)
# jitter ratio for publishing events to the destination
PUBLISHER_JITTER_RATIO = ray_constants.env_float(
    f"{env_var_prefix}_PUBLISH_JITTER_RATIO", 0.1
)


class PublishStats:
    """Data class that represents stats of publishing a batch of events."""

    def __init__(
        self,
        publish_status: bool,
        num_events_published: int,
        num_events_filtered_out: int,
    ):
        self.publish_status = publish_status
        self.num_events_published = num_events_published
        self.num_events_filtered_out = num_events_filtered_out


class PublisherClientInterface(ABC):
    """Abstract interface for publishing Ray event batches to external destinations.

    Implementations should handle the actual publishing logic, filtering,
    and format conversion appropriate for their specific destination type.
    """

    @abstractmethod
    async def publish(self, batch) -> PublishStats:
        """Publish a batch of events to the destination."""
        pass

    @abstractmethod
    def count_num_events_in_batch(self, batch) -> int:
        """Count the number of events in a given batch."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up any resources used by this client."""
        pass


class AsyncHttpPublisherClient(PublisherClientInterface):
    """Client for publishing ray event batches to an external HTTP service."""

    def __init__(
        self,
        endpoint: str,
        events_filter_fn: Callable[[object], bool],
        timeout: float = PUBLISHER_TIMEOUT_SECONDS,
    ) -> None:
        self._endpoint = endpoint
        self._events_filter_fn = events_filter_fn
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = None

    async def publish(
        self, events_batch: list[events_base_event_pb2.RayEvent]
    ) -> PublishStats:
        if not events_batch:
            return PublishStats(True, 0, 0)
        filtered = [e for e in events_batch if self._events_filter_fn(e)]
        filtered_out = len(events_batch) - len(filtered)
        if not filtered:
            # All filtered out -> success but nothing published
            return PublishStats(True, 0, filtered_out)
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
                return PublishStats(True, len(filtered), filtered_out)
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)
            return PublishStats(False, 0, 0)

    def count_num_events_in_batch(
        self, events_batch: list[events_base_event_pb2.RayEvent]
    ) -> int:
        try:
            return len(events_batch)
        except Exception:
            return 0

    async def close(self) -> None:
        """Closes the http session if one was created"""
        if self._session:
            await self._session.close()
            self._session = None

    def set_session(self, session) -> None:
        """Inject an HTTP client session. Intended for testing.

        If a session is set explicitly, it will be used and managed by close().
        """
        self._session = session


class RayEventsPublisher:
    """RayEvents publisher that publishes batches of events to a destination using a dedicated async worker.

    The publisher is single-threaded and uses a queue to store batches of events.
    The worker loop continuously pulls batches from the queue and publishes them.
    """

    def __init__(
        self,
        name: str,
        publish_client: PublisherClientInterface,
        queue_max_size: int = PUBLISHER_QUEUE_MAX_SIZE,
        max_retries: int = PUBLISHER_MAX_RETRIES,
        initial_backoff: float = PUBLISHER_INITIAL_BACKOFF_SECONDS,
        max_backoff: float = PUBLISHER_MAX_BACKOFF_SECONDS,
        jitter_ratio: float = PUBLISHER_JITTER_RATIO,
    ) -> None:
        """Initialize a RayEventsPublisher.

        Args:
            name: Name identifier for this publisher instance
            queue_max_size: Maximum number of batches that can be queued
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
        """
        self._name = name
        self._queue = None
        self._queue_max_size = int(queue_max_size)
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._publish_worker_task = None
        self._publish_client = publish_client

        # Internal metrics (since last get_and_reset_metrics call)
        # using thread lock as non publisher threads can also call get_and_reset_metrics
        self._metrics_lock = threading.Lock()
        self._metric_events_published_since_last: int = 0
        self._metric_events_filtered_out_since_last: int = 0
        self._metric_events_publish_failures_since_last: int = 0
        self._metric_queue_dropped_since_last: int = 0

    def start(self) -> None:
        """Start async worker task. Should be called from within the publisher event loop."""
        # Initialize queue in the dedicated event loop
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self._queue_max_size)
        self._publish_worker_task = asyncio.create_task(
            self._async_worker_loop(), name=f"ray_events_publisher_{self._name}"
        )

    def can_accept_events(self) -> bool:
        """Returns whether the publisher can accept new events for publishing."""
        return self._queue is None or not self._queue.full()

    def enqueue_batch(self, batch) -> None:
        """Queues events for publishing to the sink destination.

        Single-event-loop safe: runs to completion without yielding. (not thread-safe)
        If queue is full, drops oldest events to make room for new ones.
        """
        if self._queue is None:
            # If enqueue is called before start(), fail the call
            raise RuntimeError("Publisher has not yet been started")
        try:
            self._queue.put_nowait(batch)
        except asyncio.QueueFull:
            # Drop oldest then try once more
            oldest = self._queue.get_nowait()
            drop_count = self._publish_client.count_num_events_in_batch(oldest)
            with self._metrics_lock:
                self._metric_queue_dropped_since_last += drop_count
                print(self._metric_queue_dropped_since_last, oldest)
            self._queue.put_nowait(batch)

    async def shutdown(self) -> None:
        """Send sentinel to stop worker and wait for completion.

        Ensures all queued batches are published before gracefully shutting down.
        """
        if self._publish_worker_task and self._queue is not None:
            # Send sentinel (None) value to stop worker
            self.enqueue_batch(None)
            # Wait for worker to complete
            await self._publish_worker_task
            await self._publish_client.close()

    def get_and_reset_metrics(self) -> Dict[str, int]:
        """Return a snapshot of internal metrics since last call and reset them.

        Returns a dict with keys: 'published', 'filtered_out', 'failed', 'queue_dropped'.
        """
        with self._metrics_lock:
            publisher_metrics = {
                "published": self._metric_events_published_since_last,
                "filtered_out": self._metric_events_filtered_out_since_last,
                "failed": self._metric_events_publish_failures_since_last,
                "queue_dropped": self._metric_queue_dropped_since_last,
            }
            self._metric_events_published_since_last = 0
            self._metric_events_filtered_out_since_last = 0
            self._metric_events_publish_failures_since_last = 0
            self._metric_queue_dropped_since_last = 0
        return publisher_metrics

    async def _async_worker_loop(self) -> None:
        """Main async worker loop that processes a batch from the queue.

        Continuously pulls batches and publishes them until sentinel value is received.
        """
        while True:
            batch = await self._queue.get()
            # Check for sentinel value (None) indicating shutdown
            if batch is None:
                break

            await self._async_publish_with_retries(batch)

    async def _async_publish_with_retries(self, batch) -> None:
        """Attempts to publish a batch with retries.

        Will retry failed publishes up to max_retries times with increasing delays.
        """
        attempts = 0
        num_events_in_batch = self._publish_client.count_num_events_in_batch(batch)
        while True:
            result = await self._publish_client.publish(batch)
            if result.publish_status:
                with self._metrics_lock:
                    self._metric_events_published_since_last += int(
                        result.num_events_published
                    )
                    self._metric_events_filtered_out_since_last += int(
                        result.num_events_filtered_out
                    )
                return
            if attempts >= self._max_retries:
                with self._metrics_lock:
                    self._metric_events_publish_failures_since_last += int(
                        num_events_in_batch
                    )
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


class NoopPublisher:
    """A no-op publisher that adheres to the minimal interface used by AggregatorAgent.

    Used when a destination is disabled. It always has capacity, accepts publish_events,
    and has no worker tasks to start or shutdown.
    """

    def start(self) -> None:
        return

    async def shutdown(self) -> None:
        return

    def can_accept_events(self) -> bool:
        return True

    def enqueue_batch(self, batch) -> None:
        return

    def get_and_reset_metrics(self) -> Dict[str, int]:
        return {"published": 0, "filtered_out": 0, "failed": 0, "queue_dropped": 0}
