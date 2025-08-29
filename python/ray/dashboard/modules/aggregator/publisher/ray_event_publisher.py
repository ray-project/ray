from abc import ABC, abstractmethod
import asyncio
import logging
import random
from typing import Dict, List, Optional, TYPE_CHECKING

from ray.dashboard.modules.aggregator.task_metadata_buffer import TaskMetadataBuffer

from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)
from ray.dashboard.modules.aggregator.publisher.configs import (
    PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
    PUBLISHER_MAX_RETRIES,
    PUBLISHER_INITIAL_BACKOFF_SECONDS,
    PUBLISHER_MAX_BACKOFF_SECONDS,
    PUBLISHER_JITTER_RATIO,
)
from ray.dashboard.modules.aggregator.publisher.async_publisher_client import (
    PublisherClientInterface,
)

logger = logging.getLogger(__name__)


class RayEventsPublisherInterface(ABC):
    """Abstract interface for publishing Ray event batches to external destinations."""

    @abstractmethod
    async def run_forever(self) -> None:
        """Run the publisher forever until cancellation or process death."""
        pass

    @abstractmethod
    async def get_and_reset_metrics(self) -> Dict[str, int]:
        """Return a snapshot of internal metrics since last call and reset them."""
        pass

    @abstractmethod
    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        """Wait until the publisher has started."""
        pass


class RayEventsPublisher(RayEventsPublisherInterface):
    """RayEvents publisher that publishes batches of events to a destination using a dedicated async worker.

    The publisher is single-threaded and uses a queue to store batches of events.
    The worker loop continuously pulls batches from the queue and publishes them.
    """

    def __init__(
        self,
        name: str,
        publish_client: PublisherClientInterface,
        event_buffer: MultiConsumerEventBuffer,
        task_metadata_buffer: Optional[TaskMetadataBuffer] = None,
        max_retries: int = PUBLISHER_MAX_RETRIES,
        initial_backoff: float = PUBLISHER_INITIAL_BACKOFF_SECONDS,
        max_backoff: float = PUBLISHER_MAX_BACKOFF_SECONDS,
        jitter_ratio: float = PUBLISHER_JITTER_RATIO,
    ) -> None:
        """Initialize a RayEventsPublisher.

        Args:
            name: Name identifier for this publisher instance
            publish_client: Client for publishing events to the destination
            event_buffer: Buffer for reading batches of events
            task_metadata_buffer: Buffer for reading a batch of droppedtask metadata
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
        """
        self._name = name
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._publish_client = publish_client
        self._event_buffer = event_buffer
        self._task_metadata_buffer = task_metadata_buffer
        self._event_buffer_consumer_id = None

        # Internal metrics (since last get_and_reset_metrics call)
        # using thread lock as non publisher threads can also call get_and_reset_metrics
        self._metrics_lock = asyncio.Lock()
        self._metric_events_published_since_last: int = 0
        self._metric_events_filtered_out_since_last: int = 0
        self._metric_events_publish_failures_since_last: int = 0
        self._metric_success_latency_seconds_since_last: List[float] = []
        self._metric_failure_latency_seconds_since_last: List[float] = []
        self._metric_num_failed_attempts_since_last_success: int = 0
        self._metric_last_publish_success_timestamp: Optional[float] = None
        # Event set once the publisher has registered as a consumer and is ready to publish events
        self._started_event: asyncio.Event = asyncio.Event()

    async def run_forever(self) -> None:
        """Run the publisher forever until cancellation or process death.

        Registers as a consumer, starts the worker loop, and handles cleanup on cancellation.
        """
        self._event_buffer_consumer_id = await self._event_buffer.register_consumer()

        # Signal that the publisher is ready to publish events
        self._started_event.set()

        try:
            logger.info(f"Starting publisher {self._name}")
            while True:
                events_batch = await self._event_buffer.wait_for_batch(
                    self._event_buffer_consumer_id,
                    PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
                )
                if self._task_metadata_buffer is not None:
                    task_metadata_batch = await self._task_metadata_buffer.get()
                    events_batch = (events_batch, task_metadata_batch)
                await self._async_publish_with_retries(events_batch)
        except asyncio.CancelledError:
            logger.info(f"Publisher {self._name} cancelled, shutting down gracefully")
            await self._publish_client.close()
            raise
        except Exception as e:
            logger.error(f"Publisher {self._name} encountered error: {e}")
            await self._publish_client.close()
            raise

    async def get_and_reset_metrics(self) -> Dict[str, int]:
        """Return a snapshot of internal metrics since last call and reset them.

        Returns a dict with keys: 'published', 'filtered_out', 'failed', 'queue_dropped'.
        """
        async with self._metrics_lock:
            if self._metric_last_publish_success_timestamp is None:
                time_since_last_success_seconds = None
            else:
                time_since_last_success_seconds = max(
                    0.0,
                    asyncio.get_running_loop().time()
                    - self._metric_last_publish_success_timestamp,
                )
            publisher_metrics = {
                "published": self._metric_events_published_since_last,
                "filtered_out": self._metric_events_filtered_out_since_last,
                "failed": self._metric_events_publish_failures_since_last,
                "success_latency_seconds": list(
                    self._metric_success_latency_seconds_since_last
                ),
                "failure_latency_seconds": list(
                    self._metric_failure_latency_seconds_since_last
                ),
                "failed_attempts_since_last_success": self._metric_num_failed_attempts_since_last_success,
                "time_since_last_success_seconds": time_since_last_success_seconds,
            }

            # Include dropped events by type from the event buffer
            if self._event_buffer_consumer_id is not None:
                dropped_by_type = (
                    await self._event_buffer.get_and_reset_evicted_events_count(
                        self._event_buffer_consumer_id
                    )
                )
                publisher_metrics["dropped_events"] = dropped_by_type
            else:
                publisher_metrics["dropped_events"] = {}

            # Reset counters
            self._metric_events_published_since_last = 0
            self._metric_events_filtered_out_since_last = 0
            self._metric_events_publish_failures_since_last = 0
            self._metric_success_latency_seconds_since_last = []
            self._metric_failure_latency_seconds_since_last = []
            return publisher_metrics

    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        """Wait until the publisher has started.

        Returns:
            True if the publisher started before the timeout, False otherwise.
            If timeout is None, waits indefinitely.
        """
        if timeout is None:
            await self._started_event.wait()
            return True
        try:
            await asyncio.wait_for(self._started_event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _async_publish_with_retries(self, batch) -> None:
        """Attempts to publish a batch with retries.

        Will retry failed publishes up to max_retries times with increasing delays.
        """
        num_events_in_batch = self._publish_client.count_num_events_in_batch(batch)
        while True:
            start = asyncio.get_running_loop().time()
            result = await self._publish_client.publish(batch)
            duration = asyncio.get_running_loop().time() - start

            if result.is_publish_successful:
                async with self._metrics_lock:
                    self._metric_events_published_since_last += int(
                        result.num_events_published
                    )
                    self._metric_events_filtered_out_since_last += int(
                        result.num_events_filtered_out
                    )
                    self._metric_num_failed_attempts_since_last_success = 0
                    self._metric_last_publish_success_timestamp = (
                        asyncio.get_running_loop().time()
                    )
                    self._metric_success_latency_seconds_since_last.append(
                        float(duration)
                    )
                return

            async with self._metrics_lock:
                # if max retries are exhausted mark as failed and break out, retry indefinitely if max_retries is less than 0
                if (
                    self._max_retries >= 0
                    and self._metric_num_failed_attempts_since_last_success
                    >= self._max_retries
                ):
                    self._metric_events_publish_failures_since_last += int(
                        num_events_in_batch
                    )
                    self._metric_num_failed_attempts_since_last_success = 0
                    return

                # max retries not exhausted, increment failed attempts counter and add latency to failure list, retry publishing batch with backoff
                self._metric_num_failed_attempts_since_last_success += 1
                self._metric_failure_latency_seconds_since_last.append(float(duration))

            await self._async_sleep_with_backoff(
                self._metric_num_failed_attempts_since_last_success
            )

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


class NoopPublisher(RayEventsPublisherInterface):
    """A no-op publisher that adheres to the minimal interface used by AggregatorAgent.

    Used when a destination is disabled. It runs forever but does nothing.
    """

    async def run_forever(self) -> None:
        """Run forever doing nothing until cancellation."""
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep for an hour at a time
        except asyncio.CancelledError:
            logger.info("NoopPublisher cancelled")
            raise

    async def get_and_reset_metrics(self) -> Dict[str, int]:
        return {"published": 0, "filtered_out": 0, "failed": 0, "queue_dropped": 0}

    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        return True
