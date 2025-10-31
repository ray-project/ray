import asyncio
import logging
import random
from abc import ABC, abstractmethod
from typing import Dict, Optional

from ray.dashboard.modules.aggregator.constants import (
    CONSUMER_TAG_KEY,
)
from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)
from ray.dashboard.modules.aggregator.publisher.async_publisher_client import (
    PublishBatch,
    PublisherClientInterface,
)
from ray.dashboard.modules.aggregator.publisher.configs import (
    PUBLISHER_INITIAL_BACKOFF_SECONDS,
    PUBLISHER_JITTER_RATIO,
    PUBLISHER_MAX_BACKOFF_SECONDS,
    PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
    PUBLISHER_MAX_RETRIES,
)
from ray.dashboard.modules.aggregator.publisher.metrics import (
    consecutive_failures_gauge_name,
    failed_counter_name,
    filtered_counter_name,
    metric_recorder,
    publish_latency_hist_name,
    published_counter_name,
    time_since_last_success_gauge_name,
)

logger = logging.getLogger(__name__)


class RayEventPublisherInterface(ABC):
    """Abstract interface for publishing Ray event batches to external destinations."""

    @abstractmethod
    async def run_forever(self) -> None:
        """Run the publisher forever until cancellation or process death."""
        pass

    @abstractmethod
    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        """Wait until the publisher has started."""
        pass


class RayEventPublisher(RayEventPublisherInterface):
    """RayEvents publisher that publishes batches of events to a destination by running a worker loop.

    The worker loop continuously pulls batches from the event buffer and publishes them to the destination.
    """

    def __init__(
        self,
        name: str,
        publish_client: PublisherClientInterface,
        event_buffer: MultiConsumerEventBuffer,
        common_metric_tags: Optional[Dict[str, str]] = None,
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
            common_metric_tags: Common labels for all prometheus metrics
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
        """
        self._name = name
        self._common_metric_tags = dict(common_metric_tags or {})
        self._common_metric_tags[CONSUMER_TAG_KEY] = name
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._publish_client = publish_client
        self._event_buffer = event_buffer

        # Event set once the publisher has registered as a consumer and is ready to publish events
        self._started_event: asyncio.Event = asyncio.Event()

    async def run_forever(self) -> None:
        """Run the publisher forever until cancellation or process death.

        Registers as a consumer, starts the worker loop, and handles cleanup on cancellation.
        """
        await self._event_buffer.register_consumer(self._name)

        # Signal that the publisher is ready to publish events
        self._started_event.set()

        try:
            logger.info(f"Starting publisher {self._name}")
            while True:
                batch = await self._event_buffer.wait_for_batch(
                    self._name,
                    PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
                )
                publish_batch = PublishBatch(events=batch)
                await self._async_publish_with_retries(publish_batch)
        except asyncio.CancelledError:
            logger.info(f"Publisher {self._name} cancelled, shutting down gracefully")
            raise
        except Exception as e:
            logger.error(f"Publisher {self._name} encountered error: {e}")
            raise
        finally:
            self._started_event.clear()
            await self._publish_client.close()

    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        """Wait until the publisher has started.

        Args:
            timeout: Maximum time to wait in seconds. If None, waits indefinitely.

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
        failed_attempts_since_last_success = 0
        while True:
            start = asyncio.get_running_loop().time()
            result = await self._publish_client.publish(batch)
            duration = asyncio.get_running_loop().time() - start

            if result.is_publish_successful:
                await self._record_success(
                    num_published=int(result.num_events_published),
                    num_filtered=int(result.num_events_filtered_out),
                    duration=float(duration),
                )
                failed_attempts_since_last_success = 0
                return

            # Failed attempt
            # case 1: if max retries are exhausted mark as failed and break out, retry indefinitely if max_retries is less than 0
            if (
                self._max_retries >= 0
                and failed_attempts_since_last_success >= self._max_retries
            ):
                await self._record_final_failure(
                    num_failed_events=int(num_events_in_batch),
                    duration=float(duration),
                )
                return

            # case 2: max retries not exhausted, increment failed attempts counter and add latency to failure list, retry publishing batch with backoff
            failed_attempts_since_last_success += 1
            await self._record_retry_failure(
                duration=float(duration),
                failed_attempts=int(failed_attempts_since_last_success),
            )

            await self._async_sleep_with_backoff(failed_attempts_since_last_success)

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

    async def _record_success(
        self, num_published: int, num_filtered: int, duration: float
    ) -> None:
        """Update in-memory stats and Prometheus metrics for a successful publish."""
        if num_published > 0:
            metric_recorder.set_metric_value(
                published_counter_name,
                self._common_metric_tags,
                int(num_published),
            )
        if num_filtered > 0:
            metric_recorder.set_metric_value(
                filtered_counter_name, self._common_metric_tags, int(num_filtered)
            )
        metric_recorder.set_metric_value(
            consecutive_failures_gauge_name, self._common_metric_tags, 0
        )
        metric_recorder.set_metric_value(
            time_since_last_success_gauge_name, self._common_metric_tags, 0
        )
        metric_recorder.set_metric_value(
            publish_latency_hist_name,
            {**self._common_metric_tags, "Outcome": "success"},
            float(duration),
        )

    async def _record_retry_failure(
        self, duration: float, failed_attempts: int
    ) -> None:
        """Update Prometheus metrics for a retryable failure attempt."""
        metric_recorder.set_metric_value(
            consecutive_failures_gauge_name,
            self._common_metric_tags,
            int(failed_attempts),
        )
        metric_recorder.set_metric_value(
            publish_latency_hist_name,
            {**self._common_metric_tags, "Outcome": "failure"},
            float(duration),
        )

    async def _record_final_failure(
        self, num_failed_events: int, duration: float
    ) -> None:
        """Update in-memory stats and Prometheus metrics for a final (non-retryable) failure."""
        if num_failed_events > 0:
            metric_recorder.set_metric_value(
                failed_counter_name,
                self._common_metric_tags,
                int(num_failed_events),
            )
        metric_recorder.set_metric_value(
            consecutive_failures_gauge_name, self._common_metric_tags, 0
        )
        metric_recorder.set_metric_value(
            publish_latency_hist_name,
            {**self._common_metric_tags, "Outcome": "failure"},
            float(duration),
        )


class NoopPublisher(RayEventPublisherInterface):
    """A no-op publisher that adheres to the minimal interface used by AggregatorAgent.

    Used when a destination is disabled. It runs forever but does nothing.
    """

    async def run_forever(self) -> None:
        """Run forever doing nothing until cancellation."""
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            logger.info("NoopPublisher cancelled")
            raise

    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        return True
