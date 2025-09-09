from abc import ABC, abstractmethod
import asyncio
import logging
import random
from typing import Dict, Optional

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

from ray.dashboard.modules.aggregator.shared import metric_recorder, metric_prefix

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
        common_metric_tags: Dict[str, str] = {},
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
            common_metric_labels: Common labels for all prometheus metrics
            task_metadata_buffer: Buffer for reading a batch of droppedtask metadata
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
        """
        self._name = name
        self._common_metric_tags = common_metric_tags
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._publish_client = publish_client
        self._event_buffer = event_buffer
        self._task_metadata_buffer = task_metadata_buffer
        self._event_buffer_consumer_id = None

        # Event set once the publisher has registered as a consumer and is ready to publish events
        self._started_event: asyncio.Event = asyncio.Event()

        # OpenTelemetry metrics setup
        self._metric_recorder = metric_recorder

        # Register counter metrics
        self._published_counter_name = (
            f"{metric_prefix}_{self._name}_published_events_total"
        )
        self._metric_recorder.register_counter_metric(
            self._published_counter_name,
            "Total number of events successfully published to the destination.",
        )

        self._filtered_counter_name = (
            f"{metric_prefix}_{self._name}_filtered_events_total"
        )
        self._metric_recorder.register_counter_metric(
            self._filtered_counter_name,
            "Total number of events filtered out before publishing to the destination.",
        )

        self._failed_counter_name = f"{metric_prefix}_{self._name}_failures_total"
        self._metric_recorder.register_counter_metric(
            self._failed_counter_name,
            "Total number of events that failed to publish after retries.",
        )

        # Register histogram metric
        buckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5]
        self._publish_latency_hist_name = (
            f"{metric_prefix}_{self._name}_publish_duration_seconds"
        )
        self._metric_recorder.register_histogram_metric(
            self._publish_latency_hist_name,
            "Duration of publish calls in seconds.",
            buckets,
        )

        # Register gauge metrics
        self._consecutive_failures_gauge_name = (
            f"{metric_prefix}_{self._name}_consecutive_failures_since_last_success"
        )
        self._metric_recorder.register_gauge_metric(
            self._consecutive_failures_gauge_name,
            "Number of consecutive failed publish attempts since the last success.",
        )

        self._time_since_last_success_gauge_name = (
            f"{metric_prefix}_{self._name}_time_since_last_success_seconds"
        )
        self._metric_recorder.register_gauge_metric(
            self._time_since_last_success_gauge_name,
            "Seconds since the last successful publish to the destination.",
        )

    async def run_forever(self) -> None:
        """Run the publisher forever until cancellation or process death.

        Registers as a consumer, starts the worker loop, and handles cleanup on cancellation.
        """
        self._event_buffer_consumer_id = await self._event_buffer.register_consumer(
            self._name
        )

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
            self._started_event.clear()
            await self._publish_client.close()
            raise
        except Exception as e:
            logger.error(f"Publisher {self._name} encountered error: {e}")
            self._started_event.clear()
            await self._publish_client.close()
            raise

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
            self._metric_recorder.set_metric_value(
                self._published_counter_name,
                self._common_metric_tags,
                int(num_published),
            )
        if num_filtered > 0:
            self._metric_recorder.set_metric_value(
                self._filtered_counter_name, self._common_metric_tags, int(num_filtered)
            )
        self._metric_recorder.set_metric_value(
            self._consecutive_failures_gauge_name, self._common_metric_tags, 0
        )
        self._metric_recorder.set_metric_value(
            self._time_since_last_success_gauge_name, self._common_metric_tags, 0
        )
        self._metric_recorder.set_metric_value(
            self._publish_latency_hist_name,
            {**self._common_metric_tags, "Outcome": "success"},
            float(duration),
        )

    async def _record_retry_failure(
        self, duration: float, failed_attempts: int
    ) -> None:
        """Update Prometheus metrics for a retryable failure attempt."""
        self._metric_recorder.set_metric_value(
            self._consecutive_failures_gauge_name,
            self._common_metric_tags,
            int(failed_attempts),
        )
        self._metric_recorder.set_metric_value(
            self._publish_latency_hist_name,
            {**self._common_metric_tags, "Outcome": "failure"},
            float(duration),
        )

    async def _record_final_failure(
        self, num_failed_events: int, duration: float
    ) -> None:
        """Update in-memory stats and Prometheus metrics for a final (non-retryable) failure."""
        if num_failed_events > 0:
            self._metric_recorder.set_metric_value(
                self._failed_counter_name,
                self._common_metric_tags,
                int(num_failed_events),
            )
        self._metric_recorder.set_metric_value(
            self._consecutive_failures_gauge_name, self._common_metric_tags, 0
        )
        self._metric_recorder.set_metric_value(
            self._publish_latency_hist_name,
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
