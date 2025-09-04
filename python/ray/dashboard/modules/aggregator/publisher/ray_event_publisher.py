from abc import ABC, abstractmethod
import asyncio
import logging
import random
from typing import Dict, List, Optional

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

try:
    import prometheus_client
    from prometheus_client import Counter, Gauge, Histogram
except ImportError:
    prometheus_client = None

logger = logging.getLogger(__name__)


class RayEventPublisherInterface(ABC):
    """Abstract interface for publishing Ray event batches to external destinations."""

    @abstractmethod
    async def run_forever(self) -> None:
        """Run the publisher forever until cancellation or process death."""
        pass

    @abstractmethod
    async def get_and_reset_publisher_stats(self) -> Dict[str, int]:
        """Return a snapshot of publisher stats since last call and reset them."""
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
        common_metric_labels: Dict[str, str] = {"Component": "event_aggregator_agent"},
        max_retries: int = PUBLISHER_MAX_RETRIES,
        initial_backoff: float = PUBLISHER_INITIAL_BACKOFF_SECONDS,
        max_backoff: float = PUBLISHER_MAX_BACKOFF_SECONDS,
        jitter_ratio: float = PUBLISHER_JITTER_RATIO,
        enable_publisher_stats: bool = False,
    ) -> None:
        """Initialize a RayEventsPublisher.

        Args:
            name: Name identifier for this publisher instance
            publish_client: Client for publishing events to the destination
            event_buffer: Buffer for reading batches of events
            common_metric_labels: Common labels for all prometheus metrics
            max_retries: Maximum number of retries for failed publishes
            initial_backoff: Initial backoff time between retries in seconds
            max_backoff: Maximum backoff time between retries in seconds
            jitter_ratio: Random jitter ratio to add to backoff times
            enable_publisher_stats: If True, maintain in-memory publisher stats.
        """
        self._name = name
        self._common_metric_labels = common_metric_labels
        self._max_retries = int(max_retries)
        self._initial_backoff = float(initial_backoff)
        self._max_backoff = float(max_backoff)
        self._jitter_ratio = float(jitter_ratio)
        self._publish_client = publish_client
        self._event_buffer = event_buffer
        self._event_buffer_consumer_id = None
        self._stats_enabled = bool(enable_publisher_stats)

        # Internal stats (since last get_and_reset_publisher_stats call)
        self._events_published_since_last: int = 0
        self._events_filtered_out_since_last: int = 0
        self._events_publish_failures_since_last: int = 0
        self._stats_lock: Optional[asyncio.Lock] = (
            asyncio.Lock() if self._stats_enabled else None
        )
        # Event set once the publisher has registered as a consumer and is ready to publish events
        self._started_event: asyncio.Event = asyncio.Event()

        # Prometheus metrics (defined per-publisher instance using provided name)
        self._prom_metrics_enabled = bool(prometheus_client)
        if self._prom_metrics_enabled:
            metrics_prefix = "event_aggregator_agent"
            supported_labels = (
                tuple(self._common_metric_labels.keys())
                if self._common_metric_labels is not None
                else ()
            )
            self._published_counter = Counter(
                f"{metrics_prefix}_{self._name}_published_events_total",
                "Total number of events successfully published to the destination.",
                supported_labels,
                namespace="ray",
            )
            self._filtered_counter = Counter(
                f"{metrics_prefix}_{self._name}_filtered_events_total",
                "Total number of events filtered out before publishing to the destination.",
                supported_labels,
                namespace="ray",
            )
            self._failed_counter = Counter(
                f"{metrics_prefix}_{self._name}_failures_total",
                "Total number of events that failed to publish after retries.",
                supported_labels,
                namespace="ray",
            )
            self._publish_latency_hist = Histogram(
                f"{metrics_prefix}_{self._name}_publish_duration_seconds",
                "Duration of publish calls in seconds.",
                supported_labels + ("Outcome",),
                namespace="ray",
            )
            self._consecutive_failures_gauge = Gauge(
                f"{metrics_prefix}_{self._name}_consecutive_failures_since_last_success",
                "Number of consecutive failed publish attempts since the last success.",
                supported_labels,
                namespace="ray",
            )
            self._time_since_last_success_gauge = Gauge(
                f"{metrics_prefix}_{self._name}_time_since_last_success_seconds",
                "Seconds since the last successful publish to the destination.",
                supported_labels,
                namespace="ray",
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
                batch = await self._event_buffer.wait_for_batch(
                    self._event_buffer_consumer_id,
                    PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
                )
                await self._async_publish_with_retries(batch)
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

    async def get_and_reset_publisher_stats(self) -> Dict[str, int]:
        """Return a snapshot of publisher stats since last call and reset them.

        Returns a dict with the following keys:
            published: Number of events successfully published since last call
            filtered_out: Number of events filtered out before publishing since last call
            failed: Number of events that failed to publish since last call
        """
        if not self._stats_enabled:
            raise RuntimeError(
                "Publisher stats are disabled. Enable with enable_publisher_stats=True."
            )
        async with self._stats_lock:
            publisher_metrics = {
                "published": self._events_published_since_last,
                "filtered_out": self._events_filtered_out_since_last,
                "failed": self._events_publish_failures_since_last,
            }

            # Reset counters
            self._events_published_since_last = 0
            self._events_filtered_out_since_last = 0
            self._events_publish_failures_since_last = 0
            return publisher_metrics

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
        if self._stats_enabled and self._stats_lock is not None:
            async with self._stats_lock:
                self._events_published_since_last += int(num_published)
                self._events_filtered_out_since_last += int(num_filtered)
        if self._prom_metrics_enabled:
            self._published_counter.labels(**self._common_metric_labels).inc(
                int(num_published)
            )
            self._filtered_counter.labels(**self._common_metric_labels).inc(
                int(num_filtered)
            )
            self._consecutive_failures_gauge.labels(**self._common_metric_labels).set(0)
            self._time_since_last_success_gauge.labels(
                **self._common_metric_labels
            ).set(0)
            self._publish_latency_hist.labels(
                **self._common_metric_labels, Outcome="success"
            ).observe(float(duration))

    async def _record_retry_failure(
        self, duration: float, failed_attempts: int
    ) -> None:
        """Update Prometheus metrics for a retryable failure attempt."""
        if self._prom_metrics_enabled:
            self._consecutive_failures_gauge.labels(**self._common_metric_labels).set(
                int(failed_attempts)
            )
            self._publish_latency_hist.labels(
                **self._common_metric_labels, Outcome="failure"
            ).observe(float(duration))

    async def _record_final_failure(
        self, num_failed_events: int, duration: float
    ) -> None:
        """Update in-memory stats and Prometheus metrics for a final (non-retryable) failure."""
        if self._stats_enabled and self._stats_lock is not None:
            async with self._stats_lock:
                self._events_publish_failures_since_last += int(num_failed_events)
        if self._prom_metrics_enabled:
            self._failed_counter.labels(**self._common_metric_labels).inc(
                int(num_failed_events)
            )
            self._consecutive_failures_gauge.labels(**self._common_metric_labels).set(0)
            self._publish_latency_hist.labels(
                **self._common_metric_labels, Outcome="failure"
            ).observe(float(duration))


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

    async def get_and_reset_publisher_stats(self) -> Dict[str, int]:
        return {"published": 0, "filtered_out": 0, "failed": 0}

    async def wait_until_running(self, timeout: Optional[float] = None) -> bool:
        return True
