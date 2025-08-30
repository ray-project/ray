from concurrent.futures import ThreadPoolExecutor
import json
import logging

from ray._common.utils import get_or_create_event_loop
import aiohttp
from ray._private.protobuf_compat import message_to_json
from ray.core.generated import events_base_event_pb2
from ray.dashboard.modules.aggregator.publisher.configs import PUBLISHER_TIMEOUT_SECONDS
from typing import Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class PublishStats:
    """Data class that represents stats of publishing a batch of events."""

    is_publish_successful: bool
    num_events_published: int
    num_events_filtered_out: int


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
        """Clean up any resources used by this client. Should be called when the publisherClient is no longer required"""
        pass


class AsyncHttpPublisherClient(PublisherClientInterface):
    """Client for publishing ray event batches to an external HTTP service."""

    def __init__(
        self,
        endpoint: str,
        executor: ThreadPoolExecutor,
        events_filter_fn: Callable[[object], bool],
        timeout: float = PUBLISHER_TIMEOUT_SECONDS,
    ) -> None:
        self._endpoint = endpoint
        self._executor = executor
        self._events_filter_fn = events_filter_fn
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = None

    async def publish(
        self, events_batch: list[events_base_event_pb2.RayEvent]
    ) -> PublishStats:
        if not events_batch:
            return PublishStats(True, 0, 0)
        filtered = [e for e in events_batch if self._events_filter_fn(e)]
        num_filtered_out = len(events_batch) - len(filtered)
        if not filtered:
            # All filtered out -> success but nothing published
            return PublishStats(True, 0, num_filtered_out)

        # Convert protobuf objects to python dictionaries for HTTP POST
        filtered_json = await get_or_create_event_loop().run_in_executor(
            self._executor,
            lambda: [
                json.loads(
                    message_to_json(e, always_print_fields_with_no_presence=True)
                )
                for e in filtered
            ],
        )

        try:
            # Create session on first use (lazy initialization)
            if not self._session:
                self._session = aiohttp.ClientSession(timeout=self._timeout)

            return await self._send_http_request(filtered_json, num_filtered_out)
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)
            return PublishStats(False, 0, 0)

    async def _send_http_request(self, json_data, num_filtered_out):
        async with self._session.post(
            self._endpoint,
            json=json_data,
        ) as resp:
            resp.raise_for_status()
            return PublishStats(True, len(json_data), num_filtered_out)

    def count_num_events_in_batch(
        self, events_batch: list[events_base_event_pb2.RayEvent]
    ) -> int:
        return len(events_batch)

    async def close(self) -> None:
        """Closes the http session if one was created. Should be called when the publisherClient is no longer required"""
        if self._session:
            await self._session.close()
            self._session = None

    def set_session(self, session) -> None:
        """Inject an HTTP client session. Intended for testing.

        If a session is set explicitly, it will be used and managed by close().
        """
        self._session = session
