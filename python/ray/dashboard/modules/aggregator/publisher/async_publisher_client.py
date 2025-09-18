import json
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Optional, Tuple

import aiohttp

from ray._common.utils import get_or_create_event_loop
from ray._private.protobuf_compat import message_to_json
from ray.core.generated import (
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
)
from ray.dashboard.modules.aggregator.publisher.configs import (
    GCS_EXPOSABLE_EVENT_TYPES,
    HTTP_EXPOSABLE_EVENT_TYPES,
    PUBLISHER_TIMEOUT_SECONDS,
)

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

    def __init__(self):
        self._exposable_event_types = None

    def _can_expose_event(self, event) -> bool:
        """
        Check if an event should be allowed to be published.
        """
        if self._exposable_event_types is None:
            return True

        if isinstance(self._exposable_event_types, (list, set)):
            # For list/set of event types
            event_type_name = events_base_event_pb2.RayEvent.EventType.Name(
                event.event_type
            )
            return event_type_name in self._exposable_event_types
        else:
            raise ValueError(
                f"Invalid exposable event types: {self._exposable_event_types}"
            )

        return True

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
        timeout: float = PUBLISHER_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        self._endpoint = endpoint
        self._executor = executor
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = None

        # Initialize exposable event types from environment variable
        self._exposable_event_types = {
            event_type.strip()
            for event_type in HTTP_EXPOSABLE_EVENT_TYPES.split(",")
            if event_type.strip()
        }

    async def publish(
        self, events_batch: list[events_base_event_pb2.RayEvent]
    ) -> PublishStats:
        if not events_batch:
            # Nothing to publish -> success but nothing published
            return PublishStats(
                is_publish_successful=True,
                num_events_published=0,
                num_events_filtered_out=0,
            )
        filtered = [e for e in events_batch if self._can_expose_event(e)]
        num_filtered_out = len(events_batch) - len(filtered)
        if not filtered:
            # All filtered out -> success but nothing published
            return PublishStats(
                is_publish_successful=True,
                num_events_published=0,
                num_events_filtered_out=num_filtered_out,
            )

        # Convert protobuf objects to python dictionaries for HTTP POST. Run in executor to avoid blocking the event loop.
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
            return PublishStats(
                is_publish_successful=False,
                num_events_published=0,
                num_events_filtered_out=0,
            )

    async def _send_http_request(self, json_data, num_filtered_out) -> PublishStats:
        async with self._session.post(
            self._endpoint,
            json=json_data,
        ) as resp:
            resp.raise_for_status()
            return PublishStats(
                is_publish_successful=True,
                num_events_published=len(json_data),
                num_events_filtered_out=num_filtered_out,
            )

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
        """Inject an HTTP client session.

        If a session is set explicitly, it will be used and managed by close().
        """
        self._session = session


class AsyncGCSPublisherClient(PublisherClientInterface):
    """Client for publishing ray event batches to GCS."""

    def __init__(
        self,
        async_gcs_ray_event_export_service_stub,
        timeout: float = PUBLISHER_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        self._async_gcs_ray_event_export_service_stub = (
            async_gcs_ray_event_export_service_stub
        )
        self._timeout = timeout

        self._exposable_event_types = GCS_EXPOSABLE_EVENT_TYPES

    async def publish(
        self,
        events_batch: Tuple[
            List[events_base_event_pb2.RayEvent],
            Optional[events_event_aggregator_service_pb2.TaskEventsMetadata],
        ],
    ) -> PublishStats:
        events, task_events_metadata = events_batch

        has_task_events_metadata = (
            task_events_metadata and task_events_metadata.dropped_task_attempts
        )
        if not events and not has_task_events_metadata:
            # Nothing to publish -> success but nothing published
            return PublishStats(
                is_publish_successful=True,
                num_events_published=0,
                num_events_filtered_out=0,
            )

        # Filter events based on exposable event types
        filtered_events = [e for e in events if self._can_expose_event(e)]
        num_filtered_out = len(events) - len(filtered_events)

        if not filtered_events and not has_task_events_metadata:
            # all events filtered out and no task events metadata -> success but nothing published
            return PublishStats(
                is_publish_successful=True,
                num_events_published=0,
                num_events_filtered_out=num_filtered_out,
            )

        try:
            events_data = self._create_ray_events_data(
                filtered_events, task_events_metadata
            )
            request = events_event_aggregator_service_pb2.AddEventsRequest(
                events_data=events_data
            )
            response = await self._async_gcs_ray_event_export_service_stub.AddEvents(
                request, timeout=self._timeout
            )
            if response.status.code != 0:
                logger.error(f"GCS AddEvents failed: {response.status.message}")
                return PublishStats(
                    is_publish_successful=False,
                    num_events_published=0,
                    num_events_filtered_out=0,
                )
            return PublishStats(
                is_publish_successful=True,
                num_events_published=len(filtered_events),
                num_events_filtered_out=num_filtered_out,
            )
        except Exception as e:
            logger.error(f"Failed to send events to GCS: {e}")
            return PublishStats(
                is_publish_successful=False,
                num_events_published=0,
                num_events_filtered_out=0,
            )

    def count_num_events_in_batch(
        self,
        events_batch: Tuple[
            List[events_base_event_pb2.RayEvent],
            events_event_aggregator_service_pb2.TaskEventsMetadata,
        ],
    ) -> int:
        events, _ = events_batch
        return len(events)

    def _create_ray_events_data(
        self,
        event_batch: List[events_base_event_pb2.RayEvent],
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

    async def close(self) -> None:
        pass
