import asyncio
import signal
import time
import os
import json
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
from urllib3.util import Retry
from requests import Session
from requests.adapters import HTTPAdapter

from google.protobuf.json_format import MessageToJson

try:
    import prometheus_client
    from prometheus_client import Counter
except ImportError:
    prometheus_client = None

import ray
from ray._common.utils import get_or_create_event_loop
from ray._private import ray_constants
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.consts as dashboard_consts
from ray.core.generated import (
    events_event_aggregator_service_pb2,
    events_event_aggregator_service_pb2_grpc,
    events_base_event_pb2,
)
from ray._private.gcs_utils import create_gcs_channel
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated import gcs_service_pb2

logger = logging.getLogger(__name__)

# Environment variables for the aggregator agent
env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT"
# Max number of threads for the thread pool executor handling gRPC requests
GRPC_TPE_MAX_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_GRPC_TPE_MAX_WORKERS", 10
)
# Number of worker threads that publish events to the external service
PUBLISH_EVENT_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISH_EVENT_WORKERS", 1
)
# Interval to check the main thread liveness
CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS", 0.1
)
# Maximum size of the event buffer in the aggregator agent
MAX_EVENT_BUFFER_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_BUFFER_SIZE", 1000000
)
# Maximum sleep time between sending batches of events to the external service
MAX_BUFFER_SEND_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_MAX_BUFFER_SEND_INTERVAL_SECONDS", 0.1
)
# Maximum number of events to send in a single batch to the external service
MAX_EVENT_SEND_BATCH_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_SEND_BATCH_SIZE", 10000
)
# Maximum number of retries for sending events to the external service for a single request
REQUEST_BACKOFF_MAX = ray_constants.env_integer(
    f"{env_var_prefix}_REQUEST_BACKOFF_MAX", 5
)
# Backoff factor for the request retries
REQUEST_BACKOFF_FACTOR = ray_constants.env_float(
    f"{env_var_prefix}_REQUEST_BACKOFF_FACTOR", 1.0
)
# Address of the external service to send events
EVENT_SEND_ADDR = os.environ.get(
    f"{env_var_prefix}_EVENT_SEND_ADDR", "http://127.0.0.1"
)
# Port of the external service to send events
EVENT_SEND_PORT = ray_constants.env_integer(f"{env_var_prefix}_EVENT_SEND_PORT", 12345)
PUBLISH_EVENTS_TO_GCS = ray_constants.env_bool(
    f"{env_var_prefix}_PUBLISH_EVENTS_TO_GCS", True
)
# Interval to update metrics
METRICS_UPDATE_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_METRICS_UPDATE_INTERVAL_SECONDS", 0.1
)
# Event filtering configurations
# Comma-separated list of event types that are allowed to be exposed to external services
# Valid values: TASK_DEFINITION_EVENT, TASK_EXECUTION_EVENT, ACTOR_TASK_DEFINITION_EVENT, ACTOR_TASK_EXECUTION_EVENT
# The list of all supported event types can be found in src/ray/protobuf/events_base_event.proto (EventType enum)
# By default TASK_PROFILE_EVENT is not exposed to external services
DEFAULT_EXPOSABLE_EVENT_TYPES = "TASK_DEFINITION_EVENT,TASK_EXECUTION_EVENT,ACTOR_TASK_DEFINITION_EVENT,ACTOR_TASK_EXECUTION_EVENT"
EXPOSABLE_EVENT_TYPES = os.environ.get(
    f"{env_var_prefix}_EXPOSABLE_EVENT_TYPES", DEFAULT_EXPOSABLE_EVENT_TYPES
)

# Metrics
if prometheus_client:
    metrics_prefix = "event_aggregator_agent"
    events_received = Counter(
        f"{metrics_prefix}_events_received",
        "Total number of events received from the upstream components from the "
        "AddEvents gRPC call.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_add_to_aggregator = Counter(
        f"{metrics_prefix}_events_failed_to_add_to_aggregator",
        "Total number of events failed to add to the event aggregator. The metric "
        "counts the events received by the aggregator agent from the AddEvents gRPC "
        "call but failed to add to the buffer due to unexpected errors.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_at_event_aggregator = Counter(
        f"{metrics_prefix}_events_dropped_at_event_aggregator",
        "Total number of events dropped at the event aggregator due to the buffer "
        "being full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_published = Counter(
        f"{metrics_prefix}_events_published",
        "Total number of events successfully published to the external server.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_filtered_out = Counter(
        f"{metrics_prefix}_events_filtered_out",
        "Total number of events filtered out before publishing to external server.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_published_to_gcs = Counter(
        f"{metrics_prefix}_events_published_to_gcs",
        "Total number of events successfully published to GCS.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )


class AggregatorAgent(
    dashboard_utils.DashboardAgentModule,
    events_event_aggregator_service_pb2_grpc.EventAggregatorServiceServicer,
):
    """
    AggregatorAgent is a dashboard agent module that collects events sent with
    gRPC from other components, buffers them, and periodically sends them to GCS and an
    external service with HTTP POST requests for further processing or storage
    """

    class AccumulatedTaskMetadata:
        """
        Helper class to manage accumulated task events metadata with efficient deduplication.
        """

        def __init__(self):
            self.metadata = events_event_aggregator_service_pb2.TaskEventsMetadata()
            self._dropped_attempts_set = set()
            self._lock = threading.Lock()

        def merge(self, new_metadata) -> None:
            """
            Merge new task metadata, avoiding duplicates.

            Args:
                new_metadata: TaskEventsMetadata from incoming request
            """
            if not new_metadata:
                return

            with self._lock:
                for new_attempt in new_metadata.dropped_task_attempts:
                    attempt_key = (new_attempt.task_id, new_attempt.attempt_number)
                    if attempt_key not in self._dropped_attempts_set:
                        # Add to both protobuf and set
                        merged_attempt = self.metadata.dropped_task_attempts.add()
                        merged_attempt.CopyFrom(new_attempt)
                        self._dropped_attempts_set.add(attempt_key)

        def get_and_reset(self):
            """
            Get current metadata and reset for next batch.

            Returns:
                TaskEventsMetadata or None if empty
            """
            with self._lock:
                if len(self.metadata.dropped_task_attempts) == 0:
                    return None

                # Create copy of current metadata
                current_metadata = (
                    events_event_aggregator_service_pb2.TaskEventsMetadata()
                )
                current_metadata.CopyFrom(self.metadata)

                # Reset both structures efficiently
                self.metadata.Clear()
                self._dropped_attempts_set.clear()

                return current_metadata

        def is_empty(self) -> bool:
            """Check if metadata is empty."""
            with self._lock:
                return len(self.metadata.dropped_task_attempts) == 0

    def __init__(self, dashboard_agent) -> None:
        super().__init__(dashboard_agent)
        self._ip = dashboard_agent.ip
        self._pid = os.getpid()
        self._event_buffer = queue.Queue(maxsize=MAX_EVENT_BUFFER_SIZE)
        # Use the helper class for metadata management
        self._accumulated_task_metadata = self.AccumulatedTaskMetadata()
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=GRPC_TPE_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_grpc_executor",
        )
        self._gcs_channel = create_gcs_channel(self.gcs_address)
        self._gcs_event_stub = gcs_service_pb2_grpc.RayEventExportGcsServiceStub(
            self._gcs_channel
        )

        self._http_session = Session()
        retries = Retry(
            total=REQUEST_BACKOFF_MAX,
            backoff_factor=REQUEST_BACKOFF_FACTOR,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods={"POST"},
            respect_retry_after_header=True,
        )
        self._http_session.mount("http://", HTTPAdapter(max_retries=retries))
        self._http_session.mount("https://", HTTPAdapter(max_retries=retries))

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._publisher_threads = []
        self._events_received_since_last_metrics_update = 0
        self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
        self._events_dropped_at_event_aggregator_since_last_metrics_update = 0
        self._events_published_since_last_metrics_update = 0
        self._events_filtered_out_since_last_metrics_update = 0
        self._events_published_to_gcs_since_last_metrics_update = 0

        self._orig_sigterm_handler = signal.signal(
            signal.SIGTERM, self._sigterm_handler
        )

        self._is_cleanup = False
        self._cleanup_finished_event = threading.Event()

        self._exposable_event_types = {
            event_type.strip()
            for event_type in EXPOSABLE_EVENT_TYPES.split(",")
            if event_type.strip()
        }

    async def AddEvents(self, request, context) -> None:
        """
        gRPC handler for adding events to the event aggregator
        """
        loop = get_or_create_event_loop()

        return await loop.run_in_executor(
            self._grpc_executor, self._receive_events, request
        )

    def _receive_events(self, request):
        """
        Receives events from the request, adds them to the event buffer,
        """
        events_data = request.events_data
        task_events_metadata = (
            events_data.task_events_metadata
            if events_data.HasField("task_events_metadata")
            else None
        )

        # Merge metadata using helper class
        self._accumulated_task_metadata.merge(task_events_metadata)

        for event in events_data.events:
            with self._lock:
                self._events_received_since_last_metrics_update += 1
            try:
                self._event_buffer.put_nowait(event)
            except queue.Full:
                self._event_buffer.get_nowait()
                self._event_buffer.put_nowait(event)
                with self._lock:
                    self._events_dropped_at_event_aggregator_since_last_metrics_update += (
                        1
                    )
            except Exception as e:
                logger.error(
                    f"Failed to add event with id={event.event_id.decode()} to buffer. "
                    "Error: %s",
                    e,
                )
                with self._lock:
                    self._events_failed_to_add_to_aggregator_since_last_metrics_update += (
                        1
                    )

        return events_event_aggregator_service_pb2.AddEventsReply()

    def _can_expose_event(self, event) -> bool:
        """
        Check if an event should be allowed to be sent to external services.
        """
        return (
            events_base_event_pb2.RayEvent.EventType.Name(event.event_type)
            in self._exposable_event_types
        )

    def _create_ray_events_data(self, event_batch, task_events_metadata=None):
        """
        Helper method to create RayEventsData from event batch and metadata.

        Args:
            event_batch: List of RayEvent protobuf objects
            task_events_metadata: TaskEventsMetadata object (optional)

        Returns:
            RayEventsData protobuf object
        """
        events_data = events_event_aggregator_service_pb2.RayEventsData()
        events_data.events.extend(event_batch)

        if task_events_metadata:
            events_data.task_events_metadata.CopyFrom(task_events_metadata)

        return events_data

    def _send_events_to_gcs(self, event_batch, task_events_metadata=None) -> bool:
        """
        Sends a batch of events to GCS via the GCS grpc client.

        Args:
            event_batch: List of RayEvent protobuf objects
            task_events_metadata: TaskEventsMetadata object (optional)
        """
        if not PUBLISH_EVENTS_TO_GCS:
            return True

        if not event_batch and len(task_events_metadata.dropped_task_attempts) == 0:
            return True

        try:
            # Create RayEventsData from the event batch
            events_data = self._create_ray_events_data(
                event_batch, task_events_metadata
            )

            # Create the request
            request = events_event_aggregator_service_pb2.AddEventsRequest(
                events_data=events_data
            )

            # Call GCS service
            response = self._gcs_event_stub.AddEvents(request)

            # Check response status
            if response.status.code != 0:
                logger.error(f"GCS AddEvents failed: {response.status.message}")
                return False

            with self._lock:
                self._events_published_to_gcs_since_last_metrics_update += len(
                    event_batch
                )

            return True

        except Exception as e:
            logger.error(f"Failed to send events to GCS: {e}")
            return False

    def _send_events_to_external_service(self, event_batch) -> bool:
        """
        Sends a batch of events to the external service via HTTP POST request

        Returns:
            bool: True if successful, False if failed
        """
        if not event_batch:
            return True

        filtered_event_batch = [
            event for event in event_batch if self._can_expose_event(event)
        ]
        if not filtered_event_batch:
            # All events were filtered out, update metrics and return success
            with self._lock:
                self._events_filtered_out_since_last_metrics_update += len(event_batch)
            return True

        # Convert protobuf objects to JSON dictionaries for HTTP POST
        filtered_event_batch_json = [
            json.loads(MessageToJson(event)) for event in filtered_event_batch
        ]

        try:
            response = self._http_session.post(
                f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}", json=filtered_event_batch_json
            )
            response.raise_for_status()
            with self._lock:
                self._events_published_since_last_metrics_update += len(
                    filtered_event_batch
                )
                self._events_filtered_out_since_last_metrics_update += len(
                    event_batch
                ) - len(filtered_event_batch)
            return True  # Success
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)
            return False  # Failure

    def _publish_events(self) -> None:
        """
        Continuously publishes events from the event buffer to both GCS and external service
        """
        event_batch = []
        task_events_metadata = events_event_aggregator_service_pb2.TaskEventsMetadata()
        published_events_to_gcs = False

        while True:
            while len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
                try:
                    event_proto = self._event_buffer.get(block=False)
                    event_batch.append(event_proto)
                except queue.Empty:
                    break

            if event_batch:
                if not published_events_to_gcs:
                    # Get task events metadata only when starting a new batch (not on retries)
                    # This need not be in sync with the event batch.
                    if len(task_events_metadata.dropped_task_attempts) == 0:
                        task_events_metadata = (
                            self._accumulated_task_metadata.get_and_reset()
                        )

                    published_events_to_gcs = self._send_events_to_gcs(
                        event_batch, task_events_metadata
                    )

                    # If GCS fails, don't attempt external service publish - retry in next iteration
                    if not published_events_to_gcs:
                        logger.warning(
                            f"GCS publication failed for {len(event_batch)} events, will retry"
                        )
                        continue  # Retry same batch in next iteration

                # GCS succeeded, now try external service
                external_success = self._send_events_to_external_service(event_batch)

                if external_success:
                    # Both Publish succeeded - move to next batch
                    event_batch.clear()
                    if task_events_metadata:
                        task_events_metadata.Clear()
                    published_events_to_gcs = False
                else:
                    # GCS succeeded but external svc publish failed - retry only external service
                    logger.warning(
                        f"External service publication failed for {len(event_batch)} events, will retry external only"
                    )

            else:
                # No events in batch
                should_stop = self._stop_event.wait(MAX_BUFFER_SEND_INTERVAL_SECONDS)
                if should_stop:
                    # Send any remaining inflight events before stopping
                    if event_batch:
                        if not published_events_to_gcs:
                            if len(task_events_metadata.dropped_task_attempts) == 0:
                                task_events_metadata = (
                                    self._accumulated_task_metadata.get_and_reset()
                                )
                            self._send_events_to_gcs(event_batch, task_events_metadata)
                        self._send_events_to_external_service(event_batch)
                    return

    def _update_metrics(self) -> None:
        """
        Updates the Prometheus metrics
        """
        if not prometheus_client:
            return

        with self._lock:
            _events_received = self._events_received_since_last_metrics_update
            _events_failed_to_add_to_aggregator = (
                self._events_failed_to_add_to_aggregator_since_last_metrics_update
            )
            _events_dropped_at_event_aggregator = (
                self._events_dropped_at_event_aggregator_since_last_metrics_update
            )
            _events_published = self._events_published_since_last_metrics_update
            _events_filtered_out = self._events_filtered_out_since_last_metrics_update
            _events_published_to_gcs = (
                self._events_published_to_gcs_since_last_metrics_update
            )

            self._events_received_since_last_metrics_update = 0
            self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
            self._events_dropped_at_event_aggregator_since_last_metrics_update = 0
            self._events_published_since_last_metrics_update = 0
            self._events_filtered_out_since_last_metrics_update = 0
            self._events_published_to_gcs_since_last_metrics_update = 0

        labels = {
            "ip": self._ip,
            "pid": self._pid,
            "Version": ray.__version__,
            "Component": "event_aggregator_agent",
            "SessionName": self.session_name,
        }
        events_received.labels(**labels).inc(_events_received)
        events_failed_to_add_to_aggregator.labels(**labels).inc(
            _events_failed_to_add_to_aggregator
        )
        events_dropped_at_event_aggregator.labels(**labels).inc(
            _events_dropped_at_event_aggregator
        )
        events_published.labels(**labels).inc(_events_published)
        events_filtered_out.labels(**labels).inc(_events_filtered_out)
        events_published_to_gcs.labels(**labels).inc(_events_published_to_gcs)

    def _check_main_thread_liveness(self) -> None:
        """
        Continuously checks if the main thread is alive. If the main thread is not alive,
        it sets the stop event to trigger cleanup and shutdown of the agent.
        """
        while True:
            if not threading.main_thread().is_alive():
                self._stop_event.set()
            if self._stop_event.is_set():
                self._cleanup()
                break
            time.sleep(CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS)

    def _cleanup(self) -> None:
        """
        Cleans up the aggregator agent by stopping the publisher threads,
        sending any remaining events in the buffer, and updating metrics.
        """

        should_wait_cleanup_finished = False
        with self._lock:
            if self._is_cleanup:
                should_wait_cleanup_finished = True
            self._is_cleanup = True

        if should_wait_cleanup_finished:
            # If cleanup is already in progress, wait for it to finish.
            self._cleanup_finished_event.wait()
            return

        # Send any remaining events in the buffer
        event_batch = []
        while True:
            try:
                event_proto = self._event_buffer.get(block=False)
                event_batch.append(event_proto)
            except:  # noqa: E722
                break

        if event_batch:
            # Get any remaining accumulated metadata
            task_events_metadata = self._accumulated_task_metadata.get_and_reset()
            self._send_events_to_gcs(event_batch, task_events_metadata)
            self._send_events_to_external_service(event_batch)

        for thread in self._publisher_threads:
            thread.join()

        # Update metrics immediately
        self._update_metrics()

        self._cleanup_finished_event.set()

    def _sigterm_handler(self, signum: int, frame) -> None:
        self._stop_event.set()
        self._cleanup()
        self._orig_sigterm_handler(signum, frame)

    async def run(self, server) -> None:
        if server:
            events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(
                self, server
            )

        thread = threading.Thread(
            target=self._check_main_thread_liveness,
            name="event_aggregator_agent_check_main_thread_liveness",
            daemon=False,
        )
        thread.start()

        for _ in range(PUBLISH_EVENT_WORKERS):
            thread = threading.Thread(
                target=self._publish_events,
                name="event_aggregator_agent_publish_events",
                daemon=False,
            )
            self._publisher_threads.append(thread)
            thread.start()

        while True:
            self._update_metrics()
            await asyncio.sleep(METRICS_UPDATE_INTERVAL_SECONDS)

    @staticmethod
    def is_minimal_module() -> bool:
        return False
