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
from ray._private.protobuf_compat import message_to_json

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
# Address of the external service to send events with format of "http://<ip>:<port>"
EVENTS_EXPORT_ADDR = os.environ.get(f"{env_var_prefix}_EVENTS_EXPORT_ADDR", "")
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
        "Total number of events filtered out before publishing to external server. The "
        "metric counts the events that are received by the aggregator agent but are "
        "not part of the public API yet.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )


class AggregatorAgent(
    dashboard_utils.DashboardAgentModule,
    events_event_aggregator_service_pb2_grpc.EventAggregatorServiceServicer,
):
    """
    AggregatorAgent is a dashboard agent module that collects events sent with
    gRPC from other components, buffers them, and periodically sends them to an
    external service with HTTP POST requests for further processing or storage
    """

    def __init__(self, dashboard_agent) -> None:
        super().__init__(dashboard_agent)
        self._ip = dashboard_agent.ip
        self._pid = os.getpid()
        self._event_buffer = queue.Queue(maxsize=MAX_EVENT_BUFFER_SIZE)
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=GRPC_TPE_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_grpc_executor",
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
        self._events_export_addr = (
            dashboard_agent.events_export_addr or EVENTS_EXPORT_ADDR
        )

        self._event_http_target_enabled = bool(self._events_export_addr)
        if not self._event_http_target_enabled:
            logger.info(
                "Event HTTP target not set, skipping sending events to "
                f"external http service. events_export_addr: {self._events_export_addr}"
            )

        self._event_processing_enabled = self._event_http_target_enabled
        if self._event_processing_enabled:
            logger.info("Event processing enabled")
        else:
            logger.info("Event processing disabled")

        self._exposable_event_types = {
            event_type.strip()
            for event_type in EXPOSABLE_EVENT_TYPES.split(",")
            if event_type.strip()
        }

        self._orig_sigterm_handler = signal.signal(
            signal.SIGTERM, self._sigterm_handler
        )

        self._is_cleanup = False
        self._cleanup_finished_event = threading.Event()

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
        if not self._event_processing_enabled:
            return events_event_aggregator_service_pb2.AddEventsReply()

        # TODO(myan) #54515: Considering adding a mechanism to also send out the events
        # metadata (e.g. dropped task attempts) to help with event processing at the
        # downstream
        events_data = request.events_data
        for event in events_data.events:
            with self._lock:
                self._events_received_since_last_metrics_update += 1
            try:
                self._event_buffer.put_nowait(event)
            except queue.Full:
                # Remove the oldest event to make room for the new event.
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

    def _send_events_to_external_service(self, event_batch) -> None:
        """
        Sends a batch of events to the external service via HTTP POST request
        """
        if not event_batch or not self._event_http_target_enabled:
            return

        filtered_event_batch = [
            event for event in event_batch if self._can_expose_event(event)
        ]
        if not filtered_event_batch:
            # All events were filtered out, update metrics and return to avoid an empty POST.
            with self._lock:
                self._events_filtered_out_since_last_metrics_update += len(event_batch)
            event_batch.clear()
            return

        # Convert protobuf objects to JSON dictionaries for HTTP POST
        filtered_event_batch_json = [
            json.loads(
                message_to_json(event, always_print_fields_with_no_presence=True)
            )
            for event in filtered_event_batch
        ]

        try:
            response = self._http_session.post(
                f"{self._events_export_addr}", json=filtered_event_batch_json
            )
            response.raise_for_status()
            with self._lock:
                self._events_published_since_last_metrics_update += len(
                    filtered_event_batch
                )
                self._events_filtered_out_since_last_metrics_update += len(
                    event_batch
                ) - len(filtered_event_batch)
            event_batch.clear()
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)

    def _publish_events(self) -> None:
        """
        Continuously publishes events from the event buffer to the external service
        """
        event_batch = []

        while True:
            while len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
                try:
                    event_proto = self._event_buffer.get(block=False)
                    event_batch.append(event_proto)
                except queue.Empty:
                    break

            if event_batch:
                # Send the batch of events to the external service.
                # If failed, event_batch will be reused in the next iteration.
                # Retry sending with other events in the next iteration.
                self._send_events_to_external_service(event_batch)
            else:
                should_stop = self._stop_event.wait(MAX_BUFFER_SEND_INTERVAL_SECONDS)
                if should_stop:
                    # Send any remaining events before stopping.
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

            self._events_received_since_last_metrics_update = 0
            self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
            self._events_dropped_at_event_aggregator_since_last_metrics_update = 0
            self._events_published_since_last_metrics_update = 0
            self._events_filtered_out_since_last_metrics_update = 0

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
