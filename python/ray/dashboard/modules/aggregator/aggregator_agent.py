import asyncio
import signal
import time
import os
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
from ray.dashboard.modules.aggregator.task_metadata_buffer import TaskMetadataBuffer


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

from .ray_events_publisher import GCSPublisher, ExternalPublisher, NoopPublisher

logger = logging.getLogger(__name__)

# Environment variables for the aggregator agent
env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT"
# Max number of threads for the thread pool executor handling gRPC requests
GRPC_TPE_MAX_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_GRPC_TPE_MAX_WORKERS", 10
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
# Timeout for the external service to send events
EVENT_PUBLISH_TIMEOUT_SECONDS = ray_constants.env_integer(
    f"{env_var_prefix}_EVENT_PUBLISH_TIMEOUT_SECONDS", 5
)
PUBLISH_EVENTS_TO_GCS = ray_constants.env_bool(
    f"{env_var_prefix}_PUBLISH_EVENTS_TO_GCS", False
)
PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC = ray_constants.env_bool(
    f"{env_var_prefix}_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC", True
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
# Destination publisher queues and retry controls
PUBLISHER_QUEUE_MAX_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISH_DEST_QUEUE_MAX_SIZE", 50
)
PUBLISHER_MAX_RETRIES = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISH_MAX_RETRIES", 5
)
PUBLISHER_INITIAL_BACKOFF_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_PUBLISH_INITIAL_BACKOFF_SECONDS", 0.01
)
PUBLISHER_MAX_BACKOFF_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_PUBLISH_MAX_BACKOFF_SECONDS", 5.0
)
PUBLISHER_JITTER_RATIO = ray_constants.env_float(
    f"{env_var_prefix}_PUBLISH_JITTER_RATIO", 0.1
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
    metadata_dropped_at_event_aggregator = Counter(
        f"{metrics_prefix}_metadata_dropped_at_event_aggregator",
        "Total number of task metadata entries dropped at the event aggregator due to buffer being full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_publish_to_gcs = Counter(
        f"{metrics_prefix}_events_failed_to_publish_to_gcs",
        "Total number of events failed to publish to GCS after retries.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_publish_to_external_svc = Counter(
        f"{metrics_prefix}_events_failed_to_publish_to_external_svc",
        "Total number of events failed to publish to the external service after retries.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_in_gcs_publish_queue = Counter(
        f"{metrics_prefix}_events_dropped_in_gcs_publish_queue",
        "Total number of events dropped due to the GCS publish queue being full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_in_external_svc_publish_queue = Counter(
        f"{metrics_prefix}_events_dropped_in_external_svc_publish_queue",
        "Total number of events dropped due to the external publish queue being full.",
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

    def __init__(self, dashboard_agent) -> None:
        super().__init__(dashboard_agent)
        self._ip = dashboard_agent.ip
        self._pid = os.getpid()
        self._event_buffer = queue.Queue(maxsize=MAX_EVENT_BUFFER_SIZE)
        self._task_metadata_buffer = TaskMetadataBuffer()
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=GRPC_TPE_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_grpc_executor",
        )
        # Dedicated publisher event loop thread
        self._publisher_thread = None

        self._gcs_channel = create_gcs_channel(self.gcs_address, True)
        self._gcs_event_stub = gcs_service_pb2_grpc.RayEventExportGcsServiceStub(
            self._gcs_channel
        )

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._events_received_since_last_metrics_update = 0
        self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
        self._events_dropped_at_event_aggregator_since_last_metrics_update = 0

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

        # initialize publishers
        if PUBLISH_EVENTS_TO_GCS:
            self._gcs_publisher = GCSPublisher(
                gcs_event_stub=self._gcs_event_stub,
                timeout=EVENT_PUBLISH_TIMEOUT_SECONDS,
                queue_max_size=PUBLISHER_QUEUE_MAX_SIZE,
                max_retries=PUBLISHER_MAX_RETRIES,
                initial_backoff=PUBLISHER_INITIAL_BACKOFF_SECONDS,
                max_backoff=PUBLISHER_MAX_BACKOFF_SECONDS,
                jitter_ratio=PUBLISHER_JITTER_RATIO,
            )
        else:
            self._gcs_publisher = NoopPublisher()

        if PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC:
            endpoint = f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}"
            self._external_publisher = ExternalPublisher(
                endpoint=endpoint,
                events_filter_fn=self._can_expose_event,
                timeout=EVENT_PUBLISH_TIMEOUT_SECONDS,
                queue_max_size=PUBLISHER_QUEUE_MAX_SIZE,
                max_retries=PUBLISHER_MAX_RETRIES,
                initial_backoff=PUBLISHER_INITIAL_BACKOFF_SECONDS,
                max_backoff=PUBLISHER_MAX_BACKOFF_SECONDS,
                jitter_ratio=PUBLISHER_JITTER_RATIO,
            )
        else:
            self._external_publisher = NoopPublisher()

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

        self._task_metadata_buffer.merge(events_data.task_events_metadata)

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

    # TODO: This is a temporary solution to drain the event buffer to publishers when stopping the agent.
    # We need to find a better way to do this.
    async def _drain_event_buffer_to_publishers(self) -> None:
        """Drain remaining events from the internal buffer and enqueue to publishers."""
        # Drain events
        draining_event_batch = []
        while True:
            try:
                event_proto = self._event_buffer.get(block=False)
                draining_event_batch.append(event_proto)
            except Exception:
                break

        # Drain task metadata
        draining_task_metadata_batch = (
            events_event_aggregator_service_pb2.TaskEventsMetadata()
        )
        metadata_batch = self._task_metadata_buffer.get()
        while metadata_batch and len(metadata_batch.dropped_task_attempts) > 0:
            draining_task_metadata_batch.dropped_task_attempts.extend(
                metadata_batch.dropped_task_attempts
            )
            metadata_batch = self._task_metadata_buffer.get()

        if (
            draining_event_batch
            or len(draining_task_metadata_batch.dropped_task_attempts) > 0
        ):
            frozen_batch = tuple(draining_event_batch)
            self._gcs_publisher.enqueue((frozen_batch, draining_task_metadata_batch))
            if draining_event_batch:
                self._external_publisher.enqueue(frozen_batch)

    async def _publish_events(self) -> None:
        """
        Continuously builds batches from the event buffer and enqueues them to publishers.
        If both destination queues are full, it waits briefly and retries to avoid
        building more batches and growing memory.
        """
        event_batch = []

        while True:
            # Check if stop event is set
            if self._stop_event.is_set():
                break

            # If both destination queues are full, wait a bit before pulling from buffer
            if (
                not self._gcs_publisher.has_capacity()
                and not self._external_publisher.has_capacity()
            ):
                await asyncio.sleep(MAX_BUFFER_SEND_INTERVAL_SECONDS)
                if self._stop_event.is_set():
                    break
                continue

            # prepare a batch of events to send
            while len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
                try:
                    event_proto = self._event_buffer.get(block=False)
                    event_batch.append(event_proto)
                except queue.Empty:
                    break

            if event_batch:
                task_events_metadata = self._task_metadata_buffer.get()
                frozen_batch = tuple(event_batch)
                # Enqueue batch to publishers
                self._gcs_publisher.enqueue((frozen_batch, task_events_metadata))
                self._external_publisher.enqueue(frozen_batch)

                # Reset local batch
                event_batch.clear()
            else:
                # wait for a bit before pulling from buffer again
                await asyncio.sleep(MAX_BUFFER_SEND_INTERVAL_SECONDS)

    async def _publisher_event_loop(self):
        # Start destination specific publishers
        self._gcs_publisher.start()
        self._external_publisher.start()

        # Launch one batching task to continuously build batches of events and enqueue to publishers
        batching_task = asyncio.create_task(
            self._publish_events(),
            name="event_aggregator_agent_publish_events",
        )
        try:
            # Await batching completion (it will exit when stop event is set)
            await batching_task
        finally:
            # Drain any remaining items and shutdown publishers
            await self._drain_event_buffer_to_publishers()
            await asyncio.gather(
                self._gcs_publisher.shutdown(), self._external_publisher.shutdown()
            )
            self._update_metrics()

    def _create_and_start_publisher_loop(self) -> None:
        """Creates and starts a dedicated asyncio event loop with multiple async publisher workers."""
        loop = get_or_create_event_loop()
        try:
            loop.run_until_complete(self._publisher_event_loop())
        finally:
            loop.close()

    def _update_metrics(self) -> None:
        """
        Updates the Prometheus metrics
        """
        if not prometheus_client:
            return

        # Pull publisher stats without holding the aggregator lock to avoid blocking
        gcs_publisher_metrics = self._gcs_publisher.get_and_reset_metrics()
        external_publisher_metrics = self._external_publisher.get_and_reset_metrics()

        with self._lock:
            _events_received = self._events_received_since_last_metrics_update
            _events_failed_to_add_to_aggregator = (
                self._events_failed_to_add_to_aggregator_since_last_metrics_update
            )
            _events_dropped_at_event_aggregator = (
                self._events_dropped_at_event_aggregator_since_last_metrics_update
            )

            # Publisher stats
            _events_published_to_gcs = gcs_publisher_metrics.get("published", 0)
            _events_failed_to_publish_to_gcs = gcs_publisher_metrics.get("failed", 0)
            _events_dropped_in_gcs_publish_queue = gcs_publisher_metrics.get(
                "queue_dropped", 0
            )

            _events_published = external_publisher_metrics.get("published", 0)
            _events_filtered_out = external_publisher_metrics.get("filtered_out", 0)
            _events_failed_to_publish_to_external = external_publisher_metrics.get(
                "failed", 0
            )
            _events_dropped_in_external_publish_queue = external_publisher_metrics.get(
                "queue_dropped", 0
            )

            # Now reset aggregator counters
            self._events_received_since_last_metrics_update = 0
            self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
            self._events_dropped_at_event_aggregator_since_last_metrics_update = 0

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
        events_failed_to_publish_to_gcs.labels(**labels).inc(
            _events_failed_to_publish_to_gcs
        )
        events_failed_to_publish_to_external_svc.labels(**labels).inc(
            _events_failed_to_publish_to_external
        )
        events_dropped_in_gcs_publish_queue.labels(**labels).inc(
            _events_dropped_in_gcs_publish_queue
        )
        events_dropped_in_external_svc_publish_queue.labels(**labels).inc(
            _events_dropped_in_external_publish_queue
        )
        metadata_dropped_at_event_aggregator.labels(**labels).inc(
            self._task_metadata_buffer.get_and_reset_dropped_metadata_count()
        )

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

        # Update metrics immediately
        self._update_metrics()

        # Join publisher loop thread to ensure clean shutdown
        if self._publisher_thread is not None:
            try:
                self._publisher_thread.join()
            except Exception:
                pass

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

        # Start a dedicated publisher event loop in a separate thread
        publisher_thread = threading.Thread(
            target=self._create_and_start_publisher_loop,
            name="event_aggregator_agent_publisher_loop",
            daemon=False,
        )
        self._publisher_thread = publisher_thread
        publisher_thread.start()

        while True:
            self._update_metrics()
            await asyncio.sleep(METRICS_UPDATE_INTERVAL_SECONDS)

    @staticmethod
    def is_minimal_module() -> bool:
        return False
