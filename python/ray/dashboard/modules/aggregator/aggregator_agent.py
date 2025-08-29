import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
import logging
<<<<<<< HEAD
from ray._private.gcs_utils import create_gcs_channel
from ray.dashboard.modules.aggregator.ray_events_publisher import (
    ExternalSvcPublisher,
    GCSPublisher,
    NoopPublisher,
)

from ray.dashboard.modules.aggregator.bounded_queue import BoundedQueue
from ray.dashboard.modules.aggregator.task_metadata_buffer import TaskMetadataBuffer
=======
from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)
from ray.dashboard.modules.aggregator.publisher.async_publisher_client import (
    AsyncHttpPublisherClient,
)
from ray.dashboard.modules.aggregator.publisher.ray_event_publisher import (
    NoopPublisher,
    RayEventsPublisher,
)
>>>>>>> upstream/aggrToGcs2

try:
    import prometheus_client
    from prometheus_client import Counter, Gauge, Histogram
except ImportError:
    prometheus_client = None

import ray
from ray._private import ray_constants
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.consts as dashboard_consts
from ray.core.generated import (
    events_event_aggregator_service_pb2,
    events_event_aggregator_service_pb2_grpc,
    events_base_event_pb2,
    gcs_service_pb2_grpc,
)

logger = logging.getLogger(__name__)

# Environment variables for the aggregator agent
env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT"
# Max number of threads for the thread pool executor handling CPU intensive tasks
THREAD_POOL_EXECUTOR_MAX_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_THREAD_POOL_EXECUTOR_MAX_WORKERS", 1
)
# Interval to check the main thread liveness
CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS", 0.1
)
# Maximum size of the event buffer in the aggregator agent
MAX_EVENT_BUFFER_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_BUFFER_SIZE", 1000000
)
# Maximum number of events to send in a single batch to the destination
MAX_EVENT_SEND_BATCH_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_SEND_BATCH_SIZE", 10000
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
DEFAULT_EXPOSABLE_EVENT_TYPES = (
    "TASK_DEFINITION_EVENT,TASK_EXECUTION_EVENT,"
    "ACTOR_TASK_DEFINITION_EVENT,ACTOR_TASK_EXECUTION_EVENT,"
    "DRIVER_JOB_DEFINITION_EVENT,DRIVER_JOB_EXECUTION_EVENT"
)
EXPOSABLE_EVENT_TYPES = os.environ.get(
    f"{env_var_prefix}_EXPOSABLE_EVENT_TYPES", DEFAULT_EXPOSABLE_EVENT_TYPES
)
# flag to enable publishing events to the external HTTP service
PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC = ray_constants.env_bool(
    f"{env_var_prefix}_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC", True
)
# flag to enable publishing events to GCS
PUBLISH_EVENTS_TO_GCS = ray_constants.env_bool(
    f"{env_var_prefix}_PUBLISH_EVENTS_TO_GCS", False
)

# Metrics
if prometheus_client:
    metrics_prefix = "event_aggregator_agent"
    events_received = Counter(
        f"{metrics_prefix}_events_received_total",
        "Total number of events received via AddEvents gRPC.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_add_to_aggregator = Counter(
        f"{metrics_prefix}_events_buffer_add_failures_total",
        "Total number of events that failed to be added to the event buffer.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_published_to_http_svc = Counter(
        f"{metrics_prefix}_http_events_published_total",
        "Total number of events successfully published to the HTTP service.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_filtered_out_before_http_svc_publish = Counter(
        f"{metrics_prefix}_http_events_filtered_total",
        "Total number of events filtered out before publishing to the HTTP service.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_publish_to_http_svc = Counter(
        f"{metrics_prefix}_http_publish_failures_total",
        "Total number of events that failed to publish to the HTTP service after retries.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_in_http_svc_publish_queue = Counter(
        f"{metrics_prefix}_http_publish_queue_dropped_events_total",
        "Total number of events dropped because the HTTP publish queue was full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS) + ("event_type",),
        namespace="ray",
    )
    http_publish_latency_seconds = Histogram(
        f"{metrics_prefix}_http_publish_duration_seconds",
        "Duration of HTTP publish calls in seconds.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS) + ("Outcome",),
        namespace="ray",
    )
    http_failed_attempts_since_last_success = Gauge(
        f"{metrics_prefix}_http_publish_consecutive_failures",
        "Number of consecutive failed publish attempts since the last success.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    http_time_since_last_success_seconds = Gauge(
        f"{metrics_prefix}_http_time_since_last_success_seconds",
        "Seconds since the last successful publish to the HTTP service.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_published_to_gcs = Counter(
        f"{metrics_prefix}_events_published_to_gcs",
        "Total number of events successfully published to GCS.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_publish_to_gcs = Counter(
        f"{metrics_prefix}_events_failed_to_publish_to_gcs",
        "Total number of events failed to publish to GCS after retries.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_in_gcs_publish_queue = Counter(
        f"{metrics_prefix}_events_dropped_in_gcs_publish_queue",
        "Total number of events dropped due to the GCS publish queue being full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )


class AggregatorAgent(
    dashboard_utils.DashboardAgentModule,
    events_event_aggregator_service_pb2_grpc.EventAggregatorServiceServicer,
):
    """
    AggregatorAgent is a dashboard agent module that collects events sent with
    gRPC from other components, buffers them, and periodically sends them to GCS and
    an external service with HTTP POST requests for further processing or storage
    """

    def __init__(self, dashboard_agent) -> None:
        super().__init__(dashboard_agent)
        self._ip = dashboard_agent.ip
        self._pid = os.getpid()
<<<<<<< HEAD
        self._event_buffer = BoundedQueue(max_size=MAX_EVENT_BUFFER_SIZE)
        self._task_metadata_buffer = TaskMetadataBuffer()
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=GRPC_TPE_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_grpc_executor",
        )

        # Dedicated publisher event loop thread
        self._publisher_thread = None

        self._async_gcs_channel = create_gcs_channel(self.gcs_address, aio=True)
        self._async_gcs_event_stub = gcs_service_pb2_grpc.RayEventExportGcsServiceStub(
            self._async_gcs_channel
        )

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
=======
        self._event_buffer = MultiConsumerEventBuffer(
            max_size=MAX_EVENT_BUFFER_SIZE, max_batch_size=MAX_EVENT_SEND_BATCH_SIZE
        )
        self._executor = ThreadPoolExecutor(
            max_workers=THREAD_POOL_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_executor",
        )

        self._lock = asyncio.Lock()
>>>>>>> upstream/aggrToGcs2
        self._events_received_since_last_metrics_update = 0
        self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
        self._events_dropped_at_event_aggregator_since_last_metrics_update = 0
        self._events_export_addr = (
            dashboard_agent.events_export_addr or EVENTS_EXPORT_ADDR
        )

        self._exposable_event_types = {
            event_type.strip()
            for event_type in EXPOSABLE_EVENT_TYPES.split(",")
            if event_type.strip()
        }

<<<<<<< HEAD
        self._orig_sigterm_handler = signal.signal(
            signal.SIGTERM, self._sigterm_handler
        )

        self._is_cleanup = False
        self._cleanup_finished_event = threading.Event()

        # Initialize publishers
        if PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC and self._event_http_target_enabled:
=======
        self._event_processing_enabled = False
        if PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC and self._events_export_addr:
>>>>>>> upstream/aggrToGcs2
            logger.info(
                f"Publishing events to external HTTP service is enabled. events_export_addr: {self._events_export_addr}"
            )
            self._event_processing_enabled = True
            self._HttpEndpointPublisher = RayEventsPublisher(
                name="http-endpoint-publisher",
                publish_client=AsyncHttpPublisherClient(
                    endpoint=self._events_export_addr,
                    executor=self._executor,
                    events_filter_fn=self._can_expose_event,
                ),
                event_buffer=self._event_buffer,
            )
        else:
            logger.info(
                f"Event HTTP target is not enabled or publishing events to external HTTP service is disabled. Skipping sending events to external HTTP service. events_export_addr: {self._events_export_addr}"
            )
            self._HttpEndpointPublisher = NoopPublisher()

        if PUBLISH_EVENTS_TO_GCS:
            logger.info("Publishing events to GCS is enabled")
            self._gcs_publisher = GCSPublisher(
                gcs_event_stub=self._async_gcs_event_stub,
            )
        else:
            logger.info(f"Publishing events to GCS is disabled")
            self._gcs_publisher = NoopPublisher()

    async def AddEvents(self, request, context) -> None:
        """
        gRPC handler for adding events to the event aggregator. Receives events from the
        request and adds them to the event buffer.
        """
        if not self._event_processing_enabled:
            return events_event_aggregator_service_pb2.AddEventsReply()

        events_data = request.events_data
        self._task_metadata_buffer.merge(events_data.task_events_metadata)
        for event in events_data.events:
            async with self._lock:
                self._events_received_since_last_metrics_update += 1
            try:
                await self._event_buffer.add_event(event)
            except Exception as e:
                logger.error(
                    f"Failed to add event with id={event.event_id.decode()} to buffer. "
                    "Error: %s",
                    e,
                )
                async with self._lock:
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

<<<<<<< HEAD
    # TODO: This is a temporary solution to drain the event buffer to publishers when stopping the agent.
    # We need to find a better way to do this.
    async def _drain_event_buffer_to_publishers(self) -> None:
        """Drain remaining events from the internal buffer and enqueue to publishers."""
        # Drain events
        draining_event_batch = []
        while True:
            try:
                event_proto = self._event_buffer.get()
                draining_event_batch.append(event_proto)
            except queue.Empty:
                break

        # Drain task metadata
        draining_task_metadata_batch = (
            events_event_aggregator_service_pb2.TaskEventsMetadata()
        )
        metadata_batch = self._task_metadata_buffer.get()
        # merge all batches of task metadata into a single batch
        while metadata_batch and len(metadata_batch.dropped_task_attempts) > 0:
            draining_task_metadata_batch.dropped_task_attempts.extend(
                metadata_batch.dropped_task_attempts
            )
            metadata_batch = self._task_metadata_buffer.get()

        # publish events to GCS and external service
        if (
            draining_event_batch
            or len(draining_task_metadata_batch.dropped_task_attempts) > 0
        ):
            frozen_batch = tuple(draining_event_batch)
            self._gcs_publisher.publish_events(
                (frozen_batch, draining_task_metadata_batch)
            )
            if draining_event_batch:
                self._external_svc_publisher.publish_events(frozen_batch)

    async def _build_event_batches(self) -> None:
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

            # If destination queue is full, wait a bit before pulling from buffer
            if (
                not self._external_svc_publisher.can_accept_events()
                and not self._gcs_publisher.can_accept_events()
            ):
                await asyncio.sleep(MAX_BUFFER_SEND_INTERVAL_SECONDS)
                continue

            # prepare a batch of events to send
            while len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
                try:
                    event_proto = self._event_buffer.get()
                    event_batch.append(event_proto)
                except queue.Empty:
                    break

            if event_batch:
                task_events_metadata = self._task_metadata_buffer.get()
                frozen_batch = tuple(event_batch)
                # Enqueue batch to publishers
                self._external_svc_publisher.publish_events(frozen_batch)
                self._gcs_publisher.publish_events((event_batch, task_events_metadata))

                # Reset local batch
                event_batch.clear()
            else:
                # wait for a bit before pulling from buffer again
                await asyncio.sleep(MAX_BUFFER_SEND_INTERVAL_SECONDS)

    async def _publisher_event_loop(self):
        # Start destination specific publishers
        self._external_svc_publisher.start()
        self._gcs_publisher.start()
        # Launch one batching task to continuously build batches of events and enqueue to publishers
        batching_task = asyncio.create_task(
            self._build_event_batches(),
            name="event_aggregator_agent_events_batcher",
        )
        try:
            # Await batching completion (it will exit when stop event is set)
            await batching_task
        finally:
            # Drain any remaining items and shutdown publisher
            await self._drain_event_buffer_to_publishers()
            await asyncio.gather(
                self._gcs_publisher.shutdown(), self._external_svc_publisher.shutdown()
            )
            self._update_metrics()

    def _create_and_start_publisher_loop(self) -> None:
        """Creates and starts a dedicated asyncio event loop with multiple async publisher workers."""
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._publisher_event_loop())
        finally:
            loop.close()

    def _update_metrics(self) -> None:
=======
    @dashboard_utils.async_loop_forever(METRICS_UPDATE_INTERVAL_SECONDS)
    async def _update_metrics(self) -> None:
>>>>>>> upstream/aggrToGcs2
        """
        Updates the Prometheus metrics
        """
        if not prometheus_client:
            return

<<<<<<< HEAD
        external_svc_publisher_metrics = (
            self._external_svc_publisher.get_and_reset_metrics()
        )
        gcs_publisher_metrics = self._gcs_publisher.get_and_reset_metrics()

        with self._lock:
            _events_received = self._events_received_since_last_metrics_update
            _events_failed_to_add_to_aggregator = (
                self._events_failed_to_add_to_aggregator_since_last_metrics_update
            )
            _events_dropped_at_event_aggregator = (
                self._events_dropped_at_event_aggregator_since_last_metrics_update
            )

            self._events_received_since_last_metrics_update = 0
            self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0
            self._events_dropped_at_event_aggregator_since_last_metrics_update = 0

            # Publisher metrics
            _events_published_to_external_svc = external_svc_publisher_metrics.get(
                "published", 0
            )
            _events_filtered_out_to_external_svc = external_svc_publisher_metrics.get(
                "filtered_out", 0
            )
            _events_failed_to_publish_to_external_svc = (
                external_svc_publisher_metrics.get("failed", 0)
            )
            _events_dropped_in_external_publish_queue = (
                external_svc_publisher_metrics.get("queue_dropped", 0)
            )

            _events_published_to_gcs = gcs_publisher_metrics.get("published", 0)
            _events_failed_to_publish_to_gcs = gcs_publisher_metrics.get("failed", 0)
            _events_dropped_in_gcs_publish_queue = gcs_publisher_metrics.get(
                "queue_dropped", 0
            )

        labels = {
=======
        common_labels = {
>>>>>>> upstream/aggrToGcs2
            "ip": self._ip,
            "pid": self._pid,
            "Version": ray.__version__,
            "Component": "event_aggregator_agent",
            "SessionName": self.session_name,
        }

        http_endpoint_publisher_metrics = await (
            self._HttpEndpointPublisher.get_and_reset_metrics()
        )

        async with self._lock:
            # Aggregator agent metrics
            _events_received = self._events_received_since_last_metrics_update
            _events_failed_to_add_to_aggregator = (
                self._events_failed_to_add_to_aggregator_since_last_metrics_update
            )

            self._events_received_since_last_metrics_update = 0
            self._events_failed_to_add_to_aggregator_since_last_metrics_update = 0

            # HTTP service publisher metrics
            _events_published_to_http_svc = http_endpoint_publisher_metrics.get(
                "published", 0
            )
            _events_filtered_out_before_http_svc_publish = (
                http_endpoint_publisher_metrics.get("filtered_out", 0)
            )
            _events_failed_to_publish_to_http_svc = http_endpoint_publisher_metrics.get(
                "failed", 0
            )
            _events_dropped_in_http_publish_queue_by_type = (
                http_endpoint_publisher_metrics.get("dropped_events", {})
            )
            _http_publish_latency_success_samples = http_endpoint_publisher_metrics.get(
                "success_latency_seconds", []
            )
            _http_publish_latency_failure_samples = http_endpoint_publisher_metrics.get(
                "failure_latency_seconds", []
            )
            _failed_attempts_since_last_success = http_endpoint_publisher_metrics.get(
                "failed_attempts_since_last_success", 0
            )
            _time_since_last_success_seconds = http_endpoint_publisher_metrics.get(
                "time_since_last_success_seconds", None
            )

        events_received.labels(**common_labels).inc(_events_received)
        events_failed_to_add_to_aggregator.labels(**common_labels).inc(
            _events_failed_to_add_to_aggregator
        )
        events_published_to_http_svc.labels(**common_labels).inc(
            _events_published_to_http_svc
        )
        events_filtered_out_before_http_svc_publish.labels(**common_labels).inc(
            _events_filtered_out_before_http_svc_publish
        )
        events_failed_to_publish_to_http_svc.labels(**common_labels).inc(
            _events_failed_to_publish_to_http_svc
        )
        for (
            _event_type,
            _dropped_count,
        ) in _events_dropped_in_http_publish_queue_by_type.items():
            events_dropped_in_http_svc_publish_queue.labels(
                **common_labels, event_type=str(_event_type)
            ).inc(_dropped_count)

        for _sample in _http_publish_latency_success_samples:
            http_publish_latency_seconds.labels(
                **common_labels, Outcome="success"
            ).observe(float(_sample))
        for _sample in _http_publish_latency_failure_samples:
            http_publish_latency_seconds.labels(
                **common_labels, Outcome="failure"
            ).observe(float(_sample))
        http_failed_attempts_since_last_success.labels(**common_labels).set(
            int(_failed_attempts_since_last_success)
        )
<<<<<<< HEAD
        events_dropped_in_external_svc_publish_queue.labels(**labels).inc(
            _events_dropped_in_external_publish_queue
        )
        events_published_to_gcs.labels(**labels).inc(_events_published_to_gcs)
        events_failed_to_publish_to_gcs.labels(**labels).inc(
            _events_failed_to_publish_to_gcs
        )
        events_dropped_in_gcs_publish_queue.labels(**labels).inc(
            _events_dropped_in_gcs_publish_queue
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

        # Wait for publisher thread to finish
        if self._publisher_thread:
            self._publisher_thread.join()

        self._cleanup_finished_event.set()

    def _sigterm_handler(self, signum: int, frame) -> None:
        self._stop_event.set()
        self._cleanup()
        self._orig_sigterm_handler(signum, frame)
=======
        if _time_since_last_success_seconds is not None:
            http_time_since_last_success_seconds.labels(**common_labels).set(
                float(_time_since_last_success_seconds)
            )
>>>>>>> upstream/aggrToGcs2

    async def run(self, server) -> None:
        if server:
            events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(
                self, server
            )

        await asyncio.gather(
            self._HttpEndpointPublisher.run_forever(),
            self._update_metrics(),
        )

    @staticmethod
    def is_minimal_module() -> bool:
        return False
