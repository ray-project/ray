import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
import logging
from ray._private.gcs_utils import create_gcs_channel
from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)
from ray.dashboard.modules.aggregator.publisher.async_publisher_client import (
    AsyncHttpPublisherClient,
    AsyncGCSPublisherClient,
)
from ray.dashboard.modules.aggregator.publisher.ray_event_publisher import (
    NoopPublisher,
    RayEventsPublisher,
)
from ray.dashboard.modules.aggregator.task_metadata_buffer import TaskMetadataBuffer

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
    # External HTTP service publisher metrics
    events_published_to_http_svc = Counter(
        f"{metrics_prefix}_http_publisher_published_events_total",
        "Total number of events successfully published to the HTTP service.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_filtered_out_before_http_svc_publish = Counter(
        f"{metrics_prefix}_http_publisher_filtered_events_total",
        "Total number of events filtered out before publishing to the HTTP service.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_publish_to_http_svc = Counter(
        f"{metrics_prefix}_http_publisher_failures_total",
        "Total number of events that failed to publish to the HTTP service after retries.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_in_http_svc_publish_queue = Counter(
        f"{metrics_prefix}_http_publisher_queue_dropped_events_total",
        "Total number of events dropped because the HTTP publish queue was full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS) + ("event_type",),
        namespace="ray",
    )
    http_publish_latency_seconds = Histogram(
        f"{metrics_prefix}_http_publisher_publish_duration_seconds",
        "Duration of HTTP publish calls in seconds.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS) + ("Outcome",),
        namespace="ray",
    )
    http_failed_attempts_since_last_success = Gauge(
        f"{metrics_prefix}_http_publisher_consecutive_failures_since_last_success",
        "Number of consecutive failed publish attempts since the last success.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    http_time_since_last_success_seconds = Gauge(
        f"{metrics_prefix}_http_publisher_time_since_last_success_seconds",
        "Seconds since the last successful publish to the HTTP service.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    # GCS publisher metrics
    events_published_to_gcs = Counter(
        f"{metrics_prefix}_gcs_publisher_events_published_total",
        "Total number of events successfully published to GCS.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_failed_to_publish_to_gcs = Counter(
        f"{metrics_prefix}_gcs_publisher_publish_failures_total",
        "Total number of events failed to publish to GCS after retries.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_in_gcs_publish_queue = Counter(
        f"{metrics_prefix}_gcs_publisher_queue_dropped_events_total",
        "Total number of events dropped due to the GCS publish queue being full.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS) + ("event_type",),
        namespace="ray",
    )
    gcs_publish_latency_seconds = Histogram(
        f"{metrics_prefix}_gcs_publisher_publish_duration_seconds",
        "Duration of GCS publish calls in seconds.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS) + ("Outcome",),
        namespace="ray",
    )
    gcs_failed_attempts_since_last_success = Gauge(
        f"{metrics_prefix}_gcs_publisher_consecutive_failures_since_last_success",
        "Number of consecutive failed publish attempts since the last success.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    gcs_time_since_last_success_seconds = Gauge(
        f"{metrics_prefix}_gcs_publisher_time_since_last_success_seconds",
        "Seconds since the last successful publish to GCS.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    # Task metadata buffer metrics
    events_dropped_in_task_metadata_buffer = Counter(
        f"{metrics_prefix}_task_metadata_buffer_dropped_events_total",
        "Total number of events dropped due to the task metadata buffer being full.",
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
        self._event_buffer = MultiConsumerEventBuffer(
            max_size=MAX_EVENT_BUFFER_SIZE, max_batch_size=MAX_EVENT_SEND_BATCH_SIZE
        )
        self._executor = ThreadPoolExecutor(
            max_workers=THREAD_POOL_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_executor",
        )

        # Task metadata buffer accumulates dropped task attempts for GCS publishing
        self._task_metadata_buffer = TaskMetadataBuffer()

        self._lock = asyncio.Lock()
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

        self._event_processing_enabled = False
        if PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC and self._events_export_addr:
            logger.info(
                f"Publishing events to external HTTP service is enabled. events_export_addr: {self._events_export_addr}"
            )
            self._event_processing_enabled = True
            self._http_endpoint_publisher = RayEventsPublisher(
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
            self._http_endpoint_publisher = NoopPublisher()

        if PUBLISH_EVENTS_TO_GCS:
            logger.info("Publishing events to GCS is enabled")
            self._event_processing_enabled = True
            self._async_gcs_channel = create_gcs_channel(self.gcs_address, aio=True)
            self._async_gcs_event_stub = (
                gcs_service_pb2_grpc.RayEventExportGcsServiceStub(
                    self._async_gcs_channel
                )
            )
            self._gcs_publisher = RayEventsPublisher(
                name="gcs-publisher",
                publish_client=AsyncGCSPublisherClient(
                    gcs_stub=self._async_gcs_event_stub
                ),
                event_buffer=self._event_buffer,
                task_metadata_buffer=self._task_metadata_buffer,
            )
        else:
            logger.info("Publishing events to GCS is disabled")
            self._gcs_publisher = NoopPublisher()

    async def AddEvents(self, request, context) -> None:
        """
        gRPC handler for adding events to the event aggregator. Receives events from the
        request and adds them to the event buffer.
        """
        if not self._event_processing_enabled:
            return events_event_aggregator_service_pb2.AddEventsReply()

        events_data = request.events_data
        await self._task_metadata_buffer.merge(events_data.task_events_metadata)
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

    @dashboard_utils.async_loop_forever(METRICS_UPDATE_INTERVAL_SECONDS)
    async def _update_metrics(self) -> None:
        """
        Updates the Prometheus metrics
        """
        if not prometheus_client:
            return

        common_labels = {
            "ip": self._ip,
            "pid": self._pid,
            "Version": ray.__version__,
            "Component": "event_aggregator_agent",
            "SessionName": self.session_name,
        }

        http_endpoint_publisher_metrics = await (
            self._http_endpoint_publisher.get_and_reset_metrics()
        )
        gcs_publisher_metrics = await self._gcs_publisher.get_and_reset_metrics()
        task_metadata_buffer_metrics = (
            await self._task_metadata_buffer.get_and_reset_metrics()
        )

        # Aggregator agent metrics
        async with self._lock:
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

        # Task metadata buffer metrics
        _task_metadata_buffer_dropped_metadata_count = task_metadata_buffer_metrics.get(
            "dropped_metadata_count", 0
        )

        # GCS publisher metrics
        _events_published_to_gcs = gcs_publisher_metrics.get("published", 0)
        _events_failed_to_publish_to_gcs = gcs_publisher_metrics.get("failed", 0)
        _events_dropped_in_gcs_publish_queue_by_type = gcs_publisher_metrics.get(
            "dropped_events", {}
        )
        _gcs_publish_latency_success_samples = gcs_publisher_metrics.get(
            "success_latency_seconds", []
        )
        _gcs_publish_latency_failure_samples = gcs_publisher_metrics.get(
            "failure_latency_seconds", []
        )
        _gcs_failed_attempts_since_last_success = gcs_publisher_metrics.get(
            "failed_attempts_since_last_success", 0
        )
        _gcs_time_since_last_success_seconds = gcs_publisher_metrics.get(
            "time_since_last_success_seconds", None
        )

        # Emit metrics to Prometheus
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
        if _time_since_last_success_seconds is not None:
            http_time_since_last_success_seconds.labels(**common_labels).set(
                float(_time_since_last_success_seconds)
            )

        events_published_to_gcs.labels(**common_labels).inc(_events_published_to_gcs)
        events_failed_to_publish_to_gcs.labels(**common_labels).inc(
            _events_failed_to_publish_to_gcs
        )

        for (
            _event_type,
            _dropped_count,
        ) in _events_dropped_in_gcs_publish_queue_by_type.items():
            events_dropped_in_gcs_publish_queue.labels(
                **common_labels, event_type=str(_event_type)
            ).inc(_dropped_count)

        for _sample in _gcs_publish_latency_success_samples:
            gcs_publish_latency_seconds.labels(
                **common_labels, Outcome="success"
            ).observe(float(_sample))
        for _sample in _gcs_publish_latency_failure_samples:
            gcs_publish_latency_seconds.labels(
                **common_labels, Outcome="failure"
            ).observe(float(_sample))
        gcs_failed_attempts_since_last_success.labels(**common_labels).set(
            int(_gcs_failed_attempts_since_last_success)
        )
        if _gcs_time_since_last_success_seconds is not None:
            gcs_time_since_last_success_seconds.labels(**common_labels).set(
                float(_gcs_time_since_last_success_seconds)
            )

        # Task metadata buffer metrics
        events_dropped_in_task_metadata_buffer.labels(**common_labels).inc(
            _task_metadata_buffer_dropped_metadata_count
        )

    async def run(self, server) -> None:
        if server:
            events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(
                self, server
            )

        await asyncio.gather(
            self._http_endpoint_publisher.run_forever(),
            self._gcs_publisher.run_forever(),
            self._update_metrics(),
        )

        self._executor.shutdown()
        self._async_gcs_channel.close()

    @staticmethod
    def is_minimal_module() -> bool:
        return False
