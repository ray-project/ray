import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import ray
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)
from ray.core.generated import (
    events_base_event_pb2,
    events_event_aggregator_service_pb2,
    events_event_aggregator_service_pb2_grpc,
)
from ray.dashboard.modules.aggregator.constants import AGGREGATOR_AGENT_METRIC_PREFIX
from ray.dashboard.modules.aggregator.multi_consumer_event_buffer import (
    MultiConsumerEventBuffer,
)
from ray.dashboard.modules.aggregator.publisher.async_publisher_client import (
    AsyncHttpPublisherClient,
)
from ray.dashboard.modules.aggregator.publisher.ray_event_publisher import (
    NoopPublisher,
    RayEventPublisher,
)

logger = logging.getLogger(__name__)

# Max number of threads for the thread pool executor handling CPU intensive tasks
THREAD_POOL_EXECUTOR_MAX_WORKERS = ray_constants.env_integer(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_THREAD_POOL_EXECUTOR_MAX_WORKERS", 1
)
# Interval to check the main thread liveness
CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS = ray_constants.env_float(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS", 0.1
)
# Maximum size of the event buffer in the aggregator agent
MAX_EVENT_BUFFER_SIZE = ray_constants.env_integer(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE", 1000000
)
# Maximum number of events to send in a single batch to the destination
MAX_EVENT_SEND_BATCH_SIZE = ray_constants.env_integer(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_SEND_BATCH_SIZE", 10000
)
# Address of the external service to send events with format of "http://<ip>:<port>"
EVENTS_EXPORT_ADDR = os.environ.get(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR", ""
)
# Event filtering configurations
# Comma-separated list of event types that are allowed to be exposed to external services
# Valid values: TASK_DEFINITION_EVENT, TASK_EXECUTION_EVENT, ACTOR_TASK_DEFINITION_EVENT, ACTOR_TASK_EXECUTION_EVENT
# The list of all supported event types can be found in src/ray/protobuf/public/events_base_event.proto (EventType enum)
# By default TASK_PROFILE_EVENT is not exposed to external services
DEFAULT_EXPOSABLE_EVENT_TYPES = (
    "TASK_DEFINITION_EVENT,TASK_LIFECYCLE_EVENT,ACTOR_TASK_DEFINITION_EVENT,"
    "DRIVER_JOB_DEFINITION_EVENT,DRIVER_JOB_LIFECYCLE_EVENT,"
    "ACTOR_DEFINITION_EVENT,ACTOR_LIFECYCLE_EVENT,"
    "NODE_DEFINITION_EVENT,NODE_LIFECYCLE_EVENT,"
)
EXPOSABLE_EVENT_TYPES = os.environ.get(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES",
    DEFAULT_EXPOSABLE_EVENT_TYPES,
)
# flag to enable publishing events to the external HTTP service
PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE = ray_constants.env_bool(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE", True
)
# flag to control whether preserve the proto field name when converting the events to
# JSON. If True, the proto field name will be preserved. If False, the proto field name
# will be converted to camel case.
PRESERVE_PROTO_FIELD_NAME = ray_constants.env_bool(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_PRESERVE_PROTO_FIELD_NAME", False
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

        # common prometheus labels for aggregator-owned metrics
        self._common_tags = {
            "ip": self._ip,
            "pid": str(self._pid),
            "Version": ray.__version__,
            "Component": "aggregator_agent",
            "SessionName": self.session_name,
        }

        self._event_buffer = MultiConsumerEventBuffer(
            max_size=MAX_EVENT_BUFFER_SIZE,
            max_batch_size=MAX_EVENT_SEND_BATCH_SIZE,
            common_metric_tags=self._common_tags,
        )
        self._executor = ThreadPoolExecutor(
            max_workers=THREAD_POOL_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="aggregator_agent_executor",
        )

        self._events_export_addr = (
            dashboard_agent.events_export_addr or EVENTS_EXPORT_ADDR
        )

        self._exposable_event_types = {
            event_type.strip()
            for event_type in EXPOSABLE_EVENT_TYPES.split(",")
            if event_type.strip()
        }

        self._event_processing_enabled = False
        if PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE and self._events_export_addr:
            logger.info(
                f"Publishing events to external HTTP service is enabled. events_export_addr: {self._events_export_addr}"
            )
            self._event_processing_enabled = True
            self._http_endpoint_publisher = RayEventPublisher(
                name="http_publisher",
                publish_client=AsyncHttpPublisherClient(
                    endpoint=self._events_export_addr,
                    executor=self._executor,
                    events_filter_fn=self._can_expose_event,
                    preserve_proto_field_name=PRESERVE_PROTO_FIELD_NAME,
                ),
                event_buffer=self._event_buffer,
                common_metric_tags=self._common_tags,
            )
        else:
            logger.info(
                f"Event HTTP target is not enabled or publishing events to external HTTP service is disabled. Skipping sending events to external HTTP service. events_export_addr: {self._events_export_addr}"
            )
            self._http_endpoint_publisher = NoopPublisher()

        # Metrics
        self._open_telemetry_metric_recorder = OpenTelemetryMetricRecorder()

        # Register counter metrics
        self._events_received_metric_name = (
            f"{AGGREGATOR_AGENT_METRIC_PREFIX}_events_received_total"
        )
        self._open_telemetry_metric_recorder.register_counter_metric(
            self._events_received_metric_name,
            "Total number of events received via AddEvents gRPC.",
        )

        self._events_failed_to_add_metric_name = (
            f"{AGGREGATOR_AGENT_METRIC_PREFIX}_events_buffer_add_failures_total"
        )
        self._open_telemetry_metric_recorder.register_counter_metric(
            self._events_failed_to_add_metric_name,
            "Total number of events that failed to be added to the event buffer.",
        )

    async def AddEvents(self, request, context) -> None:
        """
        gRPC handler for adding events to the event aggregator. Receives events from the
        request and adds them to the event buffer.
        """
        if not self._event_processing_enabled:
            return events_event_aggregator_service_pb2.AddEventsReply()

        # TODO(myan) #54515: Considering adding a mechanism to also send out the events
        # metadata (e.g. dropped task attempts) to help with event processing at the
        # downstream
        events_data = request.events_data
        for event in events_data.events:
            self._open_telemetry_metric_recorder.set_metric_value(
                self._events_received_metric_name, self._common_tags, 1
            )
            try:
                await self._event_buffer.add_event(event)
            except Exception as e:
                logger.error(
                    f"Failed to add event with id={event.event_id.decode()} to buffer. "
                    "Error: %s",
                    e,
                )
                self._open_telemetry_metric_recorder.set_metric_value(
                    self._events_failed_to_add_metric_name, self._common_tags, 1
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

    async def run(self, server) -> None:
        if server:
            events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(
                self, server
            )

        try:
            await asyncio.gather(
                self._http_endpoint_publisher.run_forever(),
            )
        finally:
            self._executor.shutdown()

    @staticmethod
    def is_minimal_module() -> bool:
        return False
