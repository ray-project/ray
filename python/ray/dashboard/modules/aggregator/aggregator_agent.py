import atexit
import signal
import requests
import asyncio
import os
import json
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import logging

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
)
from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)

# Environment variables for the aggregator agent
env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT"
GRPC_TPE_MAX_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_GRPC_TPE_MAX_WORKERS", 10
)
MAX_EVENT_BUFFER_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_BUFFER_SIZE", 1000000
)
BUFFER_SEND_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_BUFFER_SEND_INTERVAL_SECONDS", 0.1
)
MAX_EVENT_SEND_BATCH_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_SEND_BATCH_SIZE", 10000
)
EVENT_SEND_ADDR = os.environ.get(
    f"{env_var_prefix}_EVENT_SEND_ADDR", "http://127.0.0.1"
)
EVENT_SEND_PORT = ray_constants.env_integer(f"{env_var_prefix}_EVENT_SEND_PORT", 12345)
METRICS_UPDATE_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_METRICS_UPDATE_INTERVAL_SECONDS", 0.1
)

# Metrics
if prometheus_client:
    metrics_prefix = "event_aggregator_agent"
    events_received = Counter(
        f"{metrics_prefix}_events_received",
        "Total number of events received.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_at_core_worker = Counter(
        f"{metrics_prefix}_events_dropped_at_core_worker",
        "Total number of events dropped at core worker.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_dropped_at_event_buffer = Counter(
        f"{metrics_prefix}_events_dropped_at_event_buffer",
        "Total number of events dropped at receiver.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_published = Counter(
        f"{metrics_prefix}_events_published",
        "Total number of events published.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )


class AggregatorAgent(
    dashboard_utils.DashboardAgentModule,
    events_event_aggregator_service_pb2_grpc.EventAggregatorServiceServicer,
):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._ip = dashboard_agent.ip
        self._pid = os.getpid()
        self._event_buffer = queue.Queue(maxsize=MAX_EVENT_BUFFER_SIZE)
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=GRPC_TPE_MAX_WORKERS,
            thread_name_prefix="event_aggregator_agent_grpc_executor",
        )

        self._lock = threading.Lock()
        self._events_received_since_last_metrics_update = 0
        self._events_dropped_at_core_worker_since_last_metrics_update = 0
        self._events_dropped_at_event_buffer_since_last_metrics_update = 0
        self._events_published_since_last_metrics_update = 0
        self._event_batch = []  # list of event json objects

        atexit.register(self._cleanup)
        self._orig_sigterm_handler = signal.signal(
            signal.SIGTERM, self._sigterm_handler
        )

    async def AddEvents(self, request, context):
        loop = get_or_create_event_loop()

        return await loop.run_in_executor(
            self._grpc_executor, self._receive_events, request
        )

    def _receive_events(self, request):
        events_data = request.events_data
        with self._lock:
            self._events_dropped_at_core_worker_since_last_metrics_update += len(
                events_data.task_events_metadata.dropped_task_attempts
            )
        for event in events_data.events:
            try:
                self._event_buffer.put(event, block=False)
                with self._lock:
                    self._events_received_since_last_metrics_update += 1
            except queue.Full:
                with self._lock:
                    self._events_dropped_at_event_buffer_since_last_metrics_update += 1
                # The status code is defined in `src/ray/common/status.h`
                status_code_io_error = 5
                status = events_event_aggregator_service_pb2.AddEventStatus(
                    status_code=status_code_io_error,
                    status_message="event buffer full, drop event",
                )
                return events_event_aggregator_service_pb2.AddEventReply(status=status)
            except Exception as e:
                logger.error(
                    "Failed to add event to buffer. Error: %s",
                    e,
                )
                with self._lock:
                    self._events_dropped_at_event_buffer_since_last_metrics_update += 1
                # The status code is defined in `src/ray/common/status.h`
                status_code_unknown_error = 9
                status = events_event_aggregator_service_pb2.AddEventStatus(
                    status_code=status_code_unknown_error,
                    status_message=f"failed to add event to buffer, error: {e}",
                )
                return events_event_aggregator_service_pb2.AddEventReply(status=status)

        # The status code is defined in `src/ray/common/status.h`
        status_code_ok = 0
        status = events_event_aggregator_service_pb2.AddEventStatus(
            status_code=status_code_ok, status_message="received"
        )
        return events_event_aggregator_service_pb2.AddEventReply(status=status)

    @async_loop_forever(BUFFER_SEND_INTERVAL_SECONDS)
    async def _publish_events(self):
        # self._event_batch is only used in this thread, so we don't need to
        # acquire the lock when accessing it.

        while len(self._event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
            try:
                event_proto = self._event_buffer.get(block=False)
                self._event_batch.append(json.loads(MessageToJson((event_proto))))
            except queue.Empty:
                break

        if self._event_batch:
            # query external service to send the events
            try:
                async with self._dashboard_agent.http_session.post(
                    f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}", json=self._event_batch
                ) as response:
                    response.raise_for_status()
                self._event_batch.clear()
                with self._lock:
                    self._events_published_since_last_metrics_update += len(
                        self._event_batch
                    )
            except Exception as e:
                logger.error(
                    "Failed to send events to external service. Error: %s",
                    e,
                )
                # If failed, self._event_batch will be reused in the next iteration.
                # Retry sending with other events in the next iteration.

    @async_loop_forever(METRICS_UPDATE_INTERVAL_SECONDS)
    async def _update_metrics(self):
        if not prometheus_client:
            return

        with self._lock:
            _events_received = self._events_received_since_last_metrics_update
            _events_dropped_at_core_worker = (
                self._events_dropped_at_core_worker_since_last_metrics_update
            )
            _events_dropped_at_event_buffer = (
                self._events_dropped_at_event_buffer_since_last_metrics_update
            )
            _events_published = self._events_published_since_last_metrics_update

            self._events_received_since_last_metrics_update = 0
            self._events_dropped_at_core_worker_since_last_metrics_update = 0
            self._events_dropped_at_event_buffer_since_last_metrics_update = 0
            self._events_published_since_last_metrics_update = 0

        labels = {
            "ip": self._ip,
            "pid": self._pid,
            "Version": ray.__version__,
            "Component": "event_aggregator_agent",
            "SessionName": self.session_name,
        }
        events_received.labels(**labels).inc(_events_received)
        events_dropped_at_core_worker.labels(**labels).inc(
            _events_dropped_at_core_worker
        )
        events_dropped_at_event_buffer.labels(**labels).inc(
            _events_dropped_at_event_buffer
        )
        events_published.labels(**labels).inc(_events_published)

    def _cleanup(self):
        event_batch = self._event_batch.copy()
        while True:
            try:
                event_proto = self._event_buffer.get(block=False)
                event_batch.append(json.loads(MessageToJson((event_proto))))
            except:  # noqa: E722
                break
        if event_batch:
            requests.post(f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}", json=event_batch)

    def _sigterm_handler(self, signum, frame):
        self._cleanup()
        self._orig_sigterm_handler(signum, frame)

    async def run(self, server):
        if server:
            events_event_aggregator_service_pb2_grpc.add_EventAggregatorServiceServicer_to_server(
                self, server
            )

        await asyncio.gather(
            self._publish_events(),
            self._update_metrics(),
        )

    @staticmethod
    def is_minimal_module():
        return False
