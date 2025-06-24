import asyncio
import signal
import time
import requests
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

logger = logging.getLogger(__name__)

# Environment variables for the aggregator agent
env_var_prefix = "RAY_DASHBOARD_AGGREGATOR_AGENT"
GRPC_TPE_MAX_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_GRPC_TPE_MAX_WORKERS", 10
)
PUBLISH_EVENT_WORKERS = ray_constants.env_integer(
    f"{env_var_prefix}_PUBLISH_EVENT_WORKERS", 1
)
CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS", 0.1
)
MAX_EVENT_BUFFER_SIZE = ray_constants.env_integer(
    f"{env_var_prefix}_MAX_EVENT_BUFFER_SIZE", 1000000
)
MAX_BUFFER_SEND_INTERVAL_SECONDS = ray_constants.env_float(
    f"{env_var_prefix}_MAX_BUFFER_SEND_INTERVAL_SECONDS", 0.1
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
USE_PROTO = ray_constants.env_bool(f"{env_var_prefix}_USE_PROTO", False)

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
        self._stop_event = threading.Event()
        self._publisher_threads = []
        self._events_received_since_last_metrics_update = 0
        self._events_dropped_at_core_worker_since_last_metrics_update = 0
        self._events_dropped_at_event_buffer_since_last_metrics_update = 0
        self._events_published_since_last_metrics_update = 0

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
        # The status code is defined in `src/ray/common/status.h`
        status_code = 0
        status_messages = []
        for event in events_data.events:
            try:
                self._event_buffer.put_nowait(event)
                with self._lock:
                    self._events_received_since_last_metrics_update += 1
            except queue.Full:
                old_event = self._event_buffer.get_nowait()
                self._event_buffer.put_nowait(event)
                with self._lock:
                    self._events_received_since_last_metrics_update += 1
                    self._events_dropped_at_event_buffer_since_last_metrics_update += 1
                if status_code == 0:
                    status_code = 5
                status_messages.append(
                    f"event {old_event.event_id.decode()} dropped because event buffer full"
                )
            except Exception as e:
                logger.error(
                    "Failed to add event to buffer. Error: %s",
                    e,
                )
                with self._lock:
                    self._events_dropped_at_event_buffer_since_last_metrics_update += 1
                status_code = 9
                status_messages.append(
                    f"event {event.event_id.decode()} failed to add to buffer with error {e}"
                )

        status_message = "all events received"
        if status_messages:
            truncate_num = 5
            status_message = ", ".join(status_messages[:truncate_num])
            if len(status_messages) > truncate_num:
                status_message += (
                    f", and {len(status_messages) - truncate_num} more events dropped"
                )
        status = events_event_aggregator_service_pb2.AddEventStatus(
            status_code=status_code, status_message=status_message
        )
        return events_event_aggregator_service_pb2.AddEventReply(status=status)

    def _send_events_to_external_service(self, event_batch):
        if not event_batch:
            return
        try:
            if USE_PROTO:
                event_batch_proto = events_event_aggregator_service_pb2.RayEventData(
                    events=event_batch
                )
                response = requests.post(
                    f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}",
                    data=event_batch_proto.SerializeToString(),
                    headers={"Content-Type": "application/x-protobuf"},
                )
            else:
                response = requests.post(
                    f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}", json=event_batch
                )
            response.raise_for_status()
            with self._lock:
                self._events_published_since_last_metrics_update += len(event_batch)
            event_batch.clear()
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)
            # Add this line just for benchmark testing purposes
            event_batch.clear()

    def _publish_events(self):
        event_batch = []

        while True:
            while len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
                try:
                    event_proto = self._event_buffer.get(block=False)
                    if USE_PROTO:
                        event_batch.append(event_proto)
                    else:
                        event_batch.append(json.loads(MessageToJson((event_proto))))
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

    def _update_metrics(self):
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

    def _check_main_thread_liveness(self):
        while True:
            if not threading.main_thread().is_alive():
                self._stop_event.set()
            if self._stop_event.is_set():
                self._cleanup()
                break
            time.sleep(CHECK_MAIN_THREAD_LIVENESS_INTERVAL_SECONDS)

    def _cleanup(self):
        # Send any remaining events in the buffer
        event_batch = []
        while True:
            try:
                event_proto = self._event_buffer.get(block=False)
                if USE_PROTO:
                    event_batch.append(event_proto)
                else:
                    event_batch.append(json.loads(MessageToJson((event_proto))))
            except:  # noqa: E722
                break

        self._send_events_to_external_service(event_batch)

        for thread in self._publisher_threads:
            thread.join()

        # Update metrics immediately
        self._update_metrics()

    def _sigterm_handler(self, signum, frame):
        self._stop_event.set()
        self._cleanup()
        self._orig_sigterm_handler(signum, frame)

    async def run(self, server):
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
    def is_minimal_module():
        return False
