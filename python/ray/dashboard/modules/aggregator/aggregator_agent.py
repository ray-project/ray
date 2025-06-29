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
# Address of the external service to send events
EVENT_SEND_ADDR = os.environ.get(
    f"{env_var_prefix}_EVENT_SEND_ADDR", "http://127.0.0.1"
)
# Port of the external service to send events
EVENT_SEND_PORT = ray_constants.env_integer(f"{env_var_prefix}_EVENT_SEND_PORT", 12345)
# Interval to update metrics
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
    events_dropped_at_event_aggregator = Counter(
        f"{metrics_prefix}_events_dropped_at_event_aggregator",
        "Total number of events dropped at the event aggregator.",
        tuple(dashboard_consts.COMPONENT_METRICS_TAG_KEYS),
        namespace="ray",
    )
    events_published = Counter(
        f"{metrics_prefix}_events_published",
        "Total number of events successfully published to the external server.",
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

    def __init__(self, dashboard_agent):
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
        self._events_dropped_at_core_worker_since_last_metrics_update = 0
        self._events_dropped_at_event_aggregator_since_last_metrics_update = 0
        self._events_published_since_last_metrics_update = 0

        self._orig_sigterm_handler = signal.signal(
            signal.SIGTERM, self._sigterm_handler
        )

        self._is_cleanup = False
        self._cleanup_finished_event = threading.Event()

    async def AddEvents(self, request, context):
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
        with self._lock:
            self._events_dropped_at_core_worker_since_last_metrics_update += len(
                events_data.task_events_metadata.dropped_task_attempts
            )
        # The status code is defined in `src/ray/common/status.h`
        status_code = 0
        error_messages = []
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
                    self._events_dropped_at_event_aggregator_since_last_metrics_update += (
                        1
                    )
                if status_code == 0:
                    status_code = 5
                error_messages.append(
                    f"event {old_event.event_id.decode()} dropped because event buffer full"
                )
            except Exception as e:
                logger.error(
                    "Failed to add event to buffer. Error: %s",
                    e,
                )
                with self._lock:
                    self._events_dropped_at_event_aggregator_since_last_metrics_update += (
                        1
                    )
                status_code = 9
                error_messages.append(
                    f"event {event.event_id.decode()} failed to add to buffer with error {e}"
                )

        status_message = "all events received"
        if error_messages:
            truncate_num = 5
            status_message = ", ".join(error_messages[:truncate_num])
            if len(error_messages) > truncate_num:
                status_message += (
                    f", and {len(error_messages) - truncate_num} more events dropped"
                )
        status = events_event_aggregator_service_pb2.AddEventStatus(
            status_code=status_code, status_message=status_message
        )
        return events_event_aggregator_service_pb2.AddEventReply(status=status)

    def _send_events_to_external_service(self, event_batch):
        """
        Sends a batch of events to the external service via HTTP POST request
        """
        if not event_batch:
            return
        try:
            response = self._http_session.post(
                f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}", json=event_batch
            )
            response.raise_for_status()
            with self._lock:
                self._events_published_since_last_metrics_update += len(event_batch)
            event_batch.clear()
        except Exception as e:
            logger.error("Failed to send events to external service. Error: %s", e)

    def _publish_events(self):
        """
        Continuously publishes events from the event buffer to the external service
        """
        event_batch = []

        while True:
            while len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE:
                try:
                    event_proto = self._event_buffer.get(block=False)
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
        """
        Updates the Prometheus metrics
        """
        if not prometheus_client:
            return

        with self._lock:
            _events_received = self._events_received_since_last_metrics_update
            _events_dropped_at_core_worker = (
                self._events_dropped_at_core_worker_since_last_metrics_update
            )
            _events_dropped_at_event_aggregator = (
                self._events_dropped_at_event_aggregator_since_last_metrics_update
            )
            _events_published = self._events_published_since_last_metrics_update

            self._events_received_since_last_metrics_update = 0
            self._events_dropped_at_core_worker_since_last_metrics_update = 0
            self._events_dropped_at_event_aggregator_since_last_metrics_update = 0
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
        events_dropped_at_event_aggregator.labels(**labels).inc(
            _events_dropped_at_event_aggregator
        )
        events_published.labels(**labels).inc(_events_published)

    def _check_main_thread_liveness(self):
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

    def _cleanup(self):
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
                event_batch.append(json.loads(MessageToJson((event_proto))))
            except:  # noqa: E722
                break

        self._send_events_to_external_service(event_batch)

        for thread in self._publisher_threads:
            thread.join()

        # Update metrics immediately
        self._update_metrics()

        self._cleanup_finished_event.set()

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
