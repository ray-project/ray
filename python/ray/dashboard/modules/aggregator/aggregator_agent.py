import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
import time
import requests
from ray._common.utils import get_or_create_event_loop
from ray._private import ray_constants
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import event_pb2_grpc, gcs_service_pb2
from google.protobuf.json_format import MessageToJson

# TODO: Productionize the aggregator agent.
logger = logging.getLogger(__name__)

RAY_DASHBOARD_AGGREGATOR_AGENT_GRPC_TPE_MAX_WORKERS = ray_constants.env_integer(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_TPE_MAX_WORKERS", 10
)
RAY_DASHBOARD_AGGREGATOR_AGENT_LOOP_TPE_MAX_WORKERS = ray_constants.env_integer(
    "RAY_DASHBOARD_AGGREGATOR_AGENT_LOOP_TPE_MAX_WORKERS", 1
)
MAX_EVENT_BUFFER_SIZE = (
    1000000  # TODO: make this configurable as internal configuration
)
MAX_EVENT_BUFFER_SEND_INTERVAL_SECONDS = (
    0.1  # TODO: make this configurable as internal configuration
)
MAX_EVENT_SEND_BATCH_SIZE = (
    100000  # TODO: make this configurable as internal configuration
)

EVENT_SEND_ADDR = "http://127.0.0.1"  # TODO: make this configurable as public API
EVENT_SEND_PORT = "12345"  # TODO: make this configurable as public API


class AggregatorAgent(
    dashboard_utils.DashboardAgentModule, event_pb2_grpc.AggregatorServiceServicer
):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._event_buffer = queue.Queue()
        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_AGGREGATOR_AGENT_GRPC_TPE_MAX_WORKERS,
            thread_name_prefix="aggregator_agent_executor",
        )
        self._loop_executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_AGGREGATOR_AGENT_LOOP_TPE_MAX_WORKERS,
            thread_name_prefix="aggregator_agent_loop_executor",
        )
        self._queue_empty = threading.Condition()
        # expose metrics for the following and add alert on them
        self._total_events_received = 0
        self._total_events_dropped_at_core_worker = 0
        self._total_events_dropped_at_receiver = 0
        self._total_events_dropped_at_publisher = 0
        self._total_events_published = 0
        self._loop_num = 0

    async def ReceiveEvents(self, request, context):
        loop = get_or_create_event_loop()

        return await loop.run_in_executor(self._executor, self._receive_events, request)

    def _receive_events(self, request):
        events_data = request.data
        self._total_events_dropped_at_core_worker += len(
            events_data.dropped_task_attempts
        )
        batch_num = 0
        received_in_this_batch = 0
        for event in events_data.events_by_task:
            try:
                batch_num += 1
                self._event_buffer.put(event, block=False)
                received_in_this_batch += 1
                self._total_events_received += 1
            except queue.QueueFull:
                self._total_events_dropped_at_receiver += 1
                if self._total_events_dropped_at_receiver % 100 == 1:
                    logger.warning(
                        "Event buffer is full. Dropped %s events at "
                        "receiver. Received %s events. Published %s events. Dropped at "
                        "core worker: %s, dropped at publisher: %s, queue size: %s",
                        self._total_events_dropped_at_receiver,
                        self._total_events_received,
                        self._total_events_published,
                        self._total_events_dropped_at_core_worker,
                        self._total_events_dropped_at_publisher,
                        self._event_buffer.qsize(),
                    )

        gcs_status = gcs_service_pb2.GcsStatus(code=0, message="received")
        return gcs_service_pb2.AddTaskEventDataReply(status=gcs_status)

    def _publish_events(self):
        while True:
            while not self._event_buffer.empty():
                self._loop_num += 1
                if self._loop_num % 10 == 1:
                    logger.info(
                        "Received %s events. Published %s events. Dropped at core worker: %s, "
                        "dropped at receiver: %s, dropped at publisher: %s, queue size: %s",
                        self._total_events_received,
                        self._total_events_published,
                        self._total_events_dropped_at_core_worker,
                        self._total_events_dropped_at_receiver,
                        self._total_events_dropped_at_publisher,
                        self._event_buffer.qsize(),
                    )

                event_batch = []  # list of event json strings

                while (
                    len(event_batch) < MAX_EVENT_SEND_BATCH_SIZE
                    and not self._event_buffer.empty()
                ):
                    event_proto = self._event_buffer.get(block=False)
                    event_batch.append(MessageToJson((event_proto)))

                if event_batch:  # Check if the list is not empty
                    # query external service to send the events
                    try:
                        response = requests.post(
                            f"{EVENT_SEND_ADDR}:{EVENT_SEND_PORT}", json=event_batch
                        )
                        response.raise_for_status()
                        self._total_events_published += len(event_batch)
                    except requests.exceptions.RequestException as e:
                        logger.error(
                            "Failed to send events to external service. " "Error: %s",
                            e,
                        )
                        self._total_events_dropped_at_publisher += len(event_batch)

            time.sleep(MAX_EVENT_BUFFER_SEND_INTERVAL_SECONDS)

        # TODO: Graceful shutdown => make sure the events are all sent before shutdown
        # TODO: Better error handling => (1) retry events if failed;
        # (2) better error messages
        # TODO: Return correct status when receiving events
        # TODO: Support better encoding for the events => e.g. protobuf
        # TODO: Configuration needs to be exposed
        # TODO: Better monitoring => e.g. number of events metrics, etc.

    async def run(self, server):
        if server:
            event_pb2_grpc.add_AggregatorServiceServicer_to_server(self, server)

        loop = get_or_create_event_loop()
        loop.run_in_executor(self._loop_executor, self._publish_events)

    @staticmethod
    def is_minimal_module():
        return False
