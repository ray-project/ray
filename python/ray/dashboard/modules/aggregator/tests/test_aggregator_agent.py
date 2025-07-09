import sys
import json
import time
import base64

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from ray.dashboard.tests.conftest import *  # noqa

from ray._private import ray_constants
from ray._private.utils import init_grpc_channel
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
import ray.dashboard.consts as dashboard_consts
from ray._private.test_utils import (
    wait_until_server_available,
)

from ray.core.generated.events_event_aggregator_service_pb2_grpc import (
    EventAggregatorServiceStub,
)
from ray.core.generated.events_event_aggregator_service_pb2 import (
    AddEventRequest,
    RayEventsData,
    TaskEventsMetadata,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.common_pb2 import TaskAttempt


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", 12345)


def get_event_aggregator_grpc_stub(webui_url, gcs_address, head_node_id):
    """
    An helper function to get the gRPC stub for the event aggregator agent.
    Should only be used in tests.
    """
    ip, port = webui_url.split(":")
    agent_address = f"{ip}:{ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
    assert wait_until_server_available(agent_address)

    gcs_address = gcs_address
    gcs_client = GcsClient(address=gcs_address)
    agent_addr = gcs_client.internal_kv_get(
        f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{head_node_id}".encode(),
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
    )
    ip, http_port, grpc_port = json.loads(agent_addr)
    options = ray_constants.GLOBAL_GRPC_OPTIONS
    channel = init_grpc_channel(f"{ip}:{grpc_port}", options=options)
    return EventAggregatorServiceStub(channel)


def test_aggregator_agent_receive_publish_events_normally(
    ray_start_cluster_head, httpserver
):
    cluster = ray_start_cluster_head
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    test_time = 1751302230130457542
    seconds, nanos = divmod(test_time, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    request = AddEventRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    reply = stub.AddEvents(request)
    assert reply.status.status_code == 0
    assert reply.status.status_message == "all events received"

    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
    assert req_json[0]["sourceType"] == "CORE_WORKER"
    assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"
    assert req_json[0]["severity"] == "INFO"
    assert req_json[0]["message"] == "hello"
    assert req_json[0]["timestamp"] == "2025-06-30T16:50:30.130457542Z"


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 1,
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_receive_event_full(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    now = time.time_ns()
    seconds, nanos = divmod(now, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    request = AddEventRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    reply = stub.AddEvents(request)
    assert reply.status.status_code == 0
    assert reply.status.status_message == "all events received"

    reply = stub.AddEvents(request)
    assert reply.status.status_code == 5
    assert reply.status.status_message == "event 1 dropped because event buffer full"


def test_aggregator_agent_receive_dropped_at_core_worker(
    ray_start_cluster_head, httpserver
):
    cluster = ray_start_cluster_head
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    now = time.time_ns()
    seconds, nanos = divmod(now, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    request = AddEventRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"5",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="core worker event",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[
                    TaskAttempt(
                        task_id=b"1",
                        attempt_number=1,
                    ),
                    TaskAttempt(
                        task_id=b"2",
                        attempt_number=2,
                    ),
                ],
            ),
        )
    )

    reply = stub.AddEvents(request)
    assert reply.status.status_code == 0
    assert reply.status.status_message == "all events received"

    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert req_json[0]["message"] == "core worker event"


def test_aggregator_agent_receive_multiple_events(ray_start_cluster_head, httpserver):
    cluster = ray_start_cluster_head
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    now = time.time_ns()
    seconds, nanos = divmod(now, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)
    request = AddEventRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"3",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
                RayEvent(
                    event_id=b"4",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event2",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )
    reply = stub.AddEvents(request)
    assert reply.status.status_code == 0
    assert reply.status.status_message == "all events received"
    wait_for_condition(lambda: len(httpserver.log) == 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert len(req_json) == 2
    assert req_json[0]["message"] == "event1"
    assert req_json[1]["message"] == "event2"


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 1,
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_receive_multiple_events_failures(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    now = time.time_ns()
    seconds, nanos = divmod(now, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)
    request = AddEventRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
                RayEvent(
                    event_id=b"2",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event2",
                ),
                RayEvent(
                    event_id=b"3",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event3",
                ),
            ],
        )
    )
    reply = stub.AddEvents(request)
    assert reply.status.status_code == 5
    assert (
        reply.status.status_message
        == "event 1 dropped because event buffer full, event 2 dropped because event buffer full"
    )


def test_aggregator_agent_receive_empty_events(ray_start_cluster_head, httpserver):
    cluster = ray_start_cluster_head
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    request = AddEventRequest(
        events_data=RayEventsData(
            events=[],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )
    reply = stub.AddEvents(request)
    assert reply.status.status_code == 0
    assert reply.status.status_message == "all events received"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
