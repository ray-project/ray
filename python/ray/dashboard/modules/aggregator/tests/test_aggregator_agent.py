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
    find_free_port,
)

from ray.core.generated.events_event_aggregator_service_pb2_grpc import (
    EventAggregatorServiceStub,
)
from ray.core.generated.events_event_aggregator_service_pb2 import (
    AddEventsRequest,
    RayEventsData,
    TaskEventsMetadata,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.profile_events_pb2 import ProfileEvents, ProfileEventEntry
from ray.core.generated.events_task_profile_events_pb2 import TaskProfileEvents
from ray.core.generated.common_pb2 import TaskAttempt


_EVENT_AGGREGATOR_AGENT_TARGET_PORT = find_free_port()


@pytest.fixture(scope="module")
def httpserver_listen_address():
    return ("127.0.0.1", _EVENT_AGGREGATOR_AGENT_TARGET_PORT)


_with_aggregator_port = pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENT_SEND_PORT": str(
                    _EVENT_AGGREGATOR_AGENT_TARGET_PORT
                ),
            },
        },
    ],
    indirect=True,
)


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


@_with_aggregator_port
def test_aggregator_agent_receive_publish_events_normally(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    test_time = 1751302230130457542
    seconds, nanos = divmod(test_time, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    request = AddEventsRequest(
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
    assert reply.status.code == 0
    assert reply.status.message == "all events received"

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
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENT_SEND_PORT": _EVENT_AGGREGATOR_AGENT_TARGET_PORT,
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

    request = AddEventsRequest(
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
    assert reply.status.code == 0
    assert reply.status.message == "all events received"

    reply = stub.AddEvents(request)
    assert reply.status.code == 5
    assert reply.status.message == "event 1 dropped because event buffer full"


@_with_aggregator_port
def test_aggregator_agent_receive_dropped_at_core_worker(
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

    request = AddEventsRequest(
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
    assert reply.status.code == 0
    assert reply.status.message == "all events received"

    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert req_json[0]["message"] == "core worker event"


@_with_aggregator_port
def test_aggregator_agent_receive_multiple_events(
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
    request = AddEventsRequest(
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
    assert reply.status.code == 0
    assert reply.status.message == "all events received"
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
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENT_SEND_PORT": _EVENT_AGGREGATOR_AGENT_TARGET_PORT,
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
    request = AddEventsRequest(
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
    assert reply.status.code == 5
    assert (
        reply.status.message
        == "event 1 dropped because event buffer full, event 2 dropped because event buffer full"
    )


@_with_aggregator_port
def test_aggregator_agent_receive_empty_events(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )
    reply = stub.AddEvents(request)
    assert reply.status.code == 0
    assert reply.status.message == "all events received"


@_with_aggregator_port
def test_aggregator_agent_receive_profile_events(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    test_time = 1751302230130457542
    seconds, nanos = (test_time // 10**9, test_time % 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_PROFILE_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="profile event test",
                    task_profile_events=TaskProfileEvents(
                        task_id=b"100",
                        attempt_number=3,
                        job_id=b"200",
                        profile_events=ProfileEvents(
                            component_type="worker",
                            component_id=b"worker_123",
                            node_ip_address="127.0.0.1",
                            events=[
                                ProfileEventEntry(
                                    start_time=1751302230130000000,
                                    end_time=1751302230131000000,
                                    event_name="task_execution",
                                    extra_data='{"cpu_usage": 0.8}',
                                )
                            ],
                        ),
                    ),
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    reply = stub.AddEvents(request)
    assert reply.status.code == 0
    assert reply.status.message == "all events received"

    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
    assert req_json[0]["sourceType"] == "CORE_WORKER"
    assert req_json[0]["eventType"] == "TASK_PROFILE_EVENT"
    assert req_json[0]["severity"] == "INFO"
    assert req_json[0]["message"] == "profile event test"
    assert req_json[0]["timestamp"] == "2025-06-30T16:50:30.130457542Z"

    # Verify task profile event specific fields
    assert "taskProfileEvents" in req_json[0]
    task_profile_events = req_json[0]["taskProfileEvents"]
    assert task_profile_events["taskId"] == base64.b64encode(b"100").decode()
    assert task_profile_events["attemptNumber"] == 3
    assert task_profile_events["jobId"] == base64.b64encode(b"200").decode()

    # Verify profile event specific fields
    profile_event = task_profile_events["profileEvents"]
    assert profile_event["componentType"] == "worker"
    assert profile_event["componentId"] == base64.b64encode(b"worker_123").decode()
    assert profile_event["nodeIpAddress"] == "127.0.0.1"
    assert len(profile_event["events"]) == 1

    event_entry = profile_event["events"][0]
    assert event_entry["eventName"] == "task_execution"
    assert event_entry["startTime"] == "1751302230130000000"
    assert event_entry["endTime"] == "1751302230131000000"
    assert event_entry["extraData"] == '{"cpu_usage": 0.8}'


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
