import sys
import json
import base64
from unittest.mock import MagicMock

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
from ray._common.network_utils import parse_address, build_address

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

from ray.dashboard.modules.aggregator.aggregator_agent import AggregatorAgent


_EVENT_AGGREGATOR_AGENT_TARGET_PORT = find_free_port()
_EVENT_AGGREGATOR_AGENT_TARGET_IP = "127.0.0.1"
_EVENT_AGGREGATOR_AGENT_TARGET_ADDR = (
    f"http://{_EVENT_AGGREGATOR_AGENT_TARGET_IP}:{_EVENT_AGGREGATOR_AGENT_TARGET_PORT}"
)


@pytest.fixture(scope="module")
def httpserver_listen_address():
    return (_EVENT_AGGREGATOR_AGENT_TARGET_IP, _EVENT_AGGREGATOR_AGENT_TARGET_PORT)


@pytest.fixture
def fake_timestamp():
    """
    Returns a fake proto timestamp and the expected timestamp string in the event JSON.
    """
    test_time = 1751302230130457542
    seconds, nanos = divmod(test_time, 10**9)
    return Timestamp(seconds=seconds, nanos=nanos), "2025-06-30T16:50:30.130457542Z"


_with_aggregator_port = pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
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
    ip, _ = parse_address(webui_url)
    agent_address = build_address(ip, ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT)
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


@pytest.mark.parametrize(
    (
        "export_addr",
        "expected_http_target_enabled",
        "expected_event_processing_enabled",
    ),
    [
        ("", False, False),
        ("http://127.0.0.1:" + str(_EVENT_AGGREGATOR_AGENT_TARGET_PORT), True, True),
    ],
)
def test_aggregator_agent_http_target_not_enabled(
    export_addr,
    expected_http_target_enabled,
    expected_event_processing_enabled,
):
    dashboard_agent = MagicMock()
    dashboard_agent.events_export_addr = export_addr
    agent = AggregatorAgent(dashboard_agent)
    assert agent._event_http_target_enabled == expected_http_target_enabled
    assert agent._event_processing_enabled == expected_event_processing_enabled


@_with_aggregator_port
def test_aggregator_agent_receive_publish_events_normally(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    stub.AddEvents(request)
    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
    assert req_json[0]["sourceType"] == "CORE_WORKER"
    assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"
    assert req_json[0]["severity"] == "INFO"
    assert req_json[0]["message"] == "hello"
    assert req_json[0]["timestamp"] == fake_timestamp[1]


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 1,
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_receive_event_full(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"2",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
                RayEvent(
                    event_id=b"3",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    stub.AddEvents(request)
    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"3").decode()


@_with_aggregator_port
def test_aggregator_agent_receive_multiple_events(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"4",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
                RayEvent(
                    event_id=b"5",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="event2",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )
    stub.AddEvents(request)
    wait_for_condition(lambda: len(httpserver.log) == 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert len(req_json) == 2
    assert req_json[0]["eventId"] == base64.b64encode(b"4").decode()
    assert req_json[0]["message"] == "event1"
    assert req_json[1]["eventId"] == base64.b64encode(b"5").decode()
    assert req_json[1]["message"] == "event2"


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 1,
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_receive_multiple_events_failures(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
                RayEvent(
                    event_id=b"2",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="event2",
                ),
                RayEvent(
                    event_id=b"3",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="event3",
                ),
            ],
        )
    )
    stub.AddEvents(request)
    wait_for_condition(lambda: len(httpserver.log) == 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"3").decode()


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
    stub.AddEvents(request)


@_with_aggregator_port
def test_aggregator_agent_profile_events_not_exposed(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    """Test that profile events are not sent when not in exposable event types."""
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                _create_profile_event_request(fake_timestamp[0]),
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    stub.AddEvents(request)

    # Wait for exactly one event to be received (the TASK_DEFINITION_EVENT)
    wait_for_condition(lambda: len(httpserver.log) == 1)

    # Verify that only the TASK_DEFINITION_EVENT was sent, not the profile event
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) == 1
    assert req_json[0]["message"] == "event1"
    assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "TASK_DEFINITION_EVENT,TASK_EXECUTION_EVENT,ACTOR_TASK_DEFINITION_EVENT,ACTOR_TASK_EXECUTION_EVENT,TASK_PROFILE_EVENT",
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_receive_profile_events(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[_create_profile_event_request(fake_timestamp[0])],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    stub.AddEvents(request)

    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    _verify_profile_event_json(req_json, fake_timestamp[1])


def _create_profile_event_request(timestamp):
    """Helper function to create a profile event request."""

    return RayEvent(
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
    )


def _verify_profile_event_json(req_json, expected_timestamp):
    """Helper function to verify profile event JSON structure."""
    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
    assert req_json[0]["sourceType"] == "CORE_WORKER"
    assert req_json[0]["eventType"] == "TASK_PROFILE_EVENT"
    assert req_json[0]["severity"] == "INFO"
    assert req_json[0]["message"] == "profile event test"
    assert req_json[0]["timestamp"] == expected_timestamp

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
