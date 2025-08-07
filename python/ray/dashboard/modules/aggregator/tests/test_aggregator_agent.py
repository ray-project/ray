import sys
import json
import time
import base64
import threading
from unittest.mock import Mock, patch, MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from ray.dashboard.tests.conftest import *  # noqa
from ray.dashboard.modules.aggregator.aggregator_agent import AggregatorAgent

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
    AddEventsReply,
    AddEventsStatus,
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
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS": False,
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
    assert reply is not None
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
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENT_SEND_PORT": str(
                    _EVENT_AGGREGATOR_AGENT_TARGET_PORT
                ),
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS": False,
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

    test_time = 1751302230130457542
    seconds, nanos = divmod(test_time, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"2",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
                RayEvent(
                    event_id=b"3",
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
    assert reply is not None
    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    assert len(req_json) == 1
    assert req_json[0]["eventId"] == base64.b64encode(b"3").decode()


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
                    event_id=b"4",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
                RayEvent(
                    event_id=b"5",
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
    assert reply is not None
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
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENT_SEND_PORT": str(
                    _EVENT_AGGREGATOR_AGENT_TARGET_PORT
                ),
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS": False,
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
    assert reply is not None
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
    reply = stub.AddEvents(request)
    assert reply is not None


@_with_aggregator_port
def test_aggregator_agent_profile_events_not_exposed(
    ray_start_cluster_head_with_env_vars, httpserver
):
    """Test that profile events are not sent when not in exposable event types."""
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
                _create_profile_event_request(),
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="event1",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    reply = stub.AddEvents(request)
    assert reply is not None

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
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENT_SEND_PORT": str(
                    _EVENT_AGGREGATOR_AGENT_TARGET_PORT
                ),
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "TASK_DEFINITION_EVENT,TASK_EXECUTION_EVENT,ACTOR_TASK_DEFINITION_EVENT,ACTOR_TASK_EXECUTION_EVENT,TASK_PROFILE_EVENT",
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS": False,
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_receive_profile_events(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.webui_url, cluster.gcs_address, cluster.head_node.node_id
    )

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[_create_profile_event_request()],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    reply = stub.AddEvents(request)
    assert reply is not None

    wait_for_condition(lambda: len(httpserver.log) == 1)

    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)

    _verify_profile_event_json(req_json)


def _create_profile_event_request():
    """Helper function to create a profile event request."""
    test_time = 1751302230130457542
    seconds, nanos = (test_time // 10**9, test_time % 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

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


def _verify_profile_event_json(req_json):
    """Helper function to verify profile event JSON structure."""
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


# =============================================================================
# Test Publishing events to GCS and external service using mocks
# =============================================================================


def create_test_event(event_id: str, message: str = "test"):
    """Helper function to create test events"""
    test_time = int(time.time() * 1_000_000_000)  # nanoseconds
    seconds, nanos = divmod(test_time, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)

    return RayEvent(
        event_id=event_id.encode(),
        source_type=RayEvent.SourceType.CORE_WORKER,
        event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
        timestamp=timestamp,
        severity=RayEvent.Severity.INFO,
        message=message,
    )


def create_test_metadata(dropped_task_ids: list = None, attempt_number=1):
    """Helper function to create test metadata"""
    metadata = TaskEventsMetadata()
    if dropped_task_ids:
        for task_id in dropped_task_ids:
            attempt = metadata.dropped_task_attempts.add()
            attempt.task_id = task_id.encode()
            attempt.attempt_number = attempt_number
    return metadata


def setup_mocks():
    # Create a mock dashboard agent
    mock_dashboard_agent = Mock()
    mock_dashboard_agent.ip = "127.0.0.1"
    mock_dashboard_agent.gcs_address = "127.0.0.1:6379"

    # Create AggregatorAgent instance
    with patch("ray._private.gcs_utils.create_gcs_channel") as mock_create_channel:
        with patch(
            "ray.core.generated.gcs_service_pb2_grpc.RayEventExportGcsServiceStub"
        ) as mock_gcs_stub_class:
            # Setup mocks
            mock_channel = Mock()
            mock_create_channel.return_value = mock_channel

            mock_gcs_stub = Mock()
            mock_gcs_stub_class.return_value = mock_gcs_stub

            agent = AggregatorAgent(mock_dashboard_agent)

            return agent, mock_gcs_stub


def test_aggregator_agent_gcs_publishing():
    """Test: AggregatorAgent component `_send_events_to_gcs` method in isolation with proper mocking"""

    agent, mock_gcs_stub = setup_mocks()

    # Mock successful GCS response
    gcs_reply = AddEventsReply()
    gcs_reply.status.code = 0
    mock_gcs_stub.AddEvents.return_value = gcs_reply

    events = [create_test_event("1"), create_test_event("2")]
    metadata = create_test_metadata(["task_X"])

    result = agent._send_events_to_gcs(events, metadata)

    # Verify behavior
    assert result is True
    assert mock_gcs_stub.AddEvents.call_count == 1

    # Verify call arguments
    call_args = mock_gcs_stub.AddEvents.call_args[0][0]
    assert len(call_args.events_data.events) == 2
    assert call_args.events_data.events[0].event_id == b"1"
    assert call_args.events_data.events[1].event_id == b"2"
    assert len(call_args.events_data.task_events_metadata.dropped_task_attempts) == 1
    assert (
        call_args.events_data.task_events_metadata.dropped_task_attempts[0].task_id
        == b"task_X"
    )
    assert (
        call_args.events_data.task_events_metadata.dropped_task_attempts[
            0
        ].attempt_number
        == 1
    )


def test_aggregator_agent_gcs_publish_failure():
    agent, mock_gcs_stub = setup_mocks()

    # Mock GCS failure
    gcs_failure_reply = AddEventsReply()
    gcs_failure_reply.status.code = 1
    gcs_failure_reply.status.message = "GCS failure"
    mock_gcs_stub.AddEvents.return_value = gcs_failure_reply

    events = [create_test_event("1")]
    metadata = create_test_metadata(["task_fail"])

    result = agent._send_events_to_gcs(events, metadata)

    # Verify failure handling
    assert result is False
    assert mock_gcs_stub.AddEvents.call_count == 1


def test_external_service_publish_mock_with_requests():
    agent, _ = setup_mocks()
    # Mock the HTTP session
    with patch.object(agent, "_http_session") as mock_session:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response

        # Test external service call
        events = [create_test_event("1", "external test")]
        result = agent._send_events_to_external_service(events)

        # Verify success
        assert result is True
        assert mock_session.post.call_count == 1

        # Test failure
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        result = agent._send_events_to_external_service(events)
        assert result is False


def test_accumulated_task_event_metadata_helper_class():
    # Create instance of the helper class
    accumulator = AggregatorAgent.AccumulatedTaskMetadata()

    # Test initial state
    assert accumulator.is_empty()
    assert accumulator.get_and_reset() is None

    # Test merging
    metadata1 = create_test_metadata(["task_1", "task_2"])
    metadata2 = create_test_metadata(["task_2", "task_3"])  # task_2 is duplicate

    accumulator.merge(metadata1)
    assert not accumulator.is_empty()

    accumulator.merge(metadata2)

    # Get and verify deduplication
    result = accumulator.get_and_reset()
    assert result is not None
    # should contain entries for task_1, task_2 (just one entry) and task_3
    assert len(result.dropped_task_attempts) == 3
    dropped_tasks = {attempt.task_id for attempt in result.dropped_task_attempts}
    assert dropped_tasks == {b"task_1", b"task_2", b"task_3"}

    # Verify reset
    assert accumulator.is_empty()
    assert accumulator.get_and_reset() is None

    # Test merging None
    accumulator.merge(None)
    assert accumulator.is_empty()

    # Test merging when task_id is same but differs in attempt number
    metadata3 = create_test_metadata(["task_2"])
    metadata4 = create_test_metadata(["task_2"], 2)
    accumulator.merge(metadata3)
    accumulator.merge(metadata4)
    result = accumulator.get_and_reset()
    assert result is not None
    # should contain entries for task_2 (attempt 1) and task_2 (attempt 2)
    assert len(result.dropped_task_attempts) == 2
    [dropped_task_1, dropped_task_2] = result.dropped_task_attempts
    assert dropped_task_1.task_id == dropped_task_2.task_id == b"task_2"
    assert dropped_task_1.attempt_number == 1
    assert dropped_task_2.attempt_number == 2


@pytest.mark.parametrize(
    "gcs_failures,external_failures,expected_gcs_calls,expected_external_calls",
    [
        # Happy path: both gcs and external service publish goes through successfully
        (0, 0, 1, 1),
        # GCS fails once, then succeeds on retry; external service succeeds immediately
        (1, 0, 2, 1),
        # GCS succeeds immediately; external fails once, then succeeds on retry
        (0, 1, 1, 2),
    ],
)
def test_publish_events_retry_scenarios(
    gcs_failures, external_failures, expected_gcs_calls, expected_external_calls
):
    """Test: _publish_events retry logic for various failure scenarios"""
    agent, _ = setup_mocks()

    # Setup failure counters and mock functions
    gcs_call_count = 0

    def mock_gcs_send(event_batch, task_events_metadata=None):
        nonlocal gcs_call_count
        gcs_call_count += 1
        return gcs_call_count > gcs_failures  # Fail first N times, then succeed

    external_call_count = 0

    def mock_external_send(event_batch):
        nonlocal external_call_count
        external_call_count += 1
        return (
            external_call_count > external_failures
        )  # Fail first N times, then succeed

    agent._send_events_to_gcs = Mock(side_effect=mock_gcs_send)
    agent._send_events_to_external_service = Mock(side_effect=mock_external_send)

    # Add test event and metadata
    agent._event_buffer.put_nowait(create_test_event("1"))
    test_metadata = create_test_metadata(["task_test"])
    agent._accumulated_task_metadata.merge(test_metadata)

    # Mock stop event to stop after successful completion
    wait_call_count = 0

    def mock_wait(timeout):
        nonlocal wait_call_count
        wait_call_count += 1
        # Stop after enough iterations for retries to complete
        return wait_call_count > (gcs_failures + external_failures + 2)

    agent._stop_event.wait = Mock(side_effect=mock_wait)

    agent._publish_events()

    # Verify call counts match expected behavior
    assert agent._send_events_to_gcs.call_count == expected_gcs_calls
    assert agent._send_events_to_external_service.call_count == expected_external_calls


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
