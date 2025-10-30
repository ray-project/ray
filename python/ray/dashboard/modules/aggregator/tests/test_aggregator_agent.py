import base64
import json
import sys
from typing import Optional
from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

import ray.dashboard.consts as dashboard_consts
from ray._common.network_utils import find_free_port
from ray._private import ray_constants
from ray._private.test_utils import wait_for_condition
from ray._private.utils import init_grpc_channel
from ray._raylet import GcsClient
from ray.core.generated.common_pb2 import (
    ErrorType,
    FunctionDescriptor,
    Language,
    PythonFunctionDescriptor,
    RayErrorInfo,
    TaskStatus,
    TaskType,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_driver_job_definition_event_pb2 import (
    DriverJobDefinitionEvent,
)
from ray.core.generated.events_driver_job_lifecycle_event_pb2 import (
    DriverJobLifecycleEvent,
)
from ray.core.generated.events_event_aggregator_service_pb2 import (
    AddEventsRequest,
    RayEventsData,
    TaskEventsMetadata,
)
from ray.core.generated.events_event_aggregator_service_pb2_grpc import (
    EventAggregatorServiceStub,
)
from ray.core.generated.events_task_definition_event_pb2 import (
    TaskDefinitionEvent,
)
from ray.core.generated.events_task_lifecycle_event_pb2 import (
    TaskLifecycleEvent,
)
from ray.core.generated.events_task_profile_events_pb2 import TaskProfileEvents
from ray.core.generated.profile_events_pb2 import ProfileEventEntry, ProfileEvents
from ray.dashboard.modules.aggregator.aggregator_agent import AggregatorAgent
from ray.dashboard.modules.aggregator.publisher.configs import (
    PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
)
from ray.dashboard.tests.conftest import *  # noqa

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


def generate_event_export_env_vars(
    preserve_proto_field_name: Optional[bool] = None, additional_env_vars: dict = None
) -> dict:
    if additional_env_vars is None:
        additional_env_vars = {}

    event_export_env_vars = {
        "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
    } | additional_env_vars

    if preserve_proto_field_name is not None:
        event_export_env_vars[
            "RAY_DASHBOARD_AGGREGATOR_AGENT_PRESERVE_PROTO_FIELD_NAME"
        ] = ("1" if preserve_proto_field_name is True else "0")

    return event_export_env_vars


def build_export_env_vars_param_list(additional_env_vars: dict = None) -> list:
    return [
        pytest.param(
            preserve_proto_field_name,
            {
                "env_vars": generate_event_export_env_vars(
                    preserve_proto_field_name, additional_env_vars
                )
            },
        )
        for preserve_proto_field_name in [True, False]
    ]


_with_preserve_proto_field_name_flag = pytest.mark.parametrize(
    ("preserve_proto_field_name", "ray_start_cluster_head_with_env_vars"),
    build_export_env_vars_param_list(),
    indirect=["ray_start_cluster_head_with_env_vars"],
)


def get_event_aggregator_grpc_stub(gcs_address, head_node_id):
    """
    An helper function to get the gRPC stub for the event aggregator agent.
    Should only be used in tests.
    """

    gcs_address = gcs_address
    gcs_client = GcsClient(address=gcs_address)

    def get_addr():
        return gcs_client.internal_kv_get(
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{head_node_id}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )

    wait_for_condition(lambda: get_addr() is not None)
    ip, _, grpc_port = json.loads(get_addr())
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
    dashboard_agent.session_name = "test_session"
    dashboard_agent.ip = "127.0.0.1"
    agent = AggregatorAgent(dashboard_agent)
    assert agent._event_processing_enabled == expected_event_processing_enabled


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": "",
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_event_processing_disabled(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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


@_with_preserve_proto_field_name_flag
def test_aggregator_agent_receive_publish_events_normally(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    fake_timestamp,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
    if preserve_proto_field_name:
        assert req_json[0]["event_id"] == base64.b64encode(b"1").decode()
        assert req_json[0]["source_type"] == "CORE_WORKER"
        assert req_json[0]["event_type"] == "TASK_DEFINITION_EVENT"
    else:
        assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
        assert req_json[0]["sourceType"] == "CORE_WORKER"
        assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"

    assert req_json[0]["severity"] == "INFO"
    assert req_json[0]["message"] == "hello"
    assert req_json[0]["timestamp"] == fake_timestamp[1]


@pytest.mark.parametrize(
    ("preserve_proto_field_name", "ray_start_cluster_head_with_env_vars"),
    build_export_env_vars_param_list(
        additional_env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 1,
        }
    ),
    indirect=["ray_start_cluster_head_with_env_vars"],
)
def test_aggregator_agent_receive_event_full(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    fake_timestamp,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
    if preserve_proto_field_name:
        assert req_json[0]["event_id"] == base64.b64encode(b"3").decode()
    else:
        assert req_json[0]["eventId"] == base64.b64encode(b"3").decode()


@_with_preserve_proto_field_name_flag
def test_aggregator_agent_receive_multiple_events(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    fake_timestamp,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
    if preserve_proto_field_name:
        assert req_json[0]["event_id"] == base64.b64encode(b"4").decode()
        assert req_json[1]["event_id"] == base64.b64encode(b"5").decode()
    else:
        assert req_json[0]["eventId"] == base64.b64encode(b"4").decode()
        assert req_json[1]["eventId"] == base64.b64encode(b"5").decode()

    assert req_json[0]["message"] == "event1"
    assert req_json[1]["message"] == "event2"


@pytest.mark.parametrize(
    ("preserve_proto_field_name", "ray_start_cluster_head_with_env_vars"),
    build_export_env_vars_param_list(
        additional_env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 1,
        }
    ),
    indirect=["ray_start_cluster_head_with_env_vars"],
)
def test_aggregator_agent_receive_multiple_events_failures(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    fake_timestamp,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
    if preserve_proto_field_name:
        assert req_json[0]["event_id"] == base64.b64encode(b"3").decode()
    else:
        assert req_json[0]["eventId"] == base64.b64encode(b"3").decode()


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [{"env_vars": generate_event_export_env_vars()}],
    indirect=True,
)
def test_aggregator_agent_receive_empty_events(
    ray_start_cluster_head_with_env_vars,
    httpserver,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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


@_with_preserve_proto_field_name_flag
def test_aggregator_agent_profile_events_not_exposed(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    fake_timestamp,
    preserve_proto_field_name,
):
    """Test that profile events are not sent when not in exposable event types."""
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
    if preserve_proto_field_name:
        assert req_json[0]["event_type"] == "TASK_DEFINITION_EVENT"
    else:
        assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"


def _create_task_definition_event_proto(timestamp):
    return RayEvent(
        event_id=b"1",
        source_type=RayEvent.SourceType.CORE_WORKER,
        event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
        timestamp=timestamp,
        severity=RayEvent.Severity.INFO,
        session_name="test_session",
        task_definition_event=TaskDefinitionEvent(
            task_id=b"1",
            task_attempt=1,
            task_type=TaskType.NORMAL_TASK,
            language=Language.PYTHON,
            task_func=FunctionDescriptor(
                python_function_descriptor=PythonFunctionDescriptor(
                    module_name="test_module",
                    class_name="test_class",
                    function_name="test_function",
                    function_hash="test_hash",
                ),
            ),
            task_name="test_task",
            required_resources={
                "CPU": 1.0,
                "GPU": 0.0,
            },
            serialized_runtime_env="{}",
            job_id=b"1",
            parent_task_id=b"1",
            placement_group_id=b"1",
            ref_ids={
                "key1": b"value1",
                "key2": b"value2",
            },
        ),
    )


def _verify_task_definition_event_json(
    req_json, expected_timestamp, preserve_proto_field_name
):
    assert len(req_json) == 1

    if preserve_proto_field_name:
        assert req_json[0]["event_id"] == base64.b64encode(b"1").decode()
        assert req_json[0]["source_type"] == "CORE_WORKER"
        assert req_json[0]["event_type"] == "TASK_DEFINITION_EVENT"
        assert req_json[0]["timestamp"] == expected_timestamp
        assert req_json[0]["severity"] == "INFO"
        assert (
            req_json[0]["message"] == ""
        )  # Make sure the default value is included when it is not set
        assert req_json[0]["session_name"] == "test_session"
        assert (
            req_json[0]["task_definition_event"]["task_id"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["task_definition_event"]["task_attempt"] == 1
        assert req_json[0]["task_definition_event"]["task_type"] == "NORMAL_TASK"
        assert req_json[0]["task_definition_event"]["language"] == "PYTHON"
        assert (
            req_json[0]["task_definition_event"]["task_func"][
                "python_function_descriptor"
            ]["module_name"]
            == "test_module"
        )
        assert (
            req_json[0]["task_definition_event"]["task_func"][
                "python_function_descriptor"
            ]["class_name"]
            == "test_class"
        )
        assert (
            req_json[0]["task_definition_event"]["task_func"][
                "python_function_descriptor"
            ]["function_name"]
            == "test_function"
        )
        assert (
            req_json[0]["task_definition_event"]["task_func"][
                "python_function_descriptor"
            ]["function_hash"]
            == "test_hash"
        )
        assert req_json[0]["task_definition_event"]["task_name"] == "test_task"
        assert req_json[0]["task_definition_event"]["required_resources"] == {
            "CPU": 1.0,
            "GPU": 0.0,
        }
        assert req_json[0]["task_definition_event"]["serialized_runtime_env"] == "{}"
        assert (
            req_json[0]["task_definition_event"]["job_id"]
            == base64.b64encode(b"1").decode()
        )
        assert (
            req_json[0]["task_definition_event"]["parent_task_id"]
            == base64.b64encode(b"1").decode()
        )
        assert (
            req_json[0]["task_definition_event"]["placement_group_id"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["task_definition_event"]["ref_ids"] == {
            "key1": base64.b64encode(b"value1").decode(),
            "key2": base64.b64encode(b"value2").decode(),
        }
    else:
        # Verify the base event fields
        assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
        assert req_json[0]["sourceType"] == "CORE_WORKER"
        assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"
        assert req_json[0]["timestamp"] == expected_timestamp
        assert req_json[0]["severity"] == "INFO"
        assert (
            req_json[0]["message"] == ""
        )  # Make sure the default value is included when it is not set
        assert req_json[0]["sessionName"] == "test_session"

        # Verify the task definition event specific fields
        assert (
            req_json[0]["taskDefinitionEvent"]["taskId"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["taskDefinitionEvent"]["taskAttempt"] == 1
        assert req_json[0]["taskDefinitionEvent"]["taskType"] == "NORMAL_TASK"
        assert req_json[0]["taskDefinitionEvent"]["language"] == "PYTHON"
        assert (
            req_json[0]["taskDefinitionEvent"]["taskFunc"]["pythonFunctionDescriptor"][
                "moduleName"
            ]
            == "test_module"
        )
        assert (
            req_json[0]["taskDefinitionEvent"]["taskFunc"]["pythonFunctionDescriptor"][
                "className"
            ]
            == "test_class"
        )
        assert (
            req_json[0]["taskDefinitionEvent"]["taskFunc"]["pythonFunctionDescriptor"][
                "functionName"
            ]
            == "test_function"
        )
        assert (
            req_json[0]["taskDefinitionEvent"]["taskFunc"]["pythonFunctionDescriptor"][
                "functionHash"
            ]
            == "test_hash"
        )
        assert req_json[0]["taskDefinitionEvent"]["taskName"] == "test_task"
        assert req_json[0]["taskDefinitionEvent"]["requiredResources"] == {
            "CPU": 1.0,
            "GPU": 0.0,
        }
        assert req_json[0]["taskDefinitionEvent"]["serializedRuntimeEnv"] == "{}"
        assert (
            req_json[0]["taskDefinitionEvent"]["jobId"]
            == base64.b64encode(b"1").decode()
        )
        assert (
            req_json[0]["taskDefinitionEvent"]["parentTaskId"]
            == base64.b64encode(b"1").decode()
        )
        assert (
            req_json[0]["taskDefinitionEvent"]["placementGroupId"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["taskDefinitionEvent"]["refIds"] == {
            "key1": base64.b64encode(b"value1").decode(),
            "key2": base64.b64encode(b"value2").decode(),
        }


def _create_task_lifecycle_event_proto(timestamp):
    return RayEvent(
        event_id=b"1",
        source_type=RayEvent.SourceType.CORE_WORKER,
        event_type=RayEvent.EventType.TASK_LIFECYCLE_EVENT,
        timestamp=timestamp,
        severity=RayEvent.Severity.INFO,
        session_name="test_session",
        task_lifecycle_event=TaskLifecycleEvent(
            task_id=b"1",
            task_attempt=1,
            state_transitions=[
                TaskLifecycleEvent.StateTransition(
                    state=TaskStatus.RUNNING,
                    timestamp=timestamp,
                ),
            ],
            ray_error_info=RayErrorInfo(
                error_type=ErrorType.TASK_EXECUTION_EXCEPTION,
            ),
            node_id=b"1",
            worker_id=b"1",
            worker_pid=1,
        ),
    )


def _verify_task_lifecycle_event_json(
    req_json, expected_timestamp, preserve_proto_field_name
):
    assert len(req_json) == 1

    if preserve_proto_field_name:
        assert req_json[0]["event_id"] == base64.b64encode(b"1").decode()
        assert req_json[0]["source_type"] == "CORE_WORKER"
        assert req_json[0]["event_type"] == "TASK_LIFECYCLE_EVENT"
        assert req_json[0]["timestamp"] == expected_timestamp
        assert req_json[0]["severity"] == "INFO"
        assert (
            req_json[0]["message"] == ""
        )  # Make sure the default value is included when it is not set
        assert req_json[0]["session_name"] == "test_session"
        assert (
            req_json[0]["task_lifecycle_event"]["task_id"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["task_lifecycle_event"]["task_attempt"] == 1
        assert req_json[0]["task_lifecycle_event"]["state_transitions"] == [
            {
                "state": "RUNNING",
                "timestamp": expected_timestamp,
            }
        ]
        assert (
            req_json[0]["task_lifecycle_event"]["ray_error_info"]["error_type"]
            == "TASK_EXECUTION_EXCEPTION"
        )
        assert (
            req_json[0]["task_lifecycle_event"]["node_id"]
            == base64.b64encode(b"1").decode()
        )
        assert (
            req_json[0]["task_lifecycle_event"]["worker_id"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["task_lifecycle_event"]["worker_pid"] == 1
    else:
        # Verify the base event fields
        assert req_json[0]["eventId"] == base64.b64encode(b"1").decode()
        assert req_json[0]["sourceType"] == "CORE_WORKER"
        assert req_json[0]["eventType"] == "TASK_LIFECYCLE_EVENT"
        assert req_json[0]["timestamp"] == expected_timestamp
        assert req_json[0]["severity"] == "INFO"
        assert (
            req_json[0]["message"] == ""
        )  # Make sure the default value is included when it is not set
        assert req_json[0]["sessionName"] == "test_session"

        # Verify the task execution event specific fields
        assert (
            req_json[0]["taskLifecycleEvent"]["taskId"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["taskLifecycleEvent"]["taskAttempt"] == 1
        assert req_json[0]["taskLifecycleEvent"]["stateTransitions"] == [
            {
                "state": "RUNNING",
                "timestamp": expected_timestamp,
            }
        ]
        assert (
            req_json[0]["taskLifecycleEvent"]["rayErrorInfo"]["errorType"]
            == "TASK_EXECUTION_EXCEPTION"
        )
        assert (
            req_json[0]["taskLifecycleEvent"]["nodeId"]
            == base64.b64encode(b"1").decode()
        )
        assert (
            req_json[0]["taskLifecycleEvent"]["workerId"]
            == base64.b64encode(b"1").decode()
        )
        assert req_json[0]["taskLifecycleEvent"]["workerPid"] == 1


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


def _verify_profile_event_json(req_json, expected_timestamp, preserve_proto_field_name):
    """Helper function to verify profile event JSON structure."""

    if preserve_proto_field_name:
        assert len(req_json) == 1
        assert req_json[0]["event_id"] == base64.b64encode(b"1").decode()
        assert req_json[0]["source_type"] == "CORE_WORKER"
        assert req_json[0]["event_type"] == "TASK_PROFILE_EVENT"
        assert req_json[0]["timestamp"] == expected_timestamp
        assert req_json[0]["severity"] == "INFO"
        assert req_json[0]["message"] == "profile event test"
        assert (
            req_json[0]["task_profile_events"]["task_id"]
            == base64.b64encode(b"100").decode()
        )
        assert req_json[0]["task_profile_events"]["attempt_number"] == 3
        assert (
            req_json[0]["task_profile_events"]["job_id"]
            == base64.b64encode(b"200").decode()
        )
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["component_type"]
            == "worker"
        )
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["component_id"]
            == base64.b64encode(b"worker_123").decode()
        )
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["node_ip_address"]
            == "127.0.0.1"
        )
        assert len(req_json[0]["task_profile_events"]["profile_events"]["events"]) == 1
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["events"][0][
                "start_time"
            ]
            == "1751302230130000000"
        )
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["events"][0][
                "end_time"
            ]
            == "1751302230131000000"
        )
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["events"][0][
                "extra_data"
            ]
            == '{"cpu_usage": 0.8}'
        )
        assert (
            req_json[0]["task_profile_events"]["profile_events"]["events"][0][
                "event_name"
            ]
            == "task_execution"
        )
    else:
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


# tuple: (create_event, verify)
EVENT_TYPES_TO_TEST = [
    pytest.param(
        _create_task_definition_event_proto,
        _verify_task_definition_event_json,
        id="task_definition_event",
    ),
    pytest.param(
        _create_task_lifecycle_event_proto,
        _verify_task_lifecycle_event_json,
        id="task_lifecycle_event",
    ),
    pytest.param(
        _create_profile_event_request, _verify_profile_event_json, id="profile_event"
    ),
]


@pytest.mark.parametrize("create_event, verify_event", EVENT_TYPES_TO_TEST)
@pytest.mark.parametrize(
    ("preserve_proto_field_name", "ray_start_cluster_head_with_env_vars"),
    build_export_env_vars_param_list(
        additional_env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "TASK_DEFINITION_EVENT,TASK_LIFECYCLE_EVENT,ACTOR_TASK_DEFINITION_EVENT,TASK_PROFILE_EVENT",
        }
    ),
    indirect=["ray_start_cluster_head_with_env_vars"],
)
def test_aggregator_agent_receive_events(
    create_event,
    verify_event,
    ray_start_cluster_head_with_env_vars,
    httpserver,
    fake_timestamp,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[create_event(fake_timestamp[0])],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    stub.AddEvents(request)
    wait_for_condition(lambda: len(httpserver.log) == 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    verify_event(req_json, fake_timestamp[1], preserve_proto_field_name)


@_with_preserve_proto_field_name_flag
def test_aggregator_agent_receive_driver_job_definition_event(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
                    event_type=RayEvent.EventType.DRIVER_JOB_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="driver job event",
                    driver_job_definition_event=DriverJobDefinitionEvent(
                        job_id=b"1",
                        config=DriverJobDefinitionEvent.Config(
                            serialized_runtime_env="{}",
                            metadata={},
                        ),
                    ),
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
    assert req_json[0]["message"] == "driver job event"
    if preserve_proto_field_name:
        assert (
            req_json[0]["driver_job_definition_event"]["config"][
                "serialized_runtime_env"
            ]
            == "{}"
        )
    else:
        assert (
            req_json[0]["driverJobDefinitionEvent"]["config"]["serializedRuntimeEnv"]
            == "{}"
        )


@_with_preserve_proto_field_name_flag
def test_aggregator_agent_receive_driver_job_lifecycle_event(
    ray_start_cluster_head_with_env_vars,
    httpserver,
    preserve_proto_field_name,
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
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
                    event_type=RayEvent.EventType.DRIVER_JOB_LIFECYCLE_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="driver job lifecycle event",
                    driver_job_lifecycle_event=DriverJobLifecycleEvent(
                        job_id=b"1",
                        state_transitions=[
                            DriverJobLifecycleEvent.StateTransition(
                                state=DriverJobLifecycleEvent.State.CREATED,
                                timestamp=Timestamp(seconds=1234567890),
                            ),
                            DriverJobLifecycleEvent.StateTransition(
                                state=DriverJobLifecycleEvent.State.FINISHED,
                                timestamp=Timestamp(seconds=1234567890),
                            ),
                        ],
                    ),
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
    assert req_json[0]["message"] == "driver job lifecycle event"
    if preserve_proto_field_name:
        assert (
            req_json[0]["driver_job_lifecycle_event"]["job_id"]
            == base64.b64encode(b"1").decode()
        )
        assert len(req_json[0]["driver_job_lifecycle_event"]["state_transitions"]) == 2
        assert (
            req_json[0]["driver_job_lifecycle_event"]["state_transitions"][0]["state"]
            == "CREATED"
        )
        assert (
            req_json[0]["driver_job_lifecycle_event"]["state_transitions"][1]["state"]
            == "FINISHED"
        )
    else:
        assert (
            req_json[0]["driverJobLifecycleEvent"]["jobId"]
            == base64.b64encode(b"1").decode()
        )
        assert len(req_json[0]["driverJobLifecycleEvent"]["stateTransitions"]) == 2
        assert (
            req_json[0]["driverJobLifecycleEvent"]["stateTransitions"][0]["state"]
            == "CREATED"
        )
        assert (
            req_json[0]["driverJobLifecycleEvent"]["stateTransitions"][1]["state"]
            == "FINISHED"
        )


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": generate_event_export_env_vars(
                additional_env_vars={
                    "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE": "False",
                }
            )
        },
    ],
    indirect=True,
)
def test_aggregator_agent_http_svc_publish_disabled(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
    )

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"10",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=fake_timestamp[0],
                    severity=RayEvent.Severity.INFO,
                    message="should not be sent",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    stub.AddEvents(request)

    with pytest.raises(
        RuntimeError, match="The condition wasn't met before the timeout expired."
    ):
        # Wait for up to 2 seconds (publish interval + 1second buffer) to ensure that the event is never published to the external HTTP service
        wait_for_condition(
            lambda: len(httpserver.log) > 0,
            1 + PUBLISHER_MAX_BUFFER_SEND_INTERVAL_SECONDS,
        )

    assert len(httpserver.log) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
