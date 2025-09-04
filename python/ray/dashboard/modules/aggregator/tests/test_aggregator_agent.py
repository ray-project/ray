import sys
import json
import base64
from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
import uuid

from ray.dashboard.tests.conftest import *  # noqa

from ray._private import ray_constants
from ray._private.utils import init_grpc_channel
from ray._private.test_utils import wait_for_condition
from ray._raylet import GcsClient
import ray.dashboard.consts as dashboard_consts
from ray._private.test_utils import (
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
from ray.core.generated.events_task_definition_event_pb2 import (
    TaskDefinitionEvent,
)
from ray.core.generated.events_task_execution_event_pb2 import (
    TaskExecutionEvent,
)
from ray.core.generated.profile_events_pb2 import ProfileEvents, ProfileEventEntry
from ray.core.generated.events_task_profile_events_pb2 import TaskProfileEvents
from ray.core.generated.events_driver_job_definition_event_pb2 import (
    DriverJobDefinitionEvent,
)
from ray.core.generated.events_driver_job_execution_event_pb2 import (
    DriverJobExecutionEvent,
)
from ray.core.generated.runtime_environment_pb2 import (
    RuntimeEnvInfo,
    RuntimeEnvUris,
    RuntimeEnvConfig,
)
from ray.core.generated.common_pb2 import (
    TaskType,
    Language,
    FunctionDescriptor,
    PythonFunctionDescriptor,
    TaskStatus,
    ErrorType,
    RayErrorInfo,
)
from ray.core.generated.gcs_service_pb2_grpc import TaskInfoGcsServiceStub
from ray.core.generated.gcs_service_pb2 import GetTaskEventsRequest, FilterPredicate
from ray._raylet import JobID, TaskID

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


def get_task_info_gcs_stub(gcs_address):
    """Helper to get the gRPC stub for TaskInfoGcsService."""
    channel = init_grpc_channel(gcs_address, options=ray_constants.GLOBAL_GRPC_OPTIONS)
    return TaskInfoGcsServiceStub(channel)


def _get_task_event_from_gcs(
    task_info_stub, unique_task_name: str, rpc_timeout_s: int = 5
):
    """Fetch and return the first matching task event by task name from GCS, or None."""
    try:
        get_req = GetTaskEventsRequest()
        get_req.limit = 100
        get_req.filters.exclude_driver = False
        name_filter = get_req.filters.task_name_filters.add()
        name_filter.predicate = FilterPredicate.EQUAL
        name_filter.task_name = unique_task_name
        reply = task_info_stub.GetTaskEvents(get_req, timeout=rpc_timeout_s)
        if reply.status.code != 0:
            return None
        for task_event in reply.events_by_task:
            if task_event.task_info.name.lower() == unique_task_name.lower():
                print(reply)
                return task_event
        return None
    except Exception:
        return None


def _create_task_definition_event_for_gcs(timestamp, unique_task_name: str):
    """Create and return a task definition event for GCS with valid task id and job id and a unique task name"""
    job_id = JobID.from_int(1)
    task_id = TaskID.for_fake_task(job_id)

    event = _create_task_definition_event_proto(timestamp)
    event.task_definition_event.task_name = unique_task_name
    event.task_definition_event.task_id = task_id.binary()
    event.task_definition_event.job_id = job_id.binary()
    event.task_definition_event.parent_task_id = task_id.binary()
    event.task_definition_event.placement_group_id = b"1"
    return event


def _wait_for_and_verify_task_definition_event_in_gcs(
    task_info_stub, unique_task_name: str, sent_event
):
    """Wait for the task event to be stored in GCS and verify the fields match the sent event"""
    wait_for_condition(
        lambda: _get_task_event_from_gcs(task_info_stub, unique_task_name) is not None
    )
    matched_task_event = _get_task_event_from_gcs(task_info_stub, unique_task_name)

    # Verify fields match
    expected = sent_event.task_definition_event
    assert matched_task_event.task_info.name == expected.task_name
    assert matched_task_event.attempt_number == expected.task_attempt
    assert matched_task_event.task_info.task_id == expected.task_id
    # job_id is set at both top-level and inside task_info. Verify top-level as ground truth
    assert matched_task_event.job_id == expected.job_id
    assert matched_task_event.task_info.parent_task_id == expected.parent_task_id
    if expected.placement_group_id:
        assert (
            matched_task_event.task_info.placement_group_id
            == expected.placement_group_id
        )

    # Optional, verify type/language/func name when present
    if hasattr(matched_task_event.task_info, "type") and expected.task_type is not None:
        assert matched_task_event.task_info.type == expected.task_type
    if (
        hasattr(matched_task_event.task_info, "language")
        and expected.language is not None
    ):
        assert matched_task_event.task_info.language == expected.language
    if hasattr(matched_task_event.task_info, "func_or_class_name"):
        assert (
            matched_task_event.task_info.func_or_class_name
            == expected.task_func.python_function_descriptor.function_name
        )


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                # Disable HTTP publisher
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC": "False",
                # Enable GCS publisher
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS": "True",
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_publish_to_gcs_only(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    # Aggregator agent (receives AddEvents from workers)
    agg_stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
    )
    # GCS TaskInfo service (query to verify events stored)
    task_info_stub = get_task_info_gcs_stub(cluster.gcs_address)

    # Create an event with a unique task name to filter on
    unique_task_name = f"gcs_only_task_{uuid.uuid4()}"
    event = _create_task_definition_event_for_gcs(fake_timestamp[0], unique_task_name)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[event],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    agg_stub.AddEvents(request)

    _wait_for_and_verify_task_definition_event_in_gcs(
        task_info_stub, unique_task_name, event
    )

    # Ensure HTTP publisher did not send anything
    with pytest.raises(
        RuntimeError, match="The condition wasn't met before the timeout expired."
    ):
        wait_for_condition(lambda: len(httpserver.log) > 0, 1)
    assert len(httpserver.log) == 0


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                # Enable both publishers
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_GCS": "True",
                "RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SVC": "True",
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
            },
        },
    ],
    indirect=True,
)
def test_aggregator_agent_publish_to_both_gcs_and_http(
    ray_start_cluster_head_with_env_vars, httpserver, fake_timestamp
):
    cluster = ray_start_cluster_head_with_env_vars
    agg_stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
    )
    task_info_stub = get_task_info_gcs_stub(cluster.gcs_address)

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    # Create an event with a unique task name to filter on
    unique_task_name = f"gcs_only_task_{uuid.uuid4()}"
    event = _create_task_definition_event_for_gcs(fake_timestamp[0], unique_task_name)

    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[event],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[],
            ),
        )
    )

    agg_stub.AddEvents(request)

    # Verify HTTP received the event
    wait_for_condition(lambda: len(httpserver.log) == 1)
    req, _ = httpserver.log[0]
    req_json = json.loads(req.data)
    assert len(req_json) == 1
    assert req_json[0]["eventType"] == "TASK_DEFINITION_EVENT"
    assert req_json[0]["taskDefinitionEvent"]["taskName"] == unique_task_name

    # Verify GCS stored the event and fields match
    _wait_for_and_verify_task_definition_event_in_gcs(
        task_info_stub, unique_task_name, event
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
