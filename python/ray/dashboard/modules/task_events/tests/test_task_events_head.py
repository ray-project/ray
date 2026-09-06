import sys
from unittest.mock import AsyncMock

import pytest

import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
from ray._common.ray_constants import (
    LOGGING_ROTATE_BACKUP_COUNT,
    LOGGING_ROTATE_BYTES,
)
from ray._raylet import JobID
from ray.core.generated import (
    events_event_aggregator_service_pb2,
    gcs_pb2,
    gcs_service_pb2,
)
from ray.core.generated.common_pb2 import TaskType
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_task_definition_event_pb2 import TaskDefinitionEvent
from ray.dashboard.modules.task_events import task_event_query
from ray.dashboard.modules.task_events.task_events_head import TaskEventsHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig

_JOB = JobID.from_int(1).binary()


def _task_id(n: int) -> bytes:
    return bytes([n]) * 24


def _make_config() -> SubprocessModuleConfig:
    return SubprocessModuleConfig(
        cluster_id_hex="deadbeef",
        gcs_address="127.0.0.1:6379",
        session_name="test_session",
        temp_dir="/tmp",
        session_dir="/tmp",
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir="/tmp",
        logging_filename=dashboard_consts.DASHBOARD_LOG_FILENAME,
        logging_rotate_bytes=LOGGING_ROTATE_BYTES,
        logging_rotate_backup_count=LOGGING_ROTATE_BACKUP_COUNT,
        socket_dir="/tmp",
    )


def _make_head() -> TaskEventsHead:
    return TaskEventsHead(_make_config())


def _make_add_events_request(num_events: int, start: int = 0) -> bytes:
    events_data = events_event_aggregator_service_pb2.RayEventsData()
    for i in range(start, start + num_events):
        events_data.events.append(
            RayEvent(
                event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                task_definition_event=TaskDefinitionEvent(
                    task_id=_task_id(i),
                    task_attempt=0,
                    job_id=_JOB,
                    task_type=TaskType.NORMAL_TASK,
                    task_name=f"task_{i}",
                ),
            )
        )
    request = events_event_aggregator_service_pb2.AddEventsRequest(
        events_data=events_data
    )
    return request.SerializeToString()


def _fake_request(body: bytes):
    request = AsyncMock()
    request.read.return_value = body
    return request


def _add_stored_task(head, task_id, worker=None, job=_JOB):
    event = gcs_pb2.TaskEvents(task_id=task_id, attempt_number=0, job_id=job)
    event.task_info.type = TaskType.NORMAL_TASK
    if worker is not None:
        event.state_updates.worker_id = worker
    head._store.add_or_replace_task_event(event)


@pytest.mark.asyncio
async def test_add_task_events_buffers_events():
    head = _make_head()
    assert head.num_task_events_stored == 0

    body = _make_add_events_request(num_events=2)
    response = await head.add_task_events(_fake_request(body))

    assert response.status == 200
    assert head.num_task_events_stored == 2


@pytest.mark.asyncio
async def test_add_task_events_accumulates_across_requests():
    head = _make_head()

    await head.add_task_events(_fake_request(_make_add_events_request(2, start=0)))
    await head.add_task_events(_fake_request(_make_add_events_request(3, start=2)))

    assert head.num_task_events_stored == 5


@pytest.mark.asyncio
async def test_add_task_events_empty_payload():
    head = _make_head()

    response = await head.add_task_events(_fake_request(_make_add_events_request(0)))

    assert response.status == 200
    assert head.num_task_events_stored == 0


@pytest.mark.asyncio
async def test_add_task_events_bad_payload_returns_error():
    head = _make_head()

    # A lone continuation byte is a truncated protobuf tag varint and reliably
    # fails to parse, unlike arbitrary ASCII which protobuf may accept as
    # unknown fields.
    response = await head.add_task_events(_fake_request(b"\xff"))

    # Malformed body must not raise; the handler returns an error response and
    # nothing is buffered.
    assert response.status != 200
    assert head.num_task_events_stored == 0


def test_deserialize_request_roundtrip():
    head = _make_head()

    body = _make_add_events_request(num_events=1)
    request = head._deserialize_request(body)

    assert len(request.events_data.events) == 1


@pytest.mark.asyncio
async def test_get_task_events_endpoint_roundtrip():
    head = _make_head()
    _add_stored_task(head, _task_id(1))

    query = gcs_service_pb2.GetTaskEventsRequest()
    response = await head.get_task_events(_fake_request(query.SerializeToString()))

    assert response.status == 200
    reply = gcs_service_pb2.GetTaskEventsReply()
    reply.ParseFromString(response.body)
    assert [event.task_id for event in reply.events_by_task] == [_task_id(1)]


@pytest.mark.asyncio
async def test_get_task_events_invalid_predicate_returns_status_in_reply():
    head = _make_head()
    _add_stored_task(head, _task_id(1))

    query = gcs_service_pb2.GetTaskEventsRequest()
    task_filter = query.filters.task_filters.add()
    task_filter.task_id = _task_id(1)
    task_filter.predicate = 100
    response = await head.get_task_events(_fake_request(query.SerializeToString()))

    assert response.status == 200
    reply = gcs_service_pb2.GetTaskEventsReply()
    reply.ParseFromString(response.body)
    assert reply.status.code == task_event_query._INVALID_ARGUMENT_STATUS_CODE
    assert len(reply.events_by_task) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
