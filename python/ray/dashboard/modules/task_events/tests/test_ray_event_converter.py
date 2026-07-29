import sys

import pytest

from ray._raylet import JobID, TaskID
from ray.core.generated.common_pb2 import (
    FunctionDescriptor,
    Language,
    TaskAttempt,
    TaskStatus,
    TaskType,
)
from ray.core.generated.events_actor_task_definition_event_pb2 import (
    ActorTaskDefinitionEvent,
)
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_event_aggregator_service_pb2 import (
    AddEventsRequest,
    RayEventsData,
    TaskEventsMetadata,
)
from ray.core.generated.events_task_definition_event_pb2 import TaskDefinitionEvent
from ray.core.generated.events_task_lifecycle_event_pb2 import TaskLifecycleEvent
from ray.core.generated.events_task_profile_events_pb2 import TaskProfileEvents
from ray.core.generated.profile_events_pb2 import ProfileEventEntry, ProfileEvents
from ray.dashboard.modules.task_events import ray_event_converter as rec


def _ids():
    job = JobID.from_int(1)
    return job, TaskID.for_fake_task(job)


def _python_fd(module_name="", class_name="", function_name=""):
    fd = FunctionDescriptor()
    fd.python_function_descriptor.module_name = module_name
    fd.python_function_descriptor.class_name = class_name
    fd.python_function_descriptor.function_name = function_name
    return fd


def test_call_string_python_no_class():
    assert rec._call_string(_python_fd(function_name="mymod.myfunc")) == "myfunc"


def test_call_string_python_with_class():
    fd = _python_fd(class_name="pkg.MyCls", function_name="pkg.MyCls.m")
    assert rec._call_string(fd) == "MyCls.m"


def test_call_string_java_and_cpp_and_empty():
    java = FunctionDescriptor()
    java.java_function_descriptor.class_name = "C"
    java.java_function_descriptor.function_name = "f"
    assert rec._call_string(java) == "C.f"

    cpp = FunctionDescriptor()
    cpp.cpp_function_descriptor.function_name = "g"
    assert rec._call_string(cpp) == "g"

    assert rec._call_string(FunctionDescriptor()) == ""


def test_convert_task_definition_event():
    job, tid = _ids()
    event = TaskDefinitionEvent(
        task_id=tid.binary(),
        task_attempt=0,
        job_id=job.binary(),
        task_type=TaskType.NORMAL_TASK,
        task_name="foo",
        parent_task_id=tid.binary(),
        task_func=_python_fd(function_name="mymod.foo"),
        language=Language.PYTHON,
        serialized_runtime_env="{}",
    )
    event.required_resources["CPU"] = 1.0

    task_event = rec._convert_task_definition_event(event)

    assert task_event.task_id == tid.binary()
    assert task_event.attempt_number == 0
    assert task_event.job_id == job.binary()
    info = task_event.task_info
    assert info.type == TaskType.NORMAL_TASK
    assert info.name == "foo"
    assert info.func_or_class_name == "foo"
    assert info.language == Language.PYTHON
    assert info.required_resources["CPU"] == 1.0
    assert info.runtime_env_info.serialized_runtime_env == "{}"


def test_convert_task_lifecycle_event():
    job, tid = _ids()
    event = TaskLifecycleEvent(
        task_id=tid.binary(),
        task_attempt=0,
        job_id=job.binary(),
        worker_pid=42,
        node_id=b"n" * 28,
    )
    transition = event.state_transitions.add()
    transition.state = TaskStatus.RUNNING
    transition.timestamp.seconds = 5
    transition.timestamp.nanos = 123

    task_event = rec._convert_task_lifecycle_event(event)

    state = task_event.state_updates
    assert state.worker_pid == 42
    assert state.node_id == b"n" * 28
    assert state.state_ts_ns[TaskStatus.RUNNING] == 5 * 10**9 + 123


def test_convert_actor_task_definition_event():
    job, tid = _ids()
    event = ActorTaskDefinitionEvent(
        task_id=tid.binary(),
        task_attempt=0,
        job_id=job.binary(),
        actor_task_name="act",
        parent_task_id=tid.binary(),
        actor_id=b"a" * 16,
        actor_func=_python_fd(function_name="m.run"),
        language=Language.PYTHON,
    )

    task_event = rec._convert_actor_task_definition_event(event)

    info = task_event.task_info
    assert info.type == TaskType.ACTOR_TASK
    assert info.name == "act"
    assert info.actor_id == b"a" * 16
    assert info.func_or_class_name == "run"


def test_convert_task_profile_events():
    job, tid = _ids()
    profile = ProfileEvents()
    entry = ProfileEventEntry(event_name="e")
    profile.events.append(entry)
    event = TaskProfileEvents(
        task_id=tid.binary(),
        attempt_number=0,
        job_id=job.binary(),
        profile_events=profile,
    )

    task_event = rec._convert_task_profile_events(event)

    assert task_event.task_id == tid.binary()
    assert len(task_event.profile_events.events) == 1


def test_convert_dispatches_and_returns_dropped_attempts():
    job, tid = _ids()
    definition = TaskDefinitionEvent(
        task_id=tid.binary(),
        task_attempt=0,
        job_id=job.binary(),
        task_type=TaskType.NORMAL_TASK,
        task_name="foo",
        task_func=_python_fd(function_name="foo"),
    )
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    task_definition_event=definition,
                )
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[
                    TaskAttempt(task_id=tid.binary(), attempt_number=3)
                ]
            ),
        )
    )

    task_events, dropped = rec.convert(request)

    assert len(task_events) == 1
    assert task_events[0].task_info.name == "foo"
    assert len(dropped) == 1
    assert dropped[0].attempt_number == 3


def test_convert_raises_on_unsupported_event_type():
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[RayEvent(event_type=RayEvent.EventType.DRIVER_JOB_LIFECYCLE_EVENT)]
        )
    )

    with pytest.raises(AssertionError):
        rec.convert(request)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
