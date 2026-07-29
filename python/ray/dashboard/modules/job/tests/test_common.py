import asyncio
import json
from dataclasses import asdict
from unittest.mock import AsyncMock, MagicMock

import pytest
from google.protobuf.json_format import Parse

from ray.core.generated.common_pb2 import (
    ActorDiedErrorContext,
    ErrorType,
    InfraCauseContext,
    JobFailureInfo,
    RuntimeEnvFailedContext,
)
from ray.core.generated.gcs_pb2 import JobsAPIInfo
from ray.dashboard.modules.job.common import (
    JobErrorType,
    JobFailureStage,
    JobInfo,
    JobInfoStorageClient,
    JobStatus,
    JobSubmitRequest,
    context_dict_from_proto,
    http_uri_components_to_uri,
    make_failure_info,
    uri_to_http_components,
    validate_request_type,
)


class TestJobSubmitRequestValidation:
    def test_validate_entrypoint(self):
        r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
        assert r.entrypoint == "abc"

        with pytest.raises(TypeError, match="required positional argument"):
            validate_request_type({}, JobSubmitRequest)

        with pytest.raises(TypeError, match="must be a string"):
            validate_request_type({"entrypoint": 123}, JobSubmitRequest)

    def test_validate_submission_id(self):
        r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
        assert r.entrypoint == "abc"
        assert r.submission_id is None

        r = validate_request_type(
            {"entrypoint": "abc", "submission_id": "123"}, JobSubmitRequest
        )
        assert r.entrypoint == "abc"
        assert r.submission_id == "123"

        with pytest.raises(TypeError, match="must be a string"):
            validate_request_type(
                {"entrypoint": 123, "submission_id": 1}, JobSubmitRequest
            )

    def test_validate_runtime_env(self):
        r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
        assert r.entrypoint == "abc"
        assert r.runtime_env is None

        r = validate_request_type(
            {"entrypoint": "abc", "runtime_env": {"hi": "hi2"}}, JobSubmitRequest
        )
        assert r.entrypoint == "abc"
        assert r.runtime_env == {"hi": "hi2"}

        with pytest.raises(TypeError, match="must be a dict"):
            validate_request_type(
                {"entrypoint": "abc", "runtime_env": 123}, JobSubmitRequest
            )

        with pytest.raises(TypeError, match="keys must be strings"):
            validate_request_type(
                {"entrypoint": "abc", "runtime_env": {1: "hi"}}, JobSubmitRequest
            )

    def test_validate_metadata(self):
        r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
        assert r.entrypoint == "abc"
        assert r.metadata is None

        r = validate_request_type(
            {"entrypoint": "abc", "metadata": {"hi": "hi2"}}, JobSubmitRequest
        )
        assert r.entrypoint == "abc"
        assert r.metadata == {"hi": "hi2"}

        with pytest.raises(TypeError, match="must be a dict"):
            validate_request_type(
                {"entrypoint": "abc", "metadata": 123}, JobSubmitRequest
            )

        with pytest.raises(TypeError, match="keys must be strings"):
            validate_request_type(
                {"entrypoint": "abc", "metadata": {1: "hi"}}, JobSubmitRequest
            )

        with pytest.raises(TypeError, match="values must be strings"):
            validate_request_type(
                {"entrypoint": "abc", "metadata": {"hi": 1}}, JobSubmitRequest
            )

    def test_validate_entrypoint_label_selector(self):
        r = validate_request_type(
            {
                "entrypoint": "abc",
                "entrypoint_label_selector": {"fragile_node": "!1"},
            },
            JobSubmitRequest,
        )
        assert r.entrypoint_label_selector == {"fragile_node": "!1"}

        with pytest.raises(TypeError, match="must be a dict"):
            validate_request_type(
                {"entrypoint": "abc", "entrypoint_label_selector": "bad"},
                JobSubmitRequest,
            )

        with pytest.raises(TypeError, match="keys must be strings"):
            validate_request_type(
                {"entrypoint": "abc", "entrypoint_label_selector": {1: "bad"}},
                JobSubmitRequest,
            )

        with pytest.raises(TypeError, match="values must be strings"):
            validate_request_type(
                {"entrypoint": "abc", "entrypoint_label_selector": {"k": 1}},
                JobSubmitRequest,
            )

    def test_entrypoint_resources_disallow_strings(self):
        with pytest.raises(TypeError, match="values must be numbers"):
            validate_request_type(
                {"entrypoint": "abc", "entrypoint_resources": {"Custom": "1"}},
                JobSubmitRequest,
            )


def test_uri_to_http_and_back():
    assert uri_to_http_components("gcs://hello.zip") == ("gcs", "hello.zip")
    assert uri_to_http_components("gcs://hello.whl") == ("gcs", "hello.whl")

    with pytest.raises(ValueError, match="'blah' is not a valid Protocol"):
        uri_to_http_components("blah://halb.zip")

    with pytest.raises(ValueError, match="does not end in .zip or .whl"):
        assert uri_to_http_components("gcs://hello.not_zip")

    with pytest.raises(ValueError, match="does not end in .zip or .whl"):
        assert uri_to_http_components("gcs://hello")

    assert http_uri_components_to_uri("gcs", "hello.zip") == "gcs://hello.zip"
    assert http_uri_components_to_uri("blah", "halb.zip") == "blah://halb.zip"
    assert http_uri_components_to_uri("blah", "halb.whl") == "blah://halb.whl"

    for original_uri in ["gcs://hello.zip", "gcs://fasdf.whl"]:
        new_uri = http_uri_components_to_uri(*uri_to_http_components(original_uri))
        assert new_uri == original_uri


def test_dynamic_status_message():
    info = JobInfo(
        status=JobStatus.PENDING, entrypoint="echo hi", entrypoint_num_cpus=1
    )
    assert "may be waiting for resources" in info.message

    info = JobInfo(
        status=JobStatus.PENDING, entrypoint="echo hi", entrypoint_num_gpus=1
    )
    assert "may be waiting for resources" in info.message

    info = JobInfo(status=JobStatus.PENDING, entrypoint="echo hi", entrypoint_memory=4)
    assert "may be waiting for resources" in info.message

    info = JobInfo(
        status=JobStatus.PENDING,
        entrypoint="echo hi",
        entrypoint_resources={"Custom": 1},
    )
    assert "may be waiting for resources" in info.message

    info = JobInfo(
        status=JobStatus.PENDING, entrypoint="echo hi", runtime_env={"conda": "env"}
    )
    assert "may be waiting for the runtime environment" in info.message


def test_job_info_to_json():
    info = JobInfo(
        status=JobStatus.PENDING,
        entrypoint="echo hi",
        entrypoint_num_cpus=1,
        entrypoint_num_gpus=1,
        entrypoint_memory=4,
        entrypoint_resources={"Custom": 1},
        runtime_env={"pip": ["pkg"]},
    )
    expected_items = {
        "status": "PENDING",
        "message": (
            "Job has not started yet. It may be waiting for resources "
            "(CPUs, GPUs, memory, custom resources) to become available. "
            "It may be waiting for the runtime environment to be set up."
        ),
        "entrypoint": "echo hi",
        "entrypoint_num_cpus": 1,
        "entrypoint_num_gpus": 1,
        "entrypoint_memory": 4,
        "entrypoint_resources": {"Custom": 1},
        "runtime_env_json": '{"pip": ["pkg"]}',
    }

    # Check that the expected items are in the JSON.
    assert expected_items.items() <= info.to_json().items()

    new_job_info = JobInfo.from_json(info.to_json())
    assert new_job_info == info

    # If `status` is just a string, then operations like status.is_terminal()
    # would fail, so we should make sure that it's a JobStatus.
    assert isinstance(new_job_info.status, JobStatus)


def test_job_info_json_to_proto():
    """Test that JobInfo JSON can be converted to JobsAPIInfo protobuf."""
    info = JobInfo(
        status=JobStatus.PENDING,
        entrypoint="echo hi",
        error_type=JobErrorType.JOB_SUPERVISOR_ACTOR_UNSCHEDULABLE,
        start_time=123,
        end_time=456,
        metadata={"hi": "hi2"},
        entrypoint_num_cpus=1,
        entrypoint_num_gpus=1,
        entrypoint_memory=4,
        entrypoint_resources={"Custom": 1},
        runtime_env={"pip": ["pkg"]},
        driver_agent_http_address="http://localhost:1234",
        driver_node_id="node_id",
        failure_info=make_failure_info(
            JobFailureStage.DRIVER_RUN,
            driver_exit_code=-9,
            context_key="driver_run",
            context={
                "error_message": "boom",
                "exception_class": "ValueError",
            },
        ),
    )
    info_json = json.dumps(info.to_json())
    info_proto = Parse(info_json, JobsAPIInfo())
    assert info_proto.status == "PENDING"
    assert info_proto.entrypoint == "echo hi"
    assert info_proto.start_time == 123
    assert info_proto.end_time == 456
    assert info_proto.metadata == {"hi": "hi2"}
    assert info_proto.entrypoint_num_cpus == 1
    assert info_proto.entrypoint_num_gpus == 1
    assert info_proto.entrypoint_memory == 4
    assert info_proto.entrypoint_resources == {"Custom": 1}
    assert info_proto.runtime_env_json == '{"pip": ["pkg"]}'
    assert info_proto.message == (
        "Job has not started yet. It may be waiting for resources "
        "(CPUs, GPUs, memory, custom resources) to become available. "
        "It may be waiting for the runtime environment to be set up."
    )
    assert info_proto.error_type == "JOB_SUPERVISOR_ACTOR_UNSCHEDULABLE"
    assert info_proto.driver_agent_http_address == "http://localhost:1234"
    assert info_proto.driver_node_id == "node_id"
    # failure_info must survive the JSON -> JobsAPIInfo hop. The GCS parses this
    # same JSON with ignore_unknown_fields=false, so a key here that the proto
    # does not know silently blanks the entire job_info record.
    assert info_proto.failure_info.stage == JobFailureInfo.Stage.DRIVER_RUN
    assert info_proto.failure_info.driver_exit_code == -9
    assert info_proto.failure_info.WhichOneof("context") == "driver_run"
    assert info_proto.failure_info.driver_run.exception_class == "ValueError"
    # Unset optionals inside the context must stay unset, not default-false.
    assert not info_proto.failure_info.driver_run.HasField("failed_to_start")

    minimal_info = JobInfo(status=JobStatus.PENDING, entrypoint="echo hi")
    minimal_info_json = json.dumps(minimal_info.to_json())
    minimal_info_proto = Parse(minimal_info_json, JobsAPIInfo())
    assert minimal_info_proto.status == "PENDING"
    assert minimal_info_proto.entrypoint == "echo hi"
    for unset_optional_field in [
        "entrypoint_num_cpus",
        "entrypoint_num_gpus",
        "entrypoint_memory",
        "runtime_env_json",
        "error_type",
        "driver_agent_http_address",
        "driver_node_id",
        "failure_info",
    ]:
        assert not minimal_info_proto.HasField(unset_optional_field)


def _parse_failure_info(failure_info):
    """Round-trip a failure_info dict through the hop the GCS performs.

    The GCS parses the job record with ignore_unknown_fields at its default of
    false, so a key the proto does not know does not get dropped: the parse fails
    and the job's entire job_info record is left empty. Every context key
    therefore needs a case here, and the assertions have to reach the leaves --
    the dict is written on one side of this hop and read on the other, with
    nothing in between to notice a mismatch.
    """
    info = JobInfo(
        status=JobStatus.FAILED,
        entrypoint="echo hi",
        failure_info=failure_info,
    )
    return Parse(json.dumps(info.to_json()), JobsAPIInfo()).failure_info


def test_runtime_env_failure_info_json_to_proto():
    setup_failure = RuntimeEnvFailedContext(
        error_message="the agent's own message",
        plugin="pip",
        phase="install",
        installer_exit_code=1,
    )
    setup_failure.attempts.add(attempt=1, exit_code=1, duration_ms=123)
    # Built from the proto rather than by hand, which is what job_manager does
    # and the only way the keys are guaranteed to be ones the proto knows.
    context = context_dict_from_proto(setup_failure)
    context["error_message"] = "Failed to set up runtime environment."

    failure_info = _parse_failure_info(
        make_failure_info(
            JobFailureStage.RUNTIME_ENV_SETUP,
            context_key="runtime_env",
            context=context,
        )
    )
    assert failure_info.stage == JobFailureInfo.Stage.RUNTIME_ENV_SETUP
    assert failure_info.WhichOneof("context") == "runtime_env"
    assert failure_info.runtime_env.plugin == "pip"
    assert failure_info.runtime_env.phase == "install"
    assert failure_info.runtime_env.installer_exit_code == 1
    assert (
        failure_info.runtime_env.error_message
        == "Failed to set up runtime environment."
    )
    # A uint64 is a string in protobuf JSON, so the dict carries "123". It parses
    # back to an int; do not "clean up" the dict builder to emit one.
    assert context["attempts"][0]["duration_ms"] == "123"
    assert len(failure_info.runtime_env.attempts) == 1
    assert failure_info.runtime_env.attempts[0].attempt == 1
    assert failure_info.runtime_env.attempts[0].duration_ms == 123
    # An installer that never ran is a different claim from one that exited 0.
    assert not failure_info.runtime_env.HasField("failed_package")


def test_supervisor_failure_info_json_to_proto():
    died = ActorDiedErrorContext(
        error_message="The actor died unexpectedly before finishing this task.",
        reason=ActorDiedErrorContext.NODE_DIED,
        actor_id=b"\x05\x06",
    )
    failure_info = _parse_failure_info(
        make_failure_info(
            JobFailureStage.SUPERVISOR_START,
            context_key="supervisor",
            context={
                "error_message": "The actor died unexpectedly.",
                "exception_class": "ActorDiedError",
                # The death cause is one branch of an ActorDeathCause, so it has
                # to be written inside that branch: ActorDiedErrorContext's own
                # field names are not ActorDeathCause's.
                "death_cause": {
                    "actor_died_error_context": context_dict_from_proto(died)
                },
            },
        )
    )
    assert failure_info.stage == JobFailureInfo.Stage.SUPERVISOR_START
    assert failure_info.WhichOneof("context") == "supervisor"
    assert failure_info.supervisor.exception_class == "ActorDiedError"
    assert (
        failure_info.supervisor.death_cause.WhichOneof("context")
        == "actor_died_error_context"
    )
    assert (
        failure_info.supervisor.death_cause.actor_died_error_context.reason
        == ActorDiedErrorContext.NODE_DIED
    )


def test_infra_cause_failure_info_json_to_proto():
    infra_cause = InfraCauseContext(
        error_type=ErrorType.NODE_DIED,
        error_message="node died",
        ray_job_id=b"\x64\x00\x00\x00",
    )
    infra_cause.sample_task_ids.append("task_id_1")
    failure_info = _parse_failure_info(
        make_failure_info(
            JobFailureStage.DRIVER_RUN,
            driver_exit_code=1,
            context_key="driver_run",
            context={"error_message": "driver exited with code 1"},
            log_excerpt_ref="node_id:job-driver-raysubmit_1.log",
            infra_cause=context_dict_from_proto(infra_cause),
        )
    )
    # infra_cause annotates the stage rather than replacing it: the driver really
    # did exit non-zero, so driver_run stays set alongside it.
    assert failure_info.stage == JobFailureInfo.Stage.DRIVER_RUN
    assert failure_info.WhichOneof("context") == "driver_run"
    # A reference rather than the tail itself: the driver log is unbounded and
    # this field is served by the state and export APIs.
    assert failure_info.log_excerpt_ref == "node_id:job-driver-raysubmit_1.log"
    assert failure_info.HasField("infra_cause")
    assert failure_info.infra_cause.error_type == ErrorType.NODE_DIED
    assert failure_info.infra_cause.ray_job_id == b"\x64\x00\x00\x00"
    assert list(failure_info.infra_cause.sample_task_ids) == ["task_id_1"]


def test_get_all_jobs_filters_out_none_job_info():
    prefix = JobInfoStorageClient.JOB_DATA_KEY_PREFIX
    mock_gcs = MagicMock()
    mock_gcs.async_internal_kv_keys = AsyncMock(
        return_value=[
            (prefix + "job1").encode(),
            (prefix + "job2").encode(),
        ]
    )

    storage = JobInfoStorageClient(mock_gcs)
    job_info_1 = JobInfo(status=JobStatus.RUNNING, entrypoint="echo 1")

    async def mock_get_info(job_id, timeout=30):
        if job_id == "job1":
            return job_info_1
        return None

    storage.get_info = mock_get_info

    result = asyncio.run(storage.get_all_jobs())

    assert result == {"job1": job_info_1}
    for job_id, job_info in result.items():
        asdict(job_info)  # This should not raise an exception


def test_job_supervisor_actor_class_is_serializable():
    """JobSupervisor must survive cloudpickle, or no job can start at all.

    `ray.remote` rebinds the class as `_modify_class.<locals>.Class`. Because
    that is defined in a local scope, cloudpickle serialises it BY VALUE, which
    means walking the globals referenced by every method. Anything unpicklable
    reachable that way breaks job submission entirely -- not the feature that
    introduced it, every job.

    Known-unpicklable things reachable that way include protobuf enum symbols
    (`TaskStatus`, `ErrorType`, `FilterPredicate`), which are EnumTypeWrapper
    instances rather than classes:

        TypeError: cannot pickle 'google._upb._message.EnumDescriptor' object

    Rather than police the list, the infra-attribution code lives at module
    scope. A module-level function is pickled BY REFERENCE, so its globals are
    never walked, and it can use protobuf and _raylet symbols freely. Anything
    moved back onto the class re-opens the hole.

    Nothing else in the suite catches this: the module imports cleanly and every
    unit test passes with an unpicklable global in place, because it only fails
    when Ray tries to ship the actor -- at which point no job can start.
    """
    import ray
    from ray import cloudpickle
    from ray.dashboard.modules.job.job_supervisor import JobSupervisor

    cloudpickle.dumps(ray.remote(JobSupervisor))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
