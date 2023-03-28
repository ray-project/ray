import pytest
import json

from ray.dashboard.modules.job.common import (
    JobInfo,
    JobStatus,
    http_uri_components_to_uri,
    uri_to_http_components,
    validate_request_type,
    JobSubmitRequest,
)

from ray.core.generated.gcs_pb2 import JobsAPIInfo
from google.protobuf.json_format import Parse


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
        entrypoint_resources={"Custom": 1},
        runtime_env={"pip": ["pkg"]},
    )
    expected_items = {
        "status": "PENDING",
        "message": (
            "Job has not started yet. It may be waiting for resources "
            "(CPUs, GPUs, custom resources) to become available. "
            "It may be waiting for the runtime environment to be set up."
        ),
        "entrypoint": "echo hi",
        "entrypoint_num_cpus": 1,
        "entrypoint_num_gpus": 1,
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
        error_type="error_type",
        start_time=123,
        end_time=456,
        metadata={"hi": "hi2"},
        entrypoint_num_cpus=1,
        entrypoint_num_gpus=1,
        entrypoint_resources={"Custom": 1},
        runtime_env={"pip": ["pkg"]},
        driver_agent_http_address="http://localhost:1234",
        driver_node_id="node_id",
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
    assert info_proto.entrypoint_resources == {"Custom": 1}
    assert info_proto.runtime_env_json == '{"pip": ["pkg"]}'
    assert info_proto.message == (
        "Job has not started yet. It may be waiting for resources "
        "(CPUs, GPUs, custom resources) to become available. "
        "It may be waiting for the runtime environment to be set up."
    )
    assert info_proto.error_type == "error_type"
    assert info_proto.driver_agent_http_address == "http://localhost:1234"
    assert info_proto.driver_node_id == "node_id"

    minimal_info = JobInfo(status=JobStatus.PENDING, entrypoint="echo hi")
    minimal_info_json = json.dumps(minimal_info.to_json())
    minimal_info_proto = Parse(minimal_info_json, JobsAPIInfo())
    assert minimal_info_proto.status == "PENDING"
    assert minimal_info_proto.entrypoint == "echo hi"
    for unset_optional_field in [
        "entrypoint_num_cpus",
        "entrypoint_num_gpus",
        "runtime_env_json",
        "error_type",
        "driver_agent_http_address",
        "driver_node_id",
    ]:
        assert not minimal_info_proto.HasField(unset_optional_field)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
