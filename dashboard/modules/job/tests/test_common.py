import pytest

from ray.dashboard.modules.job.common import (
    http_uri_components_to_uri,
    uri_to_http_components,
    validate_request_type,
    JobSubmitRequest,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
