import pytest

from ray.dashboard.modules.job.common import (
    JobSubmitRequest,
    validate_request_type,
)


def test_validate_entrypoint():
    r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
    assert r.entrypoint == "abc"

    with pytest.raises(TypeError, match="required positional argument"):
        validate_request_type({}, JobSubmitRequest)

    with pytest.raises(TypeError, match="must be a string"):
        validate_request_type({"entrypoint": 123}, JobSubmitRequest)


def test_validate_job_id():
    r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
    assert r.entrypoint == "abc"
    assert r.job_id is None

    r = validate_request_type({
        "entrypoint": "abc",
        "job_id": "123"
    }, JobSubmitRequest)
    assert r.entrypoint == "abc"
    assert r.job_id == "123"

    with pytest.raises(TypeError, match="must be a string"):
        validate_request_type({
            "entrypoint": 123,
            "job_id": 1
        }, JobSubmitRequest)


def test_validate_runtime_env():
    r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
    assert r.entrypoint == "abc"
    assert r.runtime_env is None

    r = validate_request_type({
        "entrypoint": "abc",
        "runtime_env": {
            "hi": "hi2"
        }
    }, JobSubmitRequest)
    assert r.entrypoint == "abc"
    assert r.runtime_env == {"hi": "hi2"}

    with pytest.raises(TypeError, match="must be a dict"):
        validate_request_type({
            "entrypoint": "abc",
            "runtime_env": 123
        }, JobSubmitRequest)

    with pytest.raises(TypeError, match="keys must be strings"):
        validate_request_type({
            "entrypoint": "abc",
            "runtime_env": {
                1: "hi"
            }
        }, JobSubmitRequest)


def test_validate_metadata():
    r = validate_request_type({"entrypoint": "abc"}, JobSubmitRequest)
    assert r.entrypoint == "abc"
    assert r.metadata is None

    r = validate_request_type({
        "entrypoint": "abc",
        "metadata": {
            "hi": "hi2"
        }
    }, JobSubmitRequest)
    assert r.entrypoint == "abc"
    assert r.metadata == {"hi": "hi2"}

    with pytest.raises(TypeError, match="must be a dict"):
        validate_request_type({
            "entrypoint": "abc",
            "metadata": 123
        }, JobSubmitRequest)

    with pytest.raises(TypeError, match="keys must be strings"):
        validate_request_type({
            "entrypoint": "abc",
            "metadata": {
                1: "hi"
            }
        }, JobSubmitRequest)

    with pytest.raises(TypeError, match="values must be strings"):
        validate_request_type({
            "entrypoint": "abc",
            "metadata": {
                "hi": 1
            }
        }, JobSubmitRequest)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
