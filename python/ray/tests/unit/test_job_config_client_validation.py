"""Unit tests for JobConfig._validate_runtime_env with _client_job flag.

Tests that when _client_job=True, local path validation is skipped in
_validate_runtime_env(). This is necessary because Ray Client jobs upload
the working_dir to GCS on the client side, so the server-side JobConfig
may contain paths that look local but are actually already uploaded.
"""
import pickle
import tempfile
from unittest.mock import patch

import pytest

from ray.job_config import JobConfig


@pytest.mark.parametrize(
    "runtime_env_key,path_type,client_job,should_raise",
    [
        ("working_dir", "local", True, False),
        ("working_dir", "local", False, True),
        ("working_dir", "gcs", True, False),
        ("working_dir", "gcs", False, False),
        ("py_modules", "local", True, False),
        ("py_modules", "local", False, True),
    ],
    ids=[
        "working_dir_local_with_client_job",
        "working_dir_local_without_client_job_fails",
        "working_dir_gcs_with_client_job",
        "working_dir_gcs_without_client_job",
        "py_modules_local_with_client_job",
        "py_modules_local_without_client_job_fails",
    ],
)
def test_path_validation(runtime_env_key, path_type, client_job, should_raise):
    """Test that _client_job flag controls local path validation."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        if path_type == "gcs":
            path_value = "gcs://_ray_pkg_abc123.zip"
        else:
            path_value = tmp_dir

        # py_modules needs to be a list
        if runtime_env_key == "py_modules":
            runtime_env = {runtime_env_key: [path_value]}
        else:
            runtime_env = {runtime_env_key: path_value}

        config = JobConfig(runtime_env=runtime_env, _client_job=client_job)

        if should_raise:
            with pytest.raises(ValueError, match="not a valid URI"):
                config._validate_runtime_env()
        else:
            result = config._validate_runtime_env()
            assert result is not None


@pytest.mark.parametrize(
    "method,value,expected",
    [
        ("constructor", None, False),
        ("constructor", True, True),
        ("constructor", False, False),
        ("from_json", True, True),
        ("from_json", False, False),
        ("from_json", None, False),
    ],
    ids=[
        "default_is_false",
        "constructor_set_true",
        "constructor_set_false",
        "from_json_with_true",
        "from_json_with_false",
        "from_json_defaults_false",
    ],
)
def test_client_job_flag_initialization(method, value, expected):
    """Test _client_job flag initialization via constructor and from_json."""
    if method == "constructor":
        if value is None:
            config = JobConfig()
        else:
            config = JobConfig(_client_job=value)
    else:  # from_json
        if value is None:
            config = JobConfig.from_json({})
        else:
            config = JobConfig.from_json({"client_job": value})

    assert config._client_job is expected


def test_validate_no_local_paths_not_called_for_client_job():
    """Verify _validate_no_local_paths is not called when _client_job=True."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = JobConfig(
            runtime_env={"working_dir": tmp_dir},
            _client_job=True,
        )
        with patch(
            "ray.runtime_env.runtime_env._validate_no_local_paths"
        ) as mock_validate:
            config._validate_runtime_env()
            mock_validate.assert_not_called()


def test_validate_no_local_paths_called_for_non_client_job():
    """Verify _validate_no_local_paths is called when _client_job=False."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = JobConfig(
            runtime_env={"working_dir": tmp_dir},
            _client_job=False,
        )
        with patch(
            "ray.runtime_env.runtime_env._validate_no_local_paths"
        ) as mock_validate:
            mock_validate.return_value = None
            try:
                config._validate_runtime_env()
            except ValueError:
                pass  # Expected to fail
            mock_validate.assert_called_once()


def test_extracted_temp_path_scenario():
    """Simulate the actual bug: proxy extracts GCS package to temp path.

    The proxy downloads gcs://_ray_pkg_xxx.zip and extracts it to
    /tmp/ray/.../working_dir_files/_ray_pkg_xxx. The server-side job_config
    then has this local path with _ray_pkg_ prefix, which would fail
    validation without the _client_job flag.
    """
    with tempfile.TemporaryDirectory(prefix="_ray_pkg_") as tmp_dir:
        config = JobConfig(
            runtime_env={"working_dir": tmp_dir},
            _client_job=True,
        )
        config._validate_runtime_env()  # Should not raise


@pytest.mark.parametrize(
    "client_job,has_local_path,should_raise",
    [
        (True, False, False),
        (False, False, False),
        (True, True, False),
        (False, True, True),
    ],
    ids=[
        "client_job_flag_survives_pickle",
        "non_client_job_flag_survives_pickle",
        "pickled_client_job_skips_validation",
        "pickled_non_client_job_validates",
    ],
)
def test_pickle_roundtrip(client_job, has_local_path, should_raise):
    """Test that _client_job flag survives pickle and validation is preserved."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        if has_local_path:
            config = JobConfig(
                runtime_env={"working_dir": tmp_dir},
                _client_job=client_job,
            )
        else:
            config = JobConfig(_client_job=client_job)

        pickled = pickle.dumps(config)
        unpickled = pickle.loads(pickled)

        # Verify flag survives pickle
        assert unpickled._client_job is client_job

        # Test validation if runtime_env present
        if has_local_path:
            if should_raise:
                with pytest.raises(ValueError, match="not a valid URI"):
                    unpickled._validate_runtime_env()
            else:
                unpickled._validate_runtime_env()


@pytest.mark.parametrize(
    "client_job,path_type,should_raise",
    [
        (True, "gcs", False),
        (False, "gcs", False),
        (True, "local", False),
        (False, "local", True),
    ],
    ids=[
        "serialize_client_job_gcs_uri",
        "serialize_non_client_job_gcs_uri",
        "serialize_client_job_local_path",
        "serialize_non_client_job_local_path_fails",
    ],
)
def test_serialize_crash_path(client_job, path_type, should_raise):
    """Test _serialize() handles paths correctly based on _client_job flag.

    This tests the actual crash path from the bug report. Without the fix,
    _serialize() would raise "not a valid URI" for client jobs with local paths.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        if path_type == "gcs":
            path_value = "gcs://_ray_pkg_abc.zip"
        else:
            path_value = tmp_dir

        config = JobConfig(
            runtime_env={"working_dir": path_value},
            _client_job=client_job,
        )

        if should_raise:
            with pytest.raises(ValueError, match="not a valid URI"):
                config._serialize()
        else:
            serialized = config._serialize()
            assert serialized is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
