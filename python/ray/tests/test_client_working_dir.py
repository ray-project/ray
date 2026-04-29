"""Integration tests for Ray Client working_dir validation with _client_job flag.

When connecting via Ray Client with a working_dir in RuntimeEnv, the server-side
job_config may temporarily contain local extracted paths during the proxy flow.
The _client_job flag signals that validation should be skipped since the
working_dir was already validated and uploaded on the client side.
"""
import pickle
import tempfile

import pytest

from ray.job_config import JobConfig

# Parametrized validation tests


@pytest.mark.parametrize(
    "runtime_env_key,use_list,client_job,should_raise",
    [
        ("working_dir", False, False, True),
        ("working_dir", False, True, False),
        ("py_modules", True, False, True),
        ("py_modules", True, True, False),
    ],
    ids=[
        "working_dir_without_client_job_fails",
        "working_dir_with_client_job_succeeds",
        "py_modules_without_client_job_fails",
        "py_modules_with_client_job_succeeds",
    ],
)
def test_local_path_validation(runtime_env_key, use_list, client_job, should_raise):
    """Test that _client_job flag controls local path validation."""
    with tempfile.TemporaryDirectory(prefix="_ray_pkg_") as tmpdir:
        value = [tmpdir] if use_list else tmpdir
        jc = JobConfig(runtime_env={runtime_env_key: value}, _client_job=client_job)

        if should_raise:
            with pytest.raises(ValueError, match="not a valid URI"):
                jc._serialize()
        else:
            jc._serialize()  # Should succeed


@pytest.mark.parametrize(
    "runtime_env_key,use_list,client_job",
    [
        ("working_dir", False, False),
        ("working_dir", False, True),
        ("py_modules", True, False),
        ("py_modules", True, True),
    ],
    ids=[
        "working_dir_without_client_job",
        "working_dir_with_client_job",
        "py_modules_without_client_job",
        "py_modules_with_client_job",
    ],
)
def test_gcs_uri_always_valid(runtime_env_key, use_list, client_job):
    """Test that GCS URIs work with or without _client_job flag."""
    uri = "gcs://_ray_pkg_abc.zip"
    value = [uri] if use_list else uri
    jc = JobConfig(runtime_env={runtime_env_key: value}, _client_job=client_job)
    jc._serialize()  # Should always succeed


def test_multiple_local_py_modules_with_client_job():
    """Verify multiple local py_modules paths pass with _client_job=True."""
    with tempfile.TemporaryDirectory(
        prefix="_ray_pkg_a"
    ) as tmpdir_a, tempfile.TemporaryDirectory(prefix="_ray_pkg_b") as tmpdir_b:
        jc = JobConfig(
            runtime_env={"py_modules": [tmpdir_a, tmpdir_b]},
            _client_job=True,
        )
        jc._serialize()


@pytest.mark.parametrize(
    "client_job,should_raise",
    [(True, False), (False, True)],
    ids=["with_client_job_succeeds", "without_client_job_fails"],
)
def test_mixed_working_dir_and_py_modules(client_job, should_raise):
    """Test both working_dir and py_modules together with _client_job flag."""
    with tempfile.TemporaryDirectory(
        prefix="_ray_pkg_wd"
    ) as wd_dir, tempfile.TemporaryDirectory(prefix="_ray_pkg_pm") as pm_dir:
        jc = JobConfig(
            runtime_env={"working_dir": wd_dir, "py_modules": [pm_dir]},
            _client_job=client_job,
        )

        if should_raise:
            with pytest.raises(ValueError, match="not a valid URI"):
                jc._serialize()
        else:
            jc._serialize()


# Pickle roundtrip tests


def test_pickle_roundtrip_preserves_working_dir():
    """Verify working_dir survives pickle/unpickle."""
    with tempfile.TemporaryDirectory(prefix="_ray_pkg_") as tmpdir:
        jc = JobConfig(runtime_env={"working_dir": tmpdir})
        pickled = pickle.dumps(jc)
        unpickled = pickle.loads(pickled)
        assert unpickled.runtime_env["working_dir"] == tmpdir


def test_pickle_roundtrip_preserves_client_job_flag():
    """Verify _client_job flag survives pickle/unpickle."""
    jc = JobConfig(_client_job=True)
    pickled = pickle.dumps(jc)
    unpickled = pickle.loads(pickled)
    assert unpickled._client_job is True


def test_double_pickle_roundtrip_simulates_proxy_flow():
    """Simulate the full proxy flow: client -> proxy -> server."""
    with tempfile.TemporaryDirectory(prefix="_ray_pkg_") as tmpdir:
        # Client creates job_config with working_dir
        client_jc = JobConfig(runtime_env={"working_dir": tmpdir})
        assert client_jc._client_job is False

        # Serialize for transmission to proxy
        client_serialized = pickle.dumps(client_jc)

        # Proxy unpickles
        proxy_jc = pickle.loads(client_serialized)
        assert proxy_jc._client_job is False

        # Proxy re-pickles for forwarding to specific server
        proxy_serialized = pickle.dumps(proxy_jc)

        # Specific server unpickles and sets _client_job=True
        server_jc = pickle.loads(proxy_serialized)
        assert server_jc._client_job is False
        server_jc._client_job = True

        # Server calls _serialize() which triggers validation
        server_jc._serialize()  # Should succeed


def test_py_modules_pickle_roundtrip_with_client_job():
    """Verify py_modules + _client_job survives the proxy pickle roundtrip."""
    with tempfile.TemporaryDirectory(prefix="_ray_pkg_") as tmpdir:
        # Client creates job_config with local py_modules
        client_jc = JobConfig(runtime_env={"py_modules": [tmpdir]})
        client_serialized = pickle.dumps(client_jc)

        # Proxy unpickles and re-pickles
        proxy_jc = pickle.loads(client_serialized)
        proxy_serialized = pickle.dumps(proxy_jc)

        # Server unpickles and sets _client_job=True
        server_jc = pickle.loads(proxy_serialized)
        assert server_jc._client_job is False
        server_jc._client_job = True
        server_jc._serialize()  # Should succeed


# Server Init() code path tests


@pytest.mark.parametrize(
    "runtime_env,client_job,should_raise",
    [
        ({"working_dir": "local_path"}, True, False),
        ({"working_dir": "local_path"}, False, True),
        ({"working_dir": "local_path", "env_vars": {"TEST": "value"}}, True, False),
    ],
    ids=[
        "working_dir_with_client_job",
        "working_dir_without_client_job_fails",
        "working_dir_and_env_vars_with_client_job",
    ],
)
def test_server_init_code_path(runtime_env, client_job, should_raise):
    """Simulate server.py Init() with various runtime_env configurations."""
    # Replace placeholder with actual temp directory
    if "working_dir" in runtime_env and runtime_env["working_dir"] == "local_path":
        with tempfile.TemporaryDirectory(prefix="_ray_pkg_") as tmpdir:
            runtime_env = runtime_env.copy()
            runtime_env["working_dir"] = tmpdir

            # Client creates job_config
            client_jc = JobConfig(runtime_env=runtime_env)
            serialized = pickle.dumps(client_jc)

            # Server receives and unpickles
            server_jc = pickle.loads(serialized)

            if client_job:
                server_jc._client_job = True

            if should_raise:
                with pytest.raises(ValueError, match="not a valid URI"):
                    server_jc._serialize()
            else:
                server_jc._serialize()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
