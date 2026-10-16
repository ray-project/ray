import logging
import sys
from typing import Optional, Sequence

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import _modify_context_impl

TOKEN = "secret-token-value"


@pytest.fixture
def auth_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    # Path.home() reads USERPROFILE on Windows and ignores HOME.
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    for var in ("RAY_AUTH_MODE", "RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH"):
        monkeypatch.delenv(var, raising=False)
    yield monkeypatch
    monkeypatch.undo()
    reset_auth_token_state()


def _container_command(
    context: Optional[RuntimeEnvContext] = None,
    runtime_env_dir: Optional[str] = None,
    working_dir_uri: Optional[str] = None,
    py_modules_uris: Sequence[str] = (),
) -> str:
    context = context or RuntimeEnvContext()
    _modify_context_impl(
        "fake-image",
        "/fake/default_worker.py",
        [],
        context,
        logging.getLogger(__name__),
        "/tmp/ray",
        runtime_env_dir,
        working_dir_uri,
        py_modules_uris,
    )
    return context.py_executable


def test_mounts_default_token_file(auth_env, tmp_path):
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(TOKEN)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()

    command = _container_command()

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH='{token_path}'" in command
    assert TOKEN not in command


@pytest.mark.parametrize("relative", [False, True])
def test_mounts_token_path_from_env(auth_env, tmp_path, relative):
    token_path = tmp_path / "custom_token"
    token_path.write_text(TOKEN)
    auth_env.chdir(tmp_path)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    auth_env.setenv(
        "RAY_AUTH_TOKEN_PATH", token_path.name if relative else str(token_path)
    )
    reset_auth_token_state()

    command = _container_command()

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH='{token_path}'" in command
    assert TOKEN not in command


def test_no_mount_when_token_passed_by_env(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "token")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    reset_auth_token_state()

    assert ":ro" not in _container_command()


def test_no_mount_when_auth_disabled(auth_env, tmp_path):
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(TOKEN)
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()

    command = _container_command()

    assert ":ro" not in command
    assert "RAY_AUTH_TOKEN_PATH" not in command


def test_mounts_session_dir_and_masks_runtime_resources(auth_env):
    command = _container_command(
        runtime_env_dir="/tmp/ray/session_1/runtime_resources"
    ).split()

    assert "/tmp/ray:/tmp/ray" not in command
    assert command[2:6] == [
        "-v",
        "/tmp/ray/session_1:/tmp/ray/session_1",
        "--tmpfs",
        "/tmp/ray/session_1/runtime_resources:notmpcopyup",
    ]


def test_mounts_only_own_inherited_resources_read_only(auth_env, tmp_path):
    runtime_env_dir = tmp_path / "session_1" / "runtime_resources"
    working_dir = runtime_env_dir / "working_dir_files" / "_ray_pkg_own"
    py_module = runtime_env_dir / "py_modules_files" / "_ray_pkg_mod"
    other_job_dir = runtime_env_dir / "working_dir_files" / "_ray_pkg_other"
    for path in (working_dir, py_module, other_job_dir):
        path.mkdir(parents=True)
    # The job controls PYTHONPATH through env_vars, so it must not pick mounts.
    context = RuntimeEnvContext(env_vars={"PYTHONPATH": str(other_job_dir)})

    command = _container_command(
        context,
        str(runtime_env_dir),
        "gcs://_ray_pkg_own.zip",
        ["gcs://_ray_pkg_mod.zip", "gcs://_ray_pkg_missing.zip", "local:///opt/mod"],
    )

    assert f"-v {working_dir}:{working_dir}:ro" in command
    assert f"-v {py_module}:{py_module}:ro" in command
    assert f"{other_job_dir}:{other_job_dir}" not in command
    assert "_ray_pkg_missing" not in command
    assert "/opt/mod:" not in command


def test_quotes_mount_paths_for_the_shell(auth_env, tmp_path):
    runtime_env_dir = tmp_path / "ray dir" / "session_1" / "runtime_resources"
    working_dir = runtime_env_dir / "working_dir_files" / "_ray_pkg_a;b"
    working_dir.mkdir(parents=True)
    session_dir = runtime_env_dir.parent

    command = _container_command(
        runtime_env_dir=str(runtime_env_dir), working_dir_uri="gcs://_ray_pkg_a;b.zip"
    )

    assert f"-v '{session_dir}:{session_dir}'" in command
    assert f"--tmpfs '{runtime_env_dir}:notmpcopyup'" in command
    assert f"-v '{working_dir}:{working_dir}:ro'" in command


def test_log_redacts_env_var_values(auth_env, caplog):
    auth_env.setenv("RAY_SECRET_FROM_HOST", "host-secret")
    context = RuntimeEnvContext(env_vars={"API_KEY": "user-secret"})

    with caplog.at_level(logging.INFO):
        command = _container_command(context)

    # The real command still carries the values; only the log is masked.
    assert "--env API_KEY='user-secret'" in command
    assert "--env RAY_SECRET_FROM_HOST='host-secret'" in command
    assert "--env API_KEY='<redacted>'" in caplog.text
    assert "--env RAY_JOB_ID=$RAY_JOB_ID" in caplog.text
    assert "--entrypoint python fake-image" in caplog.text
    assert "user-secret" not in caplog.text
    assert "host-secret" not in caplog.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
