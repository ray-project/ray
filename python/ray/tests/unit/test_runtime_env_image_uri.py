import logging
import os
import shlex
import subprocess
import sys

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.agent.runtime_env_agent import (
    _create_image_uri_plugin,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import ImageURIPlugin, _modify_context_impl

TOKEN = "secret-token-value"


class LegacyImageURIPlugin(ImageURIPlugin):
    def __init__(self, ray_tmp_dir: str):
        super().__init__(ray_tmp_dir)


class KwargsImageURIPlugin(ImageURIPlugin):
    def __init__(self, ray_tmp_dir: str, **kwargs):
        super().__init__(ray_tmp_dir, **kwargs)


class FailingImageURIPlugin(ImageURIPlugin):
    def __init__(self, ray_tmp_dir: str, logs_dir: str):
        raise TypeError("plugin constructor failed")


def test_custom_logs_dir_is_mounted_in_container(tmp_path):
    temp_dir = str(tmp_path.resolve() / "ray")
    logs_dir = str(tmp_path.resolve() / "logs")
    context = RuntimeEnvContext()
    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        temp_dir,
        logs_dir,
    )

    arguments = shlex.split(context.py_executable)
    assert f"{temp_dir}:{temp_dir}" in arguments
    assert f"{logs_dir}:{logs_dir}" in arguments


def test_logs_dir_under_temp_dir_is_not_mounted_twice(tmp_path):
    temp_dir = str(tmp_path.resolve() / "ray")
    context = RuntimeEnvContext()
    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        temp_dir,
        os.path.join(temp_dir, "session", "logs"),
    )

    assert context.py_executable.count("-v ") == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
@pytest.mark.parametrize("relative_link", [False, True])
def test_logs_symlink_target_is_mounted_in_container(tmp_path, relative_link):
    temp_dir = tmp_path.resolve() / "ray"
    temp_dir.mkdir()
    logs_dir = tmp_path.resolve() / "logs"
    logs_dir.mkdir()
    intermediate = tmp_path.resolve() / "intermediate"
    intermediate.symlink_to(logs_dir, target_is_directory=True)
    alias = temp_dir / "logs-alias"
    alias.symlink_to(
        "../intermediate" if relative_link else intermediate,
        target_is_directory=True,
    )
    context = RuntimeEnvContext()

    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        str(temp_dir),
        str(alias),
    )

    arguments = shlex.split(context.py_executable)
    assert f"{logs_dir}:{logs_dir}" in arguments


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_temp_dir_alias_does_not_hide_required_logs_mount(tmp_path):
    physical_dir = tmp_path.resolve() / "physical"
    logs_dir = physical_dir / "session" / "logs"
    logs_dir.mkdir(parents=True)
    temp_dir = tmp_path.resolve() / "alias"
    temp_dir.symlink_to(physical_dir, target_is_directory=True)
    context = RuntimeEnvContext()

    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        str(temp_dir),
        str(logs_dir),
    )

    arguments = shlex.split(context.py_executable)
    assert f"{temp_dir}:{temp_dir}" in arguments
    assert f"{logs_dir}:{logs_dir}" in arguments


@pytest.mark.skipif(sys.platform == "win32", reason="Requires bash")
@pytest.mark.parametrize(
    "directory_name", ["logs with spaces", "logs'quotes", "logs$RAY_JOB_ID"]
)
def test_container_volume_paths_are_shell_quoted(tmp_path, directory_name, monkeypatch):
    monkeypatch.delenv("RAY_JOB_ID", raising=False)
    temp_dir = str(tmp_path.resolve() / "ray tmp")
    logs_dir = str(tmp_path.resolve() / directory_name)
    context = RuntimeEnvContext()
    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        temp_dir,
        logs_dir,
    )

    # Capture what the worker's shell passes to Podman, including late expansion
    # of RAY_JOB_ID, without starting a container in this unit test.
    capture_command = 'podman() { printf "%s\\0" "$@"; }\n' + context.py_executable
    arguments = subprocess.check_output(
        ["bash", "-c", capture_command],
        env={**os.environ, "RAY_JOB_ID": "worker-job"},
        text=True,
    ).split("\0")

    assert f"{temp_dir}:{temp_dir}" in arguments
    assert f"{logs_dir}:{logs_dir}" in arguments
    assert "RAY_JOB_ID=worker-job" in arguments


def test_image_uri_plugin_receives_logs_dir():
    plugin = _create_image_uri_plugin(
        ImageURIPlugin,
        "/tmp/ray",
        "/var/log/ray",
        logging.getLogger(__name__),
    )

    assert plugin._logs_dir == "/var/log/ray"


def test_image_uri_plugin_accepting_kwargs_receives_logs_dir():
    plugin = _create_image_uri_plugin(
        KwargsImageURIPlugin,
        "/tmp/ray",
        "/var/log/ray",
        logging.getLogger(__name__),
    )

    assert plugin._logs_dir == "/var/log/ray"


def test_legacy_image_uri_plugin_remains_compatible(caplog):
    with caplog.at_level(logging.WARNING):
        plugin = _create_image_uri_plugin(
            LegacyImageURIPlugin,
            "/tmp/ray",
            "/var/log/ray",
            logging.getLogger(__name__),
        )

    assert plugin._ray_tmp_dir == "/tmp/ray"
    assert plugin._logs_dir is None
    assert "does not accept the logs_dir keyword" in caplog.text


def test_image_uri_plugin_constructor_type_error_is_not_hidden():
    with pytest.raises(TypeError, match="plugin constructor failed"):
        _create_image_uri_plugin(
            FailingImageURIPlugin,
            "/tmp/ray",
            "/var/log/ray",
            logging.getLogger(__name__),
        )


@pytest.fixture
def auth_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    for var in ("RAY_AUTH_MODE", "RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH"):
        monkeypatch.delenv(var, raising=False)
    yield monkeypatch
    monkeypatch.undo()
    reset_auth_token_state()


def _container_command(logs_dir=None) -> str:
    context = RuntimeEnvContext()
    _modify_context_impl(
        "fake-image",
        "/fake/default_worker.py",
        [],
        context,
        logging.getLogger(__name__),
        "/tmp/ray",
        logs_dir=logs_dir,
    )
    return context.py_executable


@pytest.mark.parametrize("external_logs", [False, True])
def test_mounts_default_token_file(auth_env, tmp_path, external_logs):
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(TOKEN)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()

    logs_dir = str(tmp_path.resolve() / "logs") if external_logs else None
    command = _container_command(logs_dir=logs_dir)

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH='{token_path}'" in command
    assert TOKEN not in command
    if logs_dir is not None:
        assert f"{logs_dir}:{logs_dir}" in shlex.split(command)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
