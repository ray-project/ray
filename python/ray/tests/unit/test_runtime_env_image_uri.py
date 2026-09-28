import logging
import os
import shlex
import subprocess
import sys

import pytest

from ray._private.runtime_env.agent.runtime_env_agent import (
    _create_image_uri_plugin,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import ImageURIPlugin, _modify_context_impl


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
