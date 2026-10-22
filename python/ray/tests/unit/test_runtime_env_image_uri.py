import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Optional, Tuple
from unittest import mock

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.agent.runtime_env_agent import (
    _create_image_uri_plugin,
)
from ray._private.runtime_env.context import RuntimeEnvContext, expand_run_option
from ray._private.runtime_env.image_uri import ImageURIPlugin, _modify_context_impl
from ray.core.generated.common_pb2 import Language

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


def _write_default_token(tmp_path: Path, token: str = TOKEN) -> Path:
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(token)
    return token_path


class _Exec(Exception):
    pass


def _launch(
    context: Optional[RuntimeEnvContext] = None,
    run_options: Optional[List[str]] = None,
    ray_tmp_dir: str = "/tmp/ray",
    logs_dir: Optional[str] = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Returns the argv and env the worker launcher would be exec'd with."""
    context = context or RuntimeEnvContext()
    _modify_context_impl(
        "fake-image",
        "/fake/default_worker.py",
        run_options or [],
        context,
        logging.getLogger(__name__),
        ray_tmp_dir,
        logs_dir,
    )

    # The worker launcher gets the context through serialize()/deserialize().
    context = RuntimeEnvContext.deserialize(context.serialize())

    def fake_execvpe(file: str, args: List[str], env: Dict[str, str]) -> NoReturn:
        raise _Exec(args, env)

    def shell_launch(*args: Any, **kwargs: Any) -> NoReturn:
        raise AssertionError("container worker launched through a shell")

    with mock.patch.dict(os.environ), mock.patch.object(
        os, "execvpe", fake_execvpe
    ), mock.patch.object(os, "execvp", shell_launch), mock.patch(
        "subprocess.Popen", shell_launch
    ):
        with pytest.raises(_Exec) as exec_call:
            context.exec_worker(["/host/default_worker.py"], Language.PYTHON)
    return exec_call.value.args


def _container_command(
    context: Optional[RuntimeEnvContext] = None,
    ray_tmp_dir: str = "/tmp/ray",
    logs_dir: Optional[str] = None,
) -> str:
    return " ".join(_launch(context, ray_tmp_dir=ray_tmp_dir, logs_dir=logs_dir)[0])


@pytest.mark.parametrize("external_logs", [False, True])
def test_mounts_default_token_file(auth_env, tmp_path, external_logs):
    token_path = _write_default_token(tmp_path)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()

    logs_dir = str(tmp_path.resolve() / "logs") if external_logs else None
    command = _container_command(logs_dir=logs_dir)

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert TOKEN not in command
    if logs_dir is not None:
        assert f"-v {logs_dir}:{logs_dir}" in command


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
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert TOKEN not in command


def test_ignores_token_path_from_job_env_vars(auth_env, tmp_path):
    token_path = _write_default_token(tmp_path)
    host_file = tmp_path / "host_file"
    host_file.write_text("not a token")
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()
    context = RuntimeEnvContext(env_vars={"RAY_AUTH_TOKEN_PATH": str(host_file)})

    command = _container_command(context)

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"-v {host_file}" not in command
    # The container sees the mounted node path, not the job's.
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert f"RAY_AUTH_TOKEN_PATH={host_file}" not in command


def test_run_options_see_the_container_token_path(auth_env, tmp_path):
    token_path = _write_default_token(tmp_path)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()
    context = RuntimeEnvContext(env_vars={"RAY_AUTH_TOKEN_PATH": "/job/path"})

    argv, _ = _launch(context, run_options=["--label=p=$RAY_AUTH_TOKEN_PATH"])

    assert f"--label=p={token_path}" in argv


def test_job_token_is_passed_as_given(auth_env, tmp_path):
    _write_default_token(tmp_path)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()
    context = RuntimeEnvContext(env_vars={"RAY_AUTH_TOKEN": "job-token"})

    command = _container_command(context)

    assert ":ro" not in command
    assert "--env RAY_AUTH_TOKEN=job-token" in command


def test_token_from_env_is_passed_by_name(auth_env, tmp_path):
    token_path = _write_default_token(tmp_path, "file-token")
    auth_env.setenv("RAY_AUTH_MODE", "token")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    reset_auth_token_state()

    argv, launcher_env = _launch()

    # RAY_AUTH_TOKEN takes precedence over the file, so nothing is mounted.
    assert ":ro" not in " ".join(argv)
    assert "RAY_AUTH_TOKEN_PATH" not in " ".join(argv)
    assert argv[argv.index("RAY_AUTH_TOKEN") - 1] == "--env"
    assert launcher_env["RAY_AUTH_TOKEN"] == TOKEN
    assert TOKEN not in " ".join(argv)
    assert list(token_path.parent.iterdir()) == [token_path]


def test_no_mount_when_auth_disabled(auth_env, tmp_path):
    _write_default_token(tmp_path)
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()

    command = _container_command()

    assert ":ro" not in command
    assert "RAY_AUTH_TOKEN_PATH" not in command


def test_log_omits_env_var_values(auth_env, caplog, tmp_path):
    auth_env.setenv("RAY_SECRET_FROM_HOST", "host-secret")
    context = RuntimeEnvContext(env_vars={"API_KEY": "user-secret"})
    logs_dir = str(tmp_path.resolve() / "logs")

    with caplog.at_level(logging.INFO):
        _launch(context, run_options=["--label=x=$HOME"], logs_dir=logs_dir)

    assert "podman run" in caplog.text
    # Volume mounts are not secret, so the log keeps the logs mount.
    assert f"{logs_dir}:{logs_dir}" in caplog.text
    assert "--label=x=$HOME" in caplog.text
    assert "fake-image" in caplog.text
    assert "user-secret" not in caplog.text
    assert "host-secret" not in caplog.text


def test_run_options_env_overrides_ray_and_job_env(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
    context = RuntimeEnvContext(env_vars={"JOB_VAR": "from_env_vars"})

    argv, _ = _launch(
        context,
        run_options=["--env=RAY_RAYLET_PID=5678", "--env=JOB_VAR=from_run_options"],
    )

    # podman applies the last --env for a name, so run_options must come last.
    assert argv.index("--env=RAY_RAYLET_PID=5678") > argv.index("RAY_RAYLET_PID")
    assert argv.index("--env=JOB_VAR=from_run_options") > argv.index(
        "JOB_VAR=from_env_vars"
    )


_RUN_OPTION_ENV = {"DIR": "/data dir", "EMPTY": ""}


@pytest.mark.parametrize(
    "run_option,expected",
    [
        ("--cap-drop=SYS_ADMIN", "--cap-drop=SYS_ADMIN"),
        ("--volume=$DIR:/data", "--volume=/data dir:/data"),
        ("${DIR}x $DIRx", "/data dirx "),
        ("--env=X=$UNSET$EMPTY", "--env=X="),
        ("price=$$5 $$DIR", "price=$5 $DIR"),
        (
            "$(id) `id` $5 $- $ 'a b' \\$DIR ~/d",
            "$(id) `id` $5 $- $ 'a b' \\/data dir ~/d",
        ),
        ("$DIRé", "/data diré"),
        ("", ""),
    ],
)
def test_expand_run_option(run_option, expected):
    assert expand_run_option(run_option, _RUN_OPTION_ENV) == expected


@pytest.mark.parametrize("run_option", ["${A:-b}", "${}", "${A", "${1}", "${é}"])
def test_expand_run_option_rejects_malformed(run_option):
    with pytest.raises(ValueError, match=r"use \$\$ for a literal \$"):
        expand_run_option(run_option, _RUN_OPTION_ENV)


def test_malformed_run_option_fails_setup(auth_env):
    # Fails in runtime env setup, before any worker launch is attempted.
    with pytest.raises(ValueError, match="Unsupported expansion"):
        _modify_context_impl(
            "fake-image",
            "/fake/default_worker.py",
            ["--volume=${HOME:-/tmp}:/data"],
            RuntimeEnvContext(),
            logging.getLogger(__name__),
            "/tmp/ray",
        )


def test_run_options_expand_from_launcher_and_job_env(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("HOST_DIR", "/host data")
    context = RuntimeEnvContext(env_vars={"JOB_VAR": "job value", "D": "${HOST_DIR}/x"})

    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    argv, _ = _launch(
        context,
        run_options=[
            "-v",
            "$HOST_DIR:/data",
            "--label=job=$JOB_VAR",
            "--label=t=$RAY_AUTH_TOKEN",
            "--label=d=$D",
        ],
    )

    # An expanded value stays inside its one argument.
    assert argv[argv.index("/host data:/data") - 1] == "-v"
    assert "--label=job=job value" in argv
    # RAY_* vars expand like any other, so this puts the token on argv; the
    # RuntimeEnv docstring says expanded values appear on the command line.
    assert f"--label=t={TOKEN}" in argv
    # A job value is used as written, not expanded a second time.
    assert "--label=d=${HOST_DIR}/x" in argv


def test_launch_runs_without_shell_and_keeps_job_env_in_container(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
    auth_env.setenv("RAY_JOB_ID", "01000000")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    auth_env.setenv("RAY_RUNTIME_ENV_BEARER_TOKEN", "bearer-secret")
    injected = "x'$(touch /tmp/pwned)'`id`"
    context = RuntimeEnvContext(
        env_vars={
            "JOB_SECRET": injected,
            "LD_PRELOAD": "/evil.so",
            "STOLEN": "${RAY_RUNTIME_ENV_BEARER_TOKEN}",
        }
    )

    argv, launcher_env = _launch(
        context,
        run_options=["--cap-drop SYS_ADMIN", "--label=a b", "$(id)"],
    )

    assert argv[0] == "podman"
    assert "bash" not in argv
    # Job values reach podman only as literal --env arguments.
    assert f"JOB_SECRET={injected}" in argv
    assert "LD_PRELOAD=/evil.so" in argv
    assert "JOB_SECRET" not in launcher_env
    assert "LD_PRELOAD" not in launcher_env
    # RAY_* vars are passed by name, so their values stay off podman's argv.
    for name, value in [
        ("RAY_RAYLET_PID", "1234"),
        ("RAY_JOB_ID", "01000000"),
        ("RAY_AUTH_TOKEN", TOKEN),
        ("RAY_RUNTIME_ENV_BEARER_TOKEN", "bearer-secret"),
    ]:
        assert argv[argv.index(name) - 1] == "--env"
        assert launcher_env[name] == value
        assert value not in " ".join(argv)
    assert "STOLEN=${RAY_RUNTIME_ENV_BEARER_TOKEN}" in argv
    # Each run_options item is exactly one argument, and nothing runs.
    assert "--cap-drop SYS_ADMIN" in argv
    assert "--label=a b" in argv
    assert "$(id)" in argv
    assert argv[-4:] == [
        "--entrypoint",
        "python",
        "fake-image",
        "/fake/default_worker.py",
    ]


def test_custom_logs_dir_is_mounted_in_container(auth_env, tmp_path):
    temp_dir = str(tmp_path.resolve() / "ray")
    logs_dir = str(tmp_path.resolve() / "logs")

    argv, _ = _launch(ray_tmp_dir=temp_dir, logs_dir=logs_dir)

    assert argv[argv.index(f"{temp_dir}:{temp_dir}") - 1] == "-v"
    assert argv[argv.index(f"{logs_dir}:{logs_dir}") - 1] == "-v"


def test_logs_dir_under_temp_dir_is_not_mounted_twice(auth_env, tmp_path):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    temp_dir = str(tmp_path.resolve() / "ray")

    argv, _ = _launch(
        ray_tmp_dir=temp_dir, logs_dir=os.path.join(temp_dir, "session", "logs")
    )

    assert argv.count("-v") == 1


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
@pytest.mark.parametrize("relative_link", [False, True])
def test_logs_symlink_target_is_mounted_in_container(auth_env, tmp_path, relative_link):
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

    argv, _ = _launch(ray_tmp_dir=str(temp_dir), logs_dir=str(alias))

    assert f"{logs_dir}:{logs_dir}" in argv


@pytest.mark.skipif(sys.platform == "win32", reason="Requires directory symlinks")
def test_temp_dir_alias_does_not_hide_required_logs_mount(auth_env, tmp_path):
    physical_dir = tmp_path.resolve() / "physical"
    logs_dir = physical_dir / "session" / "logs"
    logs_dir.mkdir(parents=True)
    temp_dir = tmp_path.resolve() / "alias"
    temp_dir.symlink_to(physical_dir, target_is_directory=True)

    argv, _ = _launch(ray_tmp_dir=str(temp_dir), logs_dir=str(logs_dir))

    assert f"{temp_dir}:{temp_dir}" in argv
    assert f"{logs_dir}:{logs_dir}" in argv


@pytest.mark.parametrize(
    "directory_name", ["logs with spaces", "logs'quotes", "logs$RAY_JOB_ID"]
)
def test_container_volume_paths_are_literal_arguments(
    auth_env, tmp_path, directory_name
):
    auth_env.setenv("RAY_JOB_ID", "worker-job")
    temp_dir = str(tmp_path.resolve() / "ray tmp")
    logs_dir = str(tmp_path.resolve() / directory_name)

    argv, _ = _launch(ray_tmp_dir=temp_dir, logs_dir=logs_dir)

    # Workers are exec'd without a shell, so each mount is one argument that
    # podman receives verbatim: no quoting and no expansion.
    assert argv[argv.index(f"{temp_dir}:{temp_dir}") - 1] == "-v"
    assert argv[argv.index(f"{logs_dir}:{logs_dir}") - 1] == "-v"


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
