import logging
import os
import stat
import sys
from typing import Dict, List, Optional, Tuple
from unittest import mock

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import _modify_context_impl
from ray.core.generated.common_pb2 import Language

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


class _Exec(Exception):
    pass


def _launch(
    context: Optional[RuntimeEnvContext] = None,
    run_options: Optional[List[str]] = None,
    ray_tmp_dir: str = "/tmp/ray",
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
    )

    def fake_execvpe(file, args, env):
        raise _Exec(args, env)

    with mock.patch.dict(os.environ), mock.patch.object(os, "execvpe", fake_execvpe):
        with pytest.raises(_Exec) as exec_call:
            context.exec_worker(["/host/default_worker.py"], Language.PYTHON)
    return exec_call.value.args


def _container_command(
    context: Optional[RuntimeEnvContext] = None, ray_tmp_dir: str = "/tmp/ray"
) -> str:
    return " ".join(_launch(context, ray_tmp_dir=ray_tmp_dir)[0])


def test_mounts_default_token_file(auth_env, tmp_path):
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(TOKEN)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()

    command = _container_command()

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
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
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert TOKEN not in command


def test_token_from_env_is_mounted_as_file(auth_env, tmp_path):
    auth_env.setenv("RAY_AUTH_MODE", "token")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    reset_auth_token_state()

    command = _container_command(ray_tmp_dir=str(tmp_path))

    token_path = tmp_path / "image_uri_auth_token"
    assert token_path.read_text() == TOKEN
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert TOKEN not in command


def test_no_mount_when_auth_disabled(auth_env, tmp_path):
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(TOKEN)
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()

    command = _container_command()

    assert ":ro" not in command
    assert "RAY_AUTH_TOKEN_PATH" not in command


def test_log_omits_env_var_values(auth_env, caplog):
    auth_env.setenv("RAY_SECRET_FROM_HOST", "host-secret")
    context = RuntimeEnvContext(env_vars={"API_KEY": "user-secret"})

    with caplog.at_level(logging.INFO):
        _container_command(context)

    assert "fake-image" in caplog.text
    assert "user-secret" not in caplog.text
    assert "host-secret" not in caplog.text


def test_launch_runs_without_shell_and_keeps_job_env_in_container(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    auth_env.setenv("RAY_RUNTIME_ENV_BEARER_TOKEN", "bearer-secret")
    injected = "x'$(touch /tmp/pwned)'`id`"
    context = RuntimeEnvContext(
        env_vars={"JOB_SECRET": injected, "LD_PRELOAD": "/evil.so"}
    )

    argv, launcher_env = _launch(
        context, run_options=["--cap-drop SYS_ADMIN", "--label 'a b'", "$(id)"]
    )

    assert argv[0] == "podman"
    assert "bash" not in argv
    # Job values reach podman only as literal --env arguments.
    assert f"JOB_SECRET={injected}" in argv
    assert "LD_PRELOAD=/evil.so" in argv
    assert "JOB_SECRET" not in launcher_env
    assert "LD_PRELOAD" not in launcher_env
    # Ray-owned vars are passed by name; node credentials are not passed.
    assert argv[argv.index("RAY_RAYLET_PID") - 1] == "--env"
    assert launcher_env["RAY_RAYLET_PID"] == "1234"
    assert "RAY_AUTH_TOKEN" not in argv
    assert "RAY_RUNTIME_ENV_BEARER_TOKEN" not in argv
    assert TOKEN not in " ".join(argv)
    # Each run_options item is split like a shell would, without expansion.
    assert argv[argv.index("--cap-drop") + 1] == "SYS_ADMIN"
    assert argv[argv.index("--label") + 1] == "a b"
    assert "$(id)" in argv
    assert argv[-4:] == [
        "--entrypoint",
        "python",
        "fake-image",
        "/fake/default_worker.py",
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
