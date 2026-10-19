import logging
import os
import stat
import sys
from typing import Dict, List, Optional, Tuple
from unittest import mock

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.context import RuntimeEnvContext, split_run_option
from ray._private.runtime_env.image_uri import (
    _modify_context_impl,
    _write_owner_only_file,
)
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


def test_ignores_token_path_from_job_env_vars(auth_env, tmp_path):
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(TOKEN)
    host_file = tmp_path / "host_file"
    host_file.write_text("not a token")
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()
    context = RuntimeEnvContext(env_vars={"RAY_AUTH_TOKEN_PATH": str(host_file)})

    command = _container_command(context)

    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"-v {host_file}" not in command


def test_token_from_env_is_mounted_as_file(auth_env, tmp_path):
    auth_env.setenv("RAY_AUTH_MODE", "token")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    reset_auth_token_state()

    ray_tmp_dir = tmp_path / "ray"
    command = _container_command(ray_tmp_dir=str(ray_tmp_dir))

    # Containers mount ray_tmp_dir read-write, so the token must live elsewhere.
    (token_path,) = (tmp_path / ".ray").glob("image_uri_auth_token_*")
    assert token_path.read_text() == TOKEN
    # Windows reports 0o666 for any writable file.
    if sys.platform != "win32":
        assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert TOKEN not in command


def test_unwritable_token_dir_names_the_workaround(auth_env, tmp_path):
    # A file where ~/.ray should be makes the directory uncreatable.
    (tmp_path / ".ray").write_text("")
    auth_env.setenv("RAY_AUTH_MODE", "token")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    reset_auth_token_state()

    with pytest.raises(RuntimeError, match="RAY_AUTH_TOKEN_PATH"):
        _container_command()


@pytest.mark.parametrize("temp_file_already_gone", [False, True])
def test_failed_token_write_leaves_no_temp_file(tmp_path, temp_file_already_gone):
    def fail_replace(src, dst):
        # When the temp file is already gone, cleanup itself fails, and that
        # error must not replace the original one.
        if temp_file_already_gone:
            os.unlink(src)
        raise PermissionError("replace failed")

    with mock.patch.object(os, "replace", fail_replace):
        with pytest.raises(PermissionError, match="replace failed"):
            _write_owner_only_file(str(tmp_path / "token"), TOKEN)
    assert list(tmp_path.iterdir()) == []


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


def test_run_options_env_overrides_ray_and_job_env(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
    context = RuntimeEnvContext(env_vars={"JOB_VAR": "from_env_vars"})

    argv, _ = _launch(
        context,
        run_options=["--env RAY_RAYLET_PID=5678", "--env JOB_VAR=from_run_options"],
    )

    # podman applies the last --env for a name, so run_options must come last.
    assert argv.index("RAY_RAYLET_PID=5678") > argv.index("RAY_RAYLET_PID")
    assert argv.index("JOB_VAR=from_run_options") > argv.index("JOB_VAR=from_env_vars")


_RUN_OPTION_ENV = {"DIR": "/data dir", "HOME_DIR": "/home/ray", "EMPTY": ""}


@pytest.mark.parametrize(
    "run_option,expected",
    [
        ("--cap-drop SYS_ADMIN", ["--cap-drop", "SYS_ADMIN"]),
        ("  --rm\t-it  ", ["--rm", "-it"]),
        ("--label 'a b'", ["--label", "a b"]),
        ("--label=x' 'y", ["--label=x y"]),
        ("-v $DIR:/data", ["-v", "/data dir:/data"]),
        ("-v ${DIR}x", ["-v", "/data dirx"]),
        ("-v $DIRx", ["-v"]),
        ('--label "home=$HOME_DIR"', ["--label", "home=/home/ray"]),
        ("--label '$HOME_DIR'", ["--label", "$HOME_DIR"]),
        ("--label \\$HOME_DIR", ["--label", "$HOME_DIR"]),
        ('--label "\\$HOME_DIR \\" \\a"', ["--label", '$HOME_DIR " \\a']),
        ("--env X=$UNSET", ["--env", "X="]),
        ("--rm $UNSET $EMPTY", ["--rm"]),
        ("--label \"$UNSET\" ''", ["--label", "", ""]),
        ("$(id) `id` $5 $ $-", ["$(id)", "`id`", "$5", "$", "$-"]),
        ("", []),
    ],
)
def test_split_run_option(run_option, expected):
    assert split_run_option(run_option, _RUN_OPTION_ENV) == expected


@pytest.mark.parametrize(
    "run_option", ["--label 'a", '--label "a', "--rm \\", "${A:-b}", "${}", "${A"]
)
def test_split_run_option_rejects_malformed(run_option):
    with pytest.raises(ValueError):
        split_run_option(run_option, _RUN_OPTION_ENV)


def test_malformed_run_option_fails_setup():
    with pytest.raises(ValueError, match="No closing quotation"):
        _launch(run_options=["--label 'a"])


def test_run_options_expand_from_launcher_and_job_env(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("HOST_DIR", "/host data")
    auth_env.setenv("RAY_AUTH_TOKEN", TOKEN)
    auth_env.setenv("RAY_RUNTIME_ENV_BEARER_TOKEN", "bearer-secret")
    context = RuntimeEnvContext(env_vars={"JOB_VAR": "job value"})

    argv, _ = _launch(
        context,
        run_options=[
            "-v $HOST_DIR:/data",
            "--label job=$JOB_VAR",
            "--label stolen=${RAY_AUTH_TOKEN}$RAY_RUNTIME_ENV_BEARER_TOKEN",
        ],
    )

    assert argv[argv.index("/host data:/data") - 1] == "-v"
    assert "job=job value" in argv
    # Node secrets are not in the expansion environment.
    assert "stolen=" in argv
    assert TOKEN not in " ".join(argv)
    assert "bearer-secret" not in " ".join(argv)


def test_launch_runs_without_shell_and_keeps_job_env_in_container(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
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
        run_options=[
            "--cap-drop SYS_ADMIN",
            "--label 'a b'",
            "$(id)",
            "--env-host",
            "--env RAY_RUNTIME_ENV_BEARER_TOKEN",
        ],
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
    # podman can't forward what it doesn't have, whatever run_options say.
    assert "RAY_AUTH_TOKEN" not in launcher_env
    assert "RAY_RUNTIME_ENV_BEARER_TOKEN" not in launcher_env
    assert TOKEN not in " ".join(argv)
    assert "STOLEN=${RAY_RUNTIME_ENV_BEARER_TOKEN}" in argv
    assert "bearer-secret" not in " ".join(argv)
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
