import contextlib
import errno
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Optional, Tuple
from unittest import mock

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.context import RuntimeEnvContext, expand_run_option
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


def _write_default_token(tmp_path: Path, token: str = TOKEN) -> Path:
    token_path = tmp_path / ".ray" / "auth_token"
    token_path.parent.mkdir()
    token_path.write_text(token)
    return token_path


class _Exec(Exception):
    pass


def _fake_memfd_create(name: str, flags: int = 0) -> int:
    with tempfile.TemporaryFile() as f:
        return os.dup(f.fileno())


def _memfd_oserror(name: str, flags: int = 0) -> int:
    raise OSError(errno.ENOSYS, "memfd_create is blocked")


@contextlib.contextmanager
def _memfd(mode: str):
    """Makes os.memfd_create work, raise OSError or be missing, on any platform."""
    saved = getattr(os, "memfd_create", None)
    if mode == "missing":
        if saved is not None:
            del os.memfd_create
    else:
        os.memfd_create = _fake_memfd_create if mode == "ok" else _memfd_oserror
    try:
        yield
    finally:
        if saved is not None:
            os.memfd_create = saved
        elif hasattr(os, "memfd_create"):
            del os.memfd_create


def _launch_with_env_file(
    context: Optional[RuntimeEnvContext] = None,
    run_options: Optional[List[str]] = None,
    ray_tmp_dir: str = "/tmp/ray",
    memfd: str = "missing",
) -> Tuple[List[str], Dict[str, str], Optional[str]]:
    """Returns the argv and env the worker launcher would be exec'd with, and
    the content of the --env-file it passes, if any.

    memfd is "ok", "oserror" or "missing", for how os.memfd_create behaves.
    """
    context = context or RuntimeEnvContext()
    _modify_context_impl(
        "fake-image",
        "/fake/default_worker.py",
        run_options or [],
        context,
        logging.getLogger(__name__),
        ray_tmp_dir,
    )

    # The worker launcher gets the context through serialize()/deserialize().
    context = RuntimeEnvContext.deserialize(context.serialize())

    def fake_execvpe(file: str, args: List[str], env: Dict[str, str]) -> NoReturn:
        # What podman would read from the inherited fd.
        env_file = None
        for arg in args:
            if arg.startswith("--env-file=/dev/fd/"):
                fd = int(arg.rsplit("/", 1)[1])
                assert os.get_inheritable(fd)
                env_file = os.pread(fd, 1 << 20, 0).decode()
        raise _Exec(args, env, env_file)

    def shell_launch(*args: Any, **kwargs: Any) -> NoReturn:
        raise AssertionError("container worker launched through a shell")

    with mock.patch.dict(os.environ), mock.patch.object(
        os, "execvpe", fake_execvpe
    ), mock.patch.object(os, "execvp", shell_launch), mock.patch(
        "subprocess.Popen", shell_launch
    ), _memfd(
        memfd
    ):
        with pytest.raises(_Exec) as exec_call:
            context.exec_worker(["/host/default_worker.py"], Language.PYTHON)
    return exec_call.value.args


def _launch(
    context: Optional[RuntimeEnvContext] = None,
    run_options: Optional[List[str]] = None,
    ray_tmp_dir: str = "/tmp/ray",
) -> Tuple[List[str], Dict[str, str]]:
    """Returns the argv and env the worker launcher would be exec'd with, when
    os.memfd_create is missing, so the job's env_vars are --env arguments."""
    argv, env, _ = _launch_with_env_file(context, run_options, ray_tmp_dir)
    return argv, env


def _container_command(
    context: Optional[RuntimeEnvContext] = None, ray_tmp_dir: str = "/tmp/ray"
) -> str:
    return " ".join(_launch(context, ray_tmp_dir=ray_tmp_dir)[0])


def test_mounts_default_token_file(auth_env, tmp_path):
    token_path = _write_default_token(tmp_path)
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


def test_log_omits_env_var_values(auth_env, caplog):
    auth_env.setenv("RAY_SECRET_FROM_HOST", "host-secret")
    context = RuntimeEnvContext(env_vars={"API_KEY": "user-secret"})

    with caplog.at_level(logging.INFO):
        _launch(context, run_options=["--label=x=$HOME"])

    assert "podman run" in caplog.text
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


@pytest.mark.skipif(sys.platform == "win32", reason="Uses /dev/fd and os.pread.")
def test_job_env_vars_go_in_env_file_not_argv(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
    context = RuntimeEnvContext(
        env_vars={
            "DB_PASSWORD": "s3cret-VALUE-123",
            "SPACED": "a b=c #d",
            "EMPTY": "",
            "OVERRIDE": "file",
        }
    )

    argv, launcher_env, env_file = _launch_with_env_file(
        context,
        run_options=["--env=OVERRIDE=run_options"],
        memfd="ok",
    )

    assert env_file == (
        "DB_PASSWORD=s3cret-VALUE-123\nSPACED=a b=c #d\nEMPTY=\nOVERRIDE=file\n"
    )
    assert "s3cret-VALUE-123" not in " ".join(argv)
    assert "DB_PASSWORD" not in " ".join(argv)
    assert "DB_PASSWORD" not in launcher_env
    # The env file comes before the --env flags and run_options, so those win.
    env_file_index = [i for i, a in enumerate(argv) if a.startswith("--env-file=")]
    assert len(env_file_index) == 1
    assert env_file_index[0] < argv.index("RAY_RAYLET_PID")
    assert env_file_index[0] < argv.index("--env=OVERRIDE=run_options")
    # RAY_* vars are still passed by name.
    assert argv[argv.index("RAY_RAYLET_PID") - 1] == "--env"


@pytest.mark.skipif(sys.platform == "win32", reason="Uses /dev/fd and os.pread.")
def test_job_ray_var_in_env_file_beats_host_var(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    auth_env.setenv("RAY_RAYLET_PID", "1234")
    context = RuntimeEnvContext(env_vars={"RAY_RAYLET_PID": "from-job"})

    argv, _, env_file = _launch_with_env_file(context, memfd="ok")

    # A by-name --env after the env file would otherwise put the host value
    # in the container.
    assert env_file == "RAY_RAYLET_PID=from-job\n"
    assert "RAY_RAYLET_PID" not in argv


@pytest.mark.skipif(sys.platform == "win32", reason="Uses /dev/fd.")
def test_env_file_fd_is_closed_when_exec_fails(auth_env):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    context = RuntimeEnvContext(env_vars={"API_KEY": "user-secret"})
    _modify_context_impl(
        "fake-image",
        "/fake/default_worker.py",
        [],
        context,
        logging.getLogger(__name__),
        "/tmp/ray",
    )
    fds = []

    def failing_execvpe(file: str, args: List[str], env: Dict[str, str]) -> NoReturn:
        fds.extend(int(a.rsplit("/", 1)[1]) for a in args if "--env-file=" in a)
        raise FileNotFoundError(errno.ENOENT, "podman", file)

    with _memfd("ok"), mock.patch.object(os, "execvpe", failing_execvpe):
        with pytest.raises(FileNotFoundError):
            context.exec_worker(["/host/default_worker.py"], Language.PYTHON)

    assert len(fds) == 1
    with pytest.raises(OSError):
        os.fstat(fds[0])


def test_no_env_file_without_job_env_vars(auth_env):
    argv, _, env_file = _launch_with_env_file(memfd="ok")

    assert env_file is None
    assert not any(a.startswith("--env-file") for a in argv)


@pytest.mark.parametrize("memfd", ["missing", "oserror"])
def test_env_vars_fall_back_to_argv_without_memfd(
    auth_env, caplog, propagate_logs, memfd
):
    context = RuntimeEnvContext(env_vars={"API_KEY": "user-secret"})

    with caplog.at_level(logging.WARNING):
        argv, _, env_file = _launch_with_env_file(context, memfd=memfd)

    assert env_file is None
    assert "API_KEY=user-secret" in argv
    assert not any(a.startswith("--env-file") for a in argv)
    assert "will appear on the container worker's command line" in caplog.text
    assert "user-secret" not in caplog.text


@pytest.mark.parametrize(
    "env_vars,bad_name",
    [
        ({"PEM": "-----BEGIN-----\nabc"}, "PEM"),
        ({"WIN": "a\r\nb"}, "WIN"),
        ({"NUL": "a\0b"}, "NUL"),
        ({"A=B": "x"}, "A=B"),
        ({"A B": "x"}, "A B"),
        ({"#COMMENT": "x"}, "#COMMENT"),
        ({"": "x"}, "''"),
    ],
)
def test_unsupported_env_vars_fail_setup(auth_env, env_vars, bad_name):
    # Fails in runtime env setup, before any worker launch is attempted.
    with pytest.raises(ValueError, match="env_vars") as error:
        _modify_context_impl(
            "fake-image",
            "/fake/default_worker.py",
            [],
            RuntimeEnvContext(env_vars=env_vars),
            logging.getLogger(__name__),
            "/tmp/ray",
        )
    assert bad_name in str(error.value)
    # The message names the variable but never echoes a value.
    assert "abc" not in str(error.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
