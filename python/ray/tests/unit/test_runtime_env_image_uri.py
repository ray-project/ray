import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Optional, Tuple
from unittest import mock

import pytest

from ray._private.authentication_test_utils import reset_auth_token_state
from ray._private.runtime_env.context import RuntimeEnvContext, expand_run_option
from ray._private.runtime_env.image_uri import (
    _CACHE_SCHEMA_VERSION,
    ImageMetadata,
    ImageURIPlugin,
    _modify_context_impl,
)
from ray.core.generated.common_pb2 import Language
from ray.runtime_env import RuntimeEnv

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
    return _exec_worker(context)


def _exec_worker(context: RuntimeEnvContext) -> Tuple[List[str], Dict[str, str]]:
    """Returns the argv and env a modified context's worker is exec'd with."""
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


IMAGE_URI_CACHE_URI = "image-pip://cache-key"


def _image_uri_plugin(tmp_path: Path) -> Tuple[ImageURIPlugin, str]:
    """Returns a plugin with a resolved, published cache entry and its path."""
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    plugin._metadata_by_uri[IMAGE_URI_CACHE_URI] = ImageMetadata(
        image_id="sha256:image",
        image_digest="sha256:digest",
        os="linux",
        architecture="amd64",
        python_executable="/usr/bin/python",
        python_version="3.12.1",
        python_implementation="CPython",
        python_cache_tag="cpython-312",
        python_soabi="cpython-312-x86_64-linux-gnu",
        python_platform="linux-x86_64",
        python_path="/image/bin",
        ray_version="2.56.0",
        ray_commit="commit",
        worker_path="/image/default_worker.py",
    )
    cache_path = plugin._get_cache_path(IMAGE_URI_CACHE_URI)
    virtualenv_python = Path(cache_path, "virtualenv", "bin", "python")
    virtualenv_python.parent.mkdir(parents=True)
    virtualenv_python.symlink_to("/usr/bin/python")
    Path(cache_path, "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "uri": IMAGE_URI_CACHE_URI,
                "has_pip": True,
                "size_bytes": 0,
            }
        )
    )
    return plugin, cache_path


def _image_uri_launch(
    tmp_path: Path, runtime_env: RuntimeEnv, context: RuntimeEnvContext
) -> Tuple[List[str], Dict[str, str], str]:
    plugin, cache_path = _image_uri_plugin(tmp_path)
    plugin.modify_context(
        [IMAGE_URI_CACHE_URI], runtime_env, context, logging.getLogger(__name__)
    )
    return (*_exec_worker(context), cache_path)


@pytest.mark.skipif(sys.platform == "win32", reason="image_uri is Linux-only.")
@pytest.mark.parametrize(
    "job_path,path_suffix", [(None, ":/image/bin"), ("", ""), ("/job/bin", ":/job/bin")]
)
def test_image_uri_pip_worker_runs_cached_virtualenv(
    auth_env, tmp_path, job_path, path_suffix
):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()
    env_vars = {"JOB_VAR": "job value"}
    if job_path is not None:
        env_vars["PATH"] = job_path

    argv, launcher_env, cache_path = _image_uri_launch(
        tmp_path,
        RuntimeEnv(image_uri="podman://example/image:tag", pip=["package==1"]),
        RuntimeEnvContext(env_vars=env_vars),
    )

    virtualenv = os.path.join(cache_path, "virtualenv")
    assert argv[0] == "podman"
    assert "--rm" in argv
    assert argv[argv.index(f"{cache_path}:{cache_path}:ro,z") - 1] == "-v"
    assert f"VIRTUAL_ENV={virtualenv}" in argv
    # One PATH: the virtualenv, then the job's PATH or else the image's.
    paths = [arg for arg in argv if arg.startswith("PATH=")]
    assert paths == [f"PATH={virtualenv}/bin{path_suffix}"]
    # Job values still reach only the container.
    assert "JOB_VAR=job value" in argv
    assert "JOB_VAR" not in launcher_env
    # The exact image the environment was built against.
    assert argv[-4:] == [
        "--entrypoint",
        f"{virtualenv}/bin/python",
        "sha256:image",
        "/image/default_worker.py",
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="image_uri is Linux-only.")
def test_image_uri_worker_without_pip_uses_image_reference(auth_env, tmp_path):
    auth_env.setenv("RAY_AUTH_MODE", "disabled")
    reset_auth_token_state()

    argv, _, cache_path = _image_uri_launch(
        tmp_path,
        RuntimeEnv(image_uri="podman://example/image:tag"),
        RuntimeEnvContext(),
    )

    assert cache_path not in " ".join(argv)
    assert not any(arg.startswith(("VIRTUAL_ENV=", "PATH=")) for arg in argv)
    # No cached state depends on the image, so podman can re-pull a pruned
    # image by its reference.
    assert argv[-4:] == [
        "--entrypoint",
        "/usr/bin/python",
        "example/image:tag",
        "/image/default_worker.py",
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="image_uri is Linux-only.")
@pytest.mark.parametrize("has_pip", [False, True])
def test_image_uri_worker_mounts_token_file(auth_env, tmp_path, has_pip):
    token_path = _write_default_token(tmp_path)
    auth_env.setenv("RAY_AUTH_MODE", "token")
    reset_auth_token_state()
    runtime_env = RuntimeEnv(image_uri="example/image:tag")
    if has_pip:
        runtime_env = RuntimeEnv(image_uri="example/image:tag", pip=["package==1"])

    argv, _, _ = _image_uri_launch(tmp_path, runtime_env, RuntimeEnvContext())

    command = " ".join(argv)
    assert f"-v {token_path}:{token_path}:ro" in command
    assert f"--env RAY_AUTH_TOKEN_PATH={token_path}" in command
    assert TOKEN not in command


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
