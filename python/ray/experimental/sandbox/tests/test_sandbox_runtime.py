"""SandboxRuntime-layer tests with fake manager/backend (no runsc needed).

These pin that ``create()`` arguments actually reach the backend's
``SandboxConfig`` — the layer where a declared-but-unforwarded field once
shipped behind a green suite.
"""

import asyncio
import concurrent.futures
import json
import os
import subprocess
import sys
import threading
import time
import warnings
from unittest.mock import Mock

import pytest

from ray.experimental.sandbox._internal.image_utils import image_cache_name
from ray.experimental.sandbox.backend.base import ExecResult, SandboxStatus
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxError
from ray.experimental.sandbox.runtime import SandboxRuntime
from ray.util.annotations import RayDeprecationWarning


class _FakeImageManager:
    def pull_image(self, image, timeout_seconds=120.0):
        return f"/tmp/fake-images/{image}"


class _FakeBackend:
    def __init__(self):
        self.configs = []
        self.deleted = []

    def create_sandbox(self, config):
        self.configs.append(config)
        return "ray-sandbox-fake0001"

    def delete_sandbox(self, sandbox_id):
        self.deleted.append(sandbox_id)

    def exec_command(
        self,
        sandbox_id,
        command,
        timeout=None,
        cwd=None,
        env=None,
        shell=None,
        user=None,
        combine_output=False,
    ):
        self.exec_options = {
            "timeout": timeout,
            "cwd": cwd,
            "env": env,
            "shell": shell,
            "user": user,
            "combine_output": combine_output,
        }
        return ExecResult(
            exit_code=0,
            stdout="",
            stderr="",
            duration_seconds=0.0,
            output=b"\xff" if combine_output else None,
        )

    def write_file(self, sandbox_id, path, content):
        pass

    def read_file(self, sandbox_id, path):
        return b""

    def get_status(self, sandbox_id):
        return SandboxStatus.RUNNING

    def checkpoint_sandbox(
        self,
        sandbox_id: str,
        checkpoint_path=None,
        leave_running: bool = True,
        **kwargs,
    ):
        path = checkpoint_path or f"/tmp/checkpoints/{sandbox_id}"
        self.checkpoints.append(
            {
                "sandbox_id": sandbox_id,
                "checkpoint_path": path,
                "leave_running": leave_running,
                "kwargs": kwargs,
            }
        )
        return {
            "checkpoint_path": path,
            "sandbox_id": sandbox_id,
            "leave_running": leave_running,
        }

    def restore_sandbox(self, checkpoint_path: str, **kwargs):
        new_id = f"ray-sandbox-restored-{len(self.restores) + 1}"
        self.restores.append(
            {"checkpoint_path": checkpoint_path, "new_id": new_id, "kwargs": kwargs}
        )
        return new_id

    def pause_sandbox(self, sandbox_id: str, timeout_seconds=None):
        self.paused.append(sandbox_id)

    def resume_sandbox(self, sandbox_id: str, timeout_seconds=None):
        self.resumed.append(sandbox_id)


def _fake_runtime():
    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    backend = _FakeBackend()
    backend.checkpoints = []
    backend.restores = []
    backend.paused = []
    backend.resumed = []
    runtime._backend = backend
    return runtime


def test_create_forwards_config_fields_to_backend():
    runtime = _fake_runtime()
    runtime.create(
        image="fake:latest",
        cpu=2.0,
        memory="1Gi",
        env={"A": "1"},
        workdir="/app",
        network="host",
        dns=["10.0.0.2"],
        capabilities=["CAP_CHOWN"],
        rootless=True,
        shell="/bin/bash",
        oom_score_adj=0,
    )
    (config,) = runtime._backend.configs
    assert config.image == "fake:latest"
    assert config.cpu == 2.0
    assert config.env == {"A": "1"}
    assert config.workdir == "/app"
    assert config.network == "host"
    assert config.dns == ["10.0.0.2"]
    assert config.capabilities == ["CAP_CHOWN"]
    assert config.rootless is True
    assert config.shell == "/bin/bash"
    assert config.oom_score_adj == 0


def test_create_ignores_readonly_false():
    """Callers written for the removed readonly option still pass
    readonly=False. It warns and changes nothing: every sandbox is writable."""
    runtime = _fake_runtime()
    with pytest.warns(RayDeprecationWarning, match="'readonly' option was removed"):
        runtime.create(image="fake:latest", readonly=False)
    (config,) = runtime._backend.configs
    assert not hasattr(config, "readonly")

    # Other unknown options still fail.
    with pytest.raises(TypeError, match="writable"):
        runtime.create(image="fake:latest", writable=True)


def test_create_ignores_readonly_none():
    """A wrapper that forwards its own unset readonly passes None. That
    isn't a use of the option, so it doesn't warn."""
    runtime = _fake_runtime()
    with warnings.catch_warnings():
        warnings.simplefilter("error", RayDeprecationWarning)
        runtime.create(image="fake:latest", readonly=None)
    (config,) = runtime._backend.configs
    assert not hasattr(config, "readonly")


def test_create_refuses_readonly_true():
    """readonly=True asked for a root filesystem the sandbox can't write.
    No sandbox has one now, so the call fails instead of starting a writable
    sandbox."""
    runtime = _fake_runtime()
    with pytest.raises(ValueError, match="readonly=True is no longer supported"):
        runtime.create(image="fake:latest", readonly=True)
    assert runtime._backend.configs == []


def test_restore_handles_the_removed_readonly_option():
    runtime = _fake_runtime()
    runtime._backend.restore_sandbox = Mock(return_value="restored-1")
    with pytest.raises(ValueError, match="readonly=True is no longer supported"):
        runtime.restore("/ckpt", readonly=True)
    runtime._backend.restore_sandbox.assert_not_called()

    with pytest.warns(RayDeprecationWarning, match="'readonly' option was removed"):
        assert runtime.restore("/ckpt", readonly=False) == "restored-1"
    assert "readonly" not in runtime._backend.restore_sandbox.call_args.kwargs


def _fake_running_backend(tmp_path):
    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
    from ray.experimental.sandbox.config import SandboxConfig

    backend = GVisorSandboxBackend(image_manager=_FakeImageManager())
    backend._sandbox_metadata["sandbox"] = {
        "config": SandboxConfig(image="fake:latest"),
        "root_dir": str(tmp_path),
        "cwd": "/",
        "status": SandboxStatus.RUNNING,
    }
    return backend


@pytest.mark.parametrize("combine_output", [False, True])
def test_gvisor_exec_binary_output(tmp_path, monkeypatch, combine_output: bool) -> None:
    backend = _fake_running_backend(tmp_path)
    popen = subprocess.Popen
    commands = []
    command = [
        sys.executable,
        "-c",
        "import os; os.write(1, b'out\\x00\\xff'); "
        "os.write(2, b'err\\xfe'); os.write(1, b'end'); os._exit(7)",
    ]

    def local_popen(args, **kwargs):
        sandbox_command = args[args.index("sandbox") + 1 :]
        commands.append(sandbox_command)
        return popen(sandbox_command, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", local_popen)
    result = backend.exec_command("sandbox", command, combine_output=combine_output)
    assert commands == [command]
    assert result.exit_code == 7
    if combine_output:
        assert result.output == b"out\x00\xfferr\xfeend"
        assert result.stdout == result.stderr == ""
    else:
        assert result.output is None
        assert result.stdout == "out\x00\ufffdend"
        assert result.stderr == "err\ufffd"


@pytest.mark.parametrize("combine_output", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_runtime_exec_forwards_combine_output(
    combine_output: bool, asynchronous: bool
) -> None:
    runtime = _fake_runtime()
    instance_id = runtime.create(image="fake:latest")
    if asynchronous:
        result = asyncio.run(
            runtime.exec_async(instance_id, ["true"], combine_output=combine_output)
        )
    else:
        result = runtime.exec(instance_id, ["true"], combine_output=combine_output)
    assert runtime._backend.exec_options["combine_output"] is combine_output
    assert result.output == (b"\xff" if combine_output else None)


@pytest.mark.parametrize(
    "append,tee_exit_code", [(False, 127), (False, 1), (False, None), (True, None)]
)
def test_gvisor_write_file_fallback_and_append(
    tmp_path, monkeypatch, append: bool, tee_exit_code
) -> None:
    backend = _fake_running_backend(tmp_path)
    popen = subprocess.Popen
    commands = []
    content = bytes(range(256)) * 4
    path = tmp_path / "nested dir" / "quote' and $(not-a-command).bin"
    if append:
        path.parent.mkdir()
    else:
        assert not path.parent.exists()
    if append:
        path.write_bytes(b"existing\x00\xff")

    def local_popen(args, **kwargs):
        command = args[args.index("sandbox") + 1 :]
        commands.append(command)
        if command[0] == "tee" and tee_exit_code is not None:
            process = Mock(returncode=tee_exit_code)
            process.communicate.return_value = (None, b"tee unavailable")
            return process
        return popen(command, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", local_popen)
    backend.write_file("sandbox", str(path), content, append=append)

    assert path.read_bytes() == (b"existing\x00\xff" if append else b"") + content
    assert len(commands) == (1 if append else 2)
    if not append:
        assert commands[0][0] == "tee"
    assert commands[-1][:2] == ["/bin/sh", "-c"]


def _fake_running_backend(tmp_path):
    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
    from ray.experimental.sandbox.config import SandboxConfig

    backend = GVisorSandboxBackend(image_manager=_FakeImageManager())
    backend._sandbox_metadata["sandbox"] = {
        "config": SandboxConfig(image="fake:latest"),
        "root_dir": str(tmp_path),
        "cwd": "/",
        "status": SandboxStatus.RUNNING,
    }
    return backend


@pytest.mark.parametrize("paused", [False, True])
@pytest.mark.parametrize("failure", [None, "error", "timeout"])
def test_export_rootfs_preserves_pause_state(tmp_path, monkeypatch, paused, failure):
    backend = _fake_running_backend(tmp_path)
    initial = SandboxStatus.PAUSED if paused else SandboxStatus.RUNNING
    backend._sandbox_metadata["sandbox"]["status"] = initial
    calls = []

    def run(args, **kwargs):
        calls.append((args, kwargs["timeout"]))
        if "tar" in args:
            assert backend.get_status("sandbox") == SandboxStatus.PAUSED
            if failure == "timeout":
                raise subprocess.TimeoutExpired(args, 7)
            return subprocess.CompletedProcess(
                args, 1 if failure else 0, stdout="", stderr="export failed"
            )
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    if failure:
        expected = subprocess.TimeoutExpired if failure == "timeout" else SandboxError
        with pytest.raises(expected):
            backend.export_rootfs_changes("sandbox", str(tmp_path / "rootfs.tar"), 7)
    else:
        backend.export_rootfs_changes("sandbox", str(tmp_path / "rootfs.tar"), 7)
    assert backend.get_status("sandbox") == initial
    assert [args[args.index("tar") :] for args, _ in calls if "tar" in args] == [
        ["tar", "rootfs-upper", "--file", str(tmp_path / "rootfs.tar"), "sandbox"]
    ]
    assert [timeout for _, timeout in calls] == ([7] if paused else [60, 7, 60])


def test_concurrent_exports_of_one_sandbox_run_one_at_a_time(tmp_path, monkeypatch):
    """Each export pauses, exports and resumes on its own: a second one neither
    pauses an already paused sandbox nor has it resumed under its export."""
    backend = _fake_running_backend(tmp_path)
    commands = []
    first_export_running = threading.Event()
    second_command_seen = threading.Event()

    def run(args, **kwargs):
        command = next(c for c in ("pause", "tar", "resume") if c in args)
        commands.append(command)
        if command == "tar" and not first_export_running.is_set():
            first_export_running.set()
            # Leave the other export time to barge in.
            second_command_seen.wait(0.3)
        elif first_export_running.is_set():
            second_command_seen.set()
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        first = pool.submit(
            backend.export_rootfs_changes, "sandbox", str(tmp_path / "a.tar")
        )
        assert first_export_running.wait(5)
        second = pool.submit(
            backend.export_rootfs_changes, "sandbox", str(tmp_path / "b.tar")
        )
        first.result()
        second.result()
    assert commands == ["pause", "tar", "resume"] * 2
    assert backend.get_status("sandbox") == SandboxStatus.RUNNING


@pytest.mark.parametrize(
    "target, cached_base, match",
    [
        (
            f"registry.example/target@sha256:{'0' * 64}",
            True,
            "needs a tag, not a digest",
        ),
        ("registry.example/target:new", False, "Cannot commit base image"),
    ],
)
def test_commit_fails_before_exporting_the_rootfs(
    tmp_path, target: str, cached_base: bool, match: str
) -> None:
    base = "registry.example/source:old"
    if cached_base:
        image_dir = tmp_path / image_cache_name(base)
        image_dir.mkdir()
        (image_dir / ".manifest.json").write_text(json.dumps({"config": {}}))
        (image_dir / ".image_config.json").write_text("{}")
    runtime = _fake_runtime()
    runtime._image_manager = Mock(images_dir=str(tmp_path))
    runtime._backend = Mock()
    runtime._backend.get_config.return_value = SandboxConfig(image=base)

    with pytest.raises(SandboxError, match=match):
        runtime.commit("sandbox", target)
    runtime._backend.export_rootfs_changes.assert_not_called()


def test_ttl_is_enforced_by_the_runtime():
    runtime = _fake_runtime()
    instance_id = runtime.create(image="fake:latest", ttl_seconds=1)
    assert runtime._backend.deleted == []
    deadline = time.monotonic() + 5
    while not runtime._backend.deleted and time.monotonic() < deadline:
        time.sleep(0.05)
    assert runtime._backend.deleted == [instance_id]
    # The timer is gone after firing (delete popped it).
    assert instance_id not in runtime._ttl_timers


def test_delete_cancels_the_ttl_timer():
    runtime = _fake_runtime()
    instance_id = runtime.create(image="fake:latest", ttl_seconds=3600)
    timer = runtime._ttl_timers[instance_id]
    runtime.delete(instance_id)
    assert instance_id not in runtime._ttl_timers
    # cancel() sets the finished event; the thread itself may take a beat to
    # exit, so assert the deterministic signal rather than thread liveness.
    assert timer.finished.is_set()
    assert runtime._backend.deleted == [instance_id]


def test_no_ttl_by_default():
    runtime = _fake_runtime()
    instance_id = runtime.create(image="fake:latest")
    assert instance_id not in runtime._ttl_timers
    (config,) = runtime._backend.configs
    assert config.ttl_seconds is None
    assert config.shell == "/bin/bash"


def test_checkpoint_and_restore_forward_to_backend():
    runtime = _fake_runtime()
    instance_id = runtime.create(image="fake:latest")

    # Checkpoint with leave_running=True
    ckpt_path = runtime.checkpoint(
        instance_id,
        checkpoint_path="/tmp/test_checkpoint",
        leave_running=True,
    )
    assert ckpt_path == "/tmp/test_checkpoint"
    assert len(runtime._backend.checkpoints) == 1
    assert runtime._backend.checkpoints[0]["sandbox_id"] == instance_id
    assert runtime._backend.checkpoints[0]["checkpoint_path"] == "/tmp/test_checkpoint"
    assert runtime._backend.checkpoints[0]["leave_running"] is True

    # Restore from checkpoint
    restored_id = runtime.restore(
        checkpoint_path=ckpt_path,
        cpu=4.0,
        memory="2Gi",
    )
    assert restored_id.startswith("ray-sandbox-restored-")
    assert len(runtime._backend.restores) == 1
    assert runtime._backend.restores[0]["checkpoint_path"] == ckpt_path
    assert runtime._backend.restores[0]["kwargs"]["cpu"] == 4.0
    assert runtime._backend.restores[0]["kwargs"]["memory"] == "2Gi"


def test_gvisor_backend_pause_resume_lifecycle(tmp_path, monkeypatch):
    """Test GVisorSandboxBackend pause/resume handling, state transitions, and error paths."""
    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
    from ray.experimental.sandbox.config import SandboxConfig
    from ray.experimental.sandbox.exceptions import SandboxError

    backend = GVisorSandboxBackend(image_manager=_FakeImageManager())
    config = SandboxConfig(image="fake:latest")
    sandbox_id = "test-sandbox-id"
    root_dir = str(tmp_path / sandbox_id)
    os.makedirs(root_dir, exist_ok=True)
    backend._sandbox_metadata[sandbox_id] = {
        "config": config,
        "root_dir": root_dir,
        "status": SandboxStatus.RUNNING,
    }

    commands_executed = []

    def fake_run(args, **kwargs):
        commands_executed.append(args)

        class Completed:
            returncode = 0
            stdout = ""
            stderr = ""

        return Completed()

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    # 1. Normal pause
    backend.pause_sandbox(sandbox_id)
    assert backend.get_status(sandbox_id) == SandboxStatus.PAUSED
    assert any("pause" in cmd for cmd in commands_executed)

    # 2. Idempotent pause (should not re-run runsc)
    len_before = len(commands_executed)
    backend.pause_sandbox(sandbox_id)
    assert len(commands_executed) == len_before

    # 3. Normal resume
    backend.resume_sandbox(sandbox_id)
    assert backend.get_status(sandbox_id) == SandboxStatus.RUNNING
    assert any("resume" in cmd for cmd in commands_executed)

    # 4. Idempotent resume (should not re-run runsc)
    len_before = len(commands_executed)
    backend.resume_sandbox(sandbox_id)
    assert len(commands_executed) == len_before

    # 5. Terminated sandbox error
    backend._sandbox_metadata[sandbox_id]["status"] = SandboxStatus.TERMINATED
    with pytest.raises(SandboxError, match="stopped unexpectedly"):
        backend.pause_sandbox(sandbox_id)
    with pytest.raises(SandboxError, match="stopped unexpectedly"):
        backend.resume_sandbox(sandbox_id)

    # 6. runsc error raises SandboxError
    backend._sandbox_metadata[sandbox_id]["status"] = SandboxStatus.RUNNING

    def fake_failing_run(args, **kwargs):
        class Failed:
            returncode = 1
            stdout = ""
            stderr = "runsc execution error"

        return Failed()

    monkeypatch.setattr(subprocess, "run", fake_failing_run)
    with pytest.raises(SandboxError, match="Failed to pause"):
        backend.pause_sandbox(sandbox_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
