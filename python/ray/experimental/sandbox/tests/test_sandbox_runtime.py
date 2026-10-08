"""SandboxRuntime-layer tests with fake manager/backend (no runsc needed).

These pin that ``create()`` arguments actually reach the backend's
``SandboxConfig`` — the layer where a declared-but-unforwarded field once
shipped behind a green suite.
"""

import os
import sys
import time

import pytest

from ray.experimental.sandbox.backend.base import ExecResult, SandboxStatus
from ray.experimental.sandbox.runtime import SandboxRuntime


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

    def exec_command(self, sandbox_id, command, timeout=None, cwd=None, env=None):
        return ExecResult(exit_code=0, stdout="", stderr="", duration_seconds=0.0)

    def write_file(self, sandbox_id, path, content):
        pass

    def read_file(self, sandbox_id, path):
        return b""

    def get_status(self, sandbox_id):
        return SandboxStatus.RUNNING

    def pause_sandbox(self, sandbox_id: str, timeout_seconds=None):
        self.paused.append(sandbox_id)

    def resume_sandbox(self, sandbox_id: str, timeout_seconds=None):
        self.resumed.append(sandbox_id)


def _fake_runtime():
    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    backend = _FakeBackend()
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
        readonly=False,
        shell="/bin/bash",
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
    assert config.readonly is False
    assert config.shell == "/bin/bash"


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


def test_pause_and_resume_forward_to_backend():
    runtime = _fake_runtime()
    instance_id = runtime.create(image="fake:latest")

    runtime.pause(instance_id, timeout_seconds=5.0)
    assert runtime._backend.paused == [instance_id]

    runtime.resume(instance_id, timeout_seconds=5.0)
    assert runtime._backend.resumed == [instance_id]


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
    with pytest.raises(SandboxError, match="Cannot pause terminated"):
        backend.pause_sandbox(sandbox_id)
    with pytest.raises(SandboxError, match="Cannot resume terminated"):
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
