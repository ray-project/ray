"""SandboxRuntime-layer tests with fake manager/backend (no runsc needed).

These pin that ``create()`` arguments actually reach the backend's
``SandboxConfig`` — the layer where a declared-but-unforwarded field once
shipped behind a green suite.
"""

import dataclasses
import os
import sys
import threading
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


class _CountingBackend(_FakeBackend):
    """Unique ids, and the env of each exec."""

    def __init__(self):
        super().__init__()
        self.exec_envs = []

    def create_sandbox(self, config):
        self.configs.append(config)
        return f"ray-sandbox-{len(self.configs):04d}"

    def exec_command(
        self, sandbox_id, command, timeout=None, cwd=None, env=None, **kwargs
    ):
        self.exec_envs.append((sandbox_id, env))
        return ExecResult(exit_code=0, stdout="", stderr="", duration_seconds=0.0)


def _pooled_runtime(profiles):
    from ray.experimental.sandbox.runtime import _WarmPool

    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = _CountingBackend()
    runtime._warm = _WarmPool(runtime, profiles)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    return runtime


def test_warm_pool_hands_out_booted_sandboxes():
    runtime = _pooled_runtime(
        [
            {
                "image": "img",
                "cpu": 0.25,
                "workdir": "/w",
                "env": {"B": "base"},
                "size": 2,
            }
        ]
    )
    booted = {f"ray-sandbox-{i:04d}" for i in (1, 2)}
    instance_id = runtime.create(
        image="img", cpu=0.25, workdir="/w", env={"A": "1"}, timeout_seconds=5
    )
    assert instance_id in booted
    # The create's env reaches every command; the exec's own env wins.
    runtime.exec(instance_id, ["true"], env={"A": "2", "C": "3"})
    assert runtime._backend.exec_envs == [(instance_id, {"A": "2", "C": "3"})]
    runtime.exec(instance_id, ["true"])
    assert runtime._backend.exec_envs[-1] == (instance_id, {"A": "1"})
    # A replacement boots in the background.
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    assert len(runtime._backend.configs) == 3
    runtime.delete(instance_id)
    assert instance_id not in runtime._exec_env


def test_wait_for_warm_pool_retries_a_failed_boot(monkeypatch):
    """A failed boot pauses refills, and no create comes along to restart
    them, so the wait retries the boot itself once the pause is over."""
    from ray.experimental.sandbox.runtime import _WarmPool

    monkeypatch.setattr("ray.experimental.sandbox.runtime._WARM_RETRY_SECONDS", 0.2)

    class _FailsFirstBoot(_CountingBackend):
        def __init__(self):
            super().__init__()
            self.failed = False
            self.fail_lock = threading.Lock()

        def create_sandbox(self, config):
            with self.fail_lock:
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("boot failed")
            return super().create_sandbox(config)

    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = _FailsFirstBoot()
    runtime._warm = _WarmPool(runtime, [{"image": "img", "size": 2}])
    assert runtime.wait_for_warm_pool(timeout_seconds=5)
    assert len(runtime._backend.configs) == 2


def test_a_create_no_profile_matches_boots_cold():
    runtime = _pooled_runtime([{"image": "img", "size": 1}])
    for kwargs in ({"image": "other"}, {"image": "img", "cpu": 1.0}):
        instance_id = runtime.create(timeout_seconds=5, **kwargs)
        assert runtime._backend.configs[-1].image == kwargs["image"]
        assert instance_id == f"ray-sandbox-{len(runtime._backend.configs):04d}"


def test_invalid_warm_pool_profiles_are_refused():
    from ray.experimental.sandbox.runtime import _WarmPool

    with pytest.raises(ValueError, match="size"):
        _WarmPool(_fake_runtime(), [{"image": "img"}])
    # A create couldn't choose between two profiles that differ only in env.
    with pytest.raises(ValueError, match="earlier profile"):
        _WarmPool(
            _fake_runtime(),
            [
                {"image": "img", "size": 1},
                {"image": "img", "env": {"A": "1"}, "size": 2},
            ],
        )


def test_boot_key_covers_every_boot_time_field():
    """Every SandboxConfig field but the per-create ones is in the boot key,
    so no create takes a sandbox booted differently. A new field fails here
    until it is added to the key or declared per-create."""
    from ray.experimental.sandbox.config import SandboxConfig
    from ray.experimental.sandbox.runtime import _boot_key

    # cidr_allowlist needs network="public", and disk a writable rootfs.
    base = {"image": "img", "network": "public", "readonly": False}
    boot_time = {
        "image": "other",
        "cpu": 1.0,
        "memory": "1Gi",
        "workdir": "/w",
        "rootless": False,
        "network": "host",
        "dns": ["1.1.1.1"],
        "cidr_allowlist": ["10.0.0.0/8"],
        "capabilities": [],
        "shell": "/bin/sh",
        "readonly": True,
        "disk": "1Gi",
        "_ignore_cgroups": True,
    }
    per_create = {"env": {"A": "1"}, "ttl_seconds": 5, "timeout_seconds": 1.0}
    fields = {f.name for f in dataclasses.fields(SandboxConfig)}
    assert fields == {*boot_time, *per_create, "_oci_spec_transform_fn"}
    key = _boot_key(SandboxConfig(**base))
    for name, value in boot_time.items():
        assert _boot_key(SandboxConfig(**{**base, name: value})) != key, name
    for name, value in per_create.items():
        assert _boot_key(SandboxConfig(**{**base, name: value})) == key, name
    # An empty allowlist (DNS only) is not the default one (the internet).
    assert _boot_key(SandboxConfig(**base, cidr_allowlist=[])) != key
    assert _boot_key(SandboxConfig(**base, _oci_spec_transform_fn=dict)) is None


def test_close_deletes_the_booted_sandboxes():
    runtime = _pooled_runtime([{"image": "img", "size": 2}])
    runtime.close()
    assert sorted(runtime._backend.deleted) == ["ray-sandbox-0001", "ray-sandbox-0002"]
    # A closed pool hands nothing out: the create boots cold.
    assert runtime.create(image="img", timeout_seconds=5) == "ray-sandbox-0003"


def test_a_failing_profile_is_not_retried_on_every_create():
    from ray.experimental.sandbox.runtime import _WarmPool

    runtime = _fake_runtime()
    runtime._backend = _CountingBackend()
    boots = []

    def broken(config):
        boots.append(config)
        raise RuntimeError("image not found")

    runtime._backend.create_sandbox = broken
    runtime._warm = _WarmPool(runtime, [{"image": "img", "size": 2}])
    deadline = time.monotonic() + 5
    while len(boots) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    key = next(iter(runtime._warm._profiles))
    while runtime._warm._booting[key] and time.monotonic() < deadline:
        time.sleep(0.01)
    runtime._backend.create_sandbox = _CountingBackend.create_sandbox.__get__(
        runtime._backend
    )
    for _ in range(3):
        runtime.create(image="img", timeout_seconds=5)
    time.sleep(0.2)
    # The creates booted cold; the failed profile booted nothing more.
    assert len(boots) == 2
    assert len(runtime._backend.configs) == 3


def test_a_pooled_sandbox_that_died_is_deleted_not_handed_out():
    runtime = _pooled_runtime([{"image": "img", "size": 2}])
    dead = "ray-sandbox-0001"
    runtime._backend.get_status = lambda sandbox_id: (
        SandboxStatus.TERMINATED if sandbox_id == dead else SandboxStatus.RUNNING
    )
    assert runtime.create(image="img", timeout_seconds=5) == "ray-sandbox-0002"
    deadline = time.monotonic() + 5
    while dead not in runtime._backend.deleted and time.monotonic() < deadline:
        time.sleep(0.01)
    assert dead in runtime._backend.deleted


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
