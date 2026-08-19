"""SandboxRuntime-layer tests with fake manager/backend (no runsc needed).

The mount_workdir plumbing bug (the field was declared on ``create()`` but
never forwarded into the ``SandboxConfig``) survived a green suite because
every test exercised ``create_oci_spec`` directly. These tests pin the layer
that bug lived in: config fields must actually reach the backend.
"""

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


def _fake_runtime():
    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = _FakeBackend()
    return runtime


def test_create_forwards_config_fields_to_backend():
    runtime = _fake_runtime()
    runtime.create(
        image="fake:latest",
        cpu=2.0,
        memory="1Gi",
        env={"A": "1"},
        workdir="/app",
        mount_workdir=False,
        network="host",
        capabilities=["CAP_CHOWN"],
        rootless=True,
        readonly=False,
    )
    (config,) = runtime._backend.configs
    assert config.image == "fake:latest"
    assert config.cpu == 2.0
    assert config.env == {"A": "1"}
    assert config.workdir == "/app"
    # The regression this file exists for: the field must reach the config.
    assert config.mount_workdir is False
    assert config.network == "host"
    assert config.capabilities == ["CAP_CHOWN"]
    assert config.rootless is True
    assert config.readonly is False


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
