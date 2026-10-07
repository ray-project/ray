"""SandboxRuntime-layer tests with fake manager/backend (no runsc needed).

These pin that ``create()`` arguments actually reach the backend's
``SandboxConfig`` — the layer where a declared-but-unforwarded field once
shipped behind a green suite.
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


def test_a_create_no_profile_matches_boots_cold():
    runtime = _pooled_runtime([{"image": "img", "size": 1}])
    for kwargs in ({"image": "other"}, {"image": "img", "cpu": 1.0}):
        instance_id = runtime.create(timeout_seconds=5, **kwargs)
        assert runtime._backend.configs[-1].image == kwargs["image"]
        assert instance_id == f"ray-sandbox-{len(runtime._backend.configs):04d}"


def test_warm_pool_profiles_need_a_size():
    from ray.experimental.sandbox.runtime import _WarmPool

    with pytest.raises(ValueError, match="size"):
        _WarmPool(_fake_runtime(), [{"image": "img"}])


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
    while runtime._warm._booting[next(iter(runtime._warm._profiles))]:
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
