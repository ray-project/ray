"""SandboxRuntime-layer tests with fake manager/backend (no runsc needed).

These pin that ``create()`` arguments actually reach the backend's
``SandboxConfig`` — the layer where a declared-but-unforwarded field once
shipped behind a green suite.
"""

import asyncio
import concurrent.futures
import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
from unittest.mock import Mock

import pytest

from ray.experimental.sandbox._internal.image_utils import image_cache_name
from ray.experimental.sandbox.backend.base import ExecResult, SandboxStatus
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxError
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
        readonly=False,
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
    assert config.readonly is False
    assert config.shell == "/bin/bash"
    assert config.oom_score_adj == 0


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
    "readonly, target, cached_base, match",
    [
        (True, "registry.example/target:new", True, "readonly=False"),
        (
            False,
            f"registry.example/target@sha256:{'0' * 64}",
            True,
            "needs a tag, not a digest",
        ),
        (False, "registry.example/target:new", False, "Cannot commit base image"),
    ],
)
def test_commit_fails_before_exporting_the_rootfs(
    tmp_path, readonly: bool, target: str, cached_base: bool, match: str
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
    runtime._backend.get_config.return_value = SandboxConfig(
        image=base, readonly=readonly
    )

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


class _CountingBackend(_FakeBackend):
    """Unique ids, the env of each exec, and each assigned config."""

    def __init__(self):
        super().__init__()
        self.exec_envs = []
        self.assigned = []
        self.checkpoints = []
        self.ids_lock = threading.Lock()

    def create_sandbox(self, config):
        # Pool boots run on several threads at once.
        with self.ids_lock:
            self.configs.append(config)
            return f"ray-sandbox-{len(self.configs):04d}"

    def assign_config(self, sandbox_id, config):
        self.assigned.append((sandbox_id, config))

    def exec_command(
        self, sandbox_id, command, timeout=None, cwd=None, env=None, **kwargs
    ):
        self.exec_envs.append((sandbox_id, env))
        return ExecResult(exit_code=0, stdout="", stderr="", duration_seconds=0.0)


def _pooled_runtime(profiles, backend=None):
    from ray.experimental.sandbox.runtime import _WarmPool

    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = backend or _CountingBackend()
    runtime._warm = _WarmPool(runtime, profiles)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    return runtime


def _wait_until(predicate, timeout_seconds=5.0):
    deadline = time.monotonic() + timeout_seconds
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


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
        image="img",
        cpu=0.25,
        workdir="/w",
        env={"A": "1"},
        ttl_seconds=60,
        timeout_seconds=5,
    )
    assert instance_id in booted
    # The backend takes the create's config, env and TTL included, so every
    # reader sees it; the runtime keeps no env of its own.
    ((assigned_id, config),) = runtime._backend.assigned
    assert assigned_id == instance_id
    assert (config.env, config.ttl_seconds) == ({"A": "1"}, 60)
    runtime.exec(instance_id, ["true"], env={"C": "3"})
    assert runtime._backend.exec_envs == [(instance_id, {"C": "3"})]
    # Taking a sandbox boots nothing: its slot stays the create's until the
    # sandbox is deleted, and only then does a new one boot in it.
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    time.sleep(0.1)
    assert len(runtime._backend.configs) == 2
    runtime.delete(instance_id)
    assert _wait_until(lambda: len(runtime._backend.configs) == 3)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    runtime.close()


def test_assigned_config_reaches_get_config_exec_and_checkpoints(tmp_path, monkeypatch):
    """A pooled sandbox handed to a create stores the create's config, with
    its env on top of the boot's, and runs every command with that env: the
    backend's own exec_command, checkpoints and get_config all see it."""
    import subprocess

    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
    from ray.experimental.sandbox.config import SandboxConfig

    backend = GVisorSandboxBackend(image_manager=_FakeImageManager())
    booted = SandboxConfig(image="img", env={"A": "boot", "B": "boot"})
    backend._sandbox_metadata["sb-1"] = {
        "config": booted,
        "root_dir": str(tmp_path),
        "cwd": "/",
        "status": SandboxStatus.RUNNING,
    }
    create = dataclasses.replace(booted, env={"A": "create"}, ttl_seconds=60)
    backend.assign_config("sb-1", create)

    config = backend._sandbox_metadata["sb-1"]["config"]
    assert config.env == {"A": "create", "B": "boot"}
    assert config.ttl_seconds == 60

    launched = []

    class _Done:
        returncode = 0

        def __init__(self, args, **kwargs):
            launched.append(args)

        def communicate(self, timeout=None):
            return b"", b""

    monkeypatch.setattr(subprocess, "Popen", _Done)
    backend.exec_command("sb-1", ["env"], env={"C": "exec"})
    backend.exec_command("sb-1", ["env"], env={"A": "exec"})
    envs = [
        [args[i + 1] for i, arg in enumerate(args) if arg == "-env"]
        for args in launched
    ]
    # The boot's env is in the sandbox's spec; the create's is passed on
    # every command, under the command's own.
    assert envs == [["A=create", "C=exec"], ["A=exec"]]


def test_a_failed_boot_is_retried_once_the_pause_is_over(monkeypatch):
    """A failed boot pauses its profile's refills, and a timer refills it
    once the pause is over, with no create or retry loop to restart it."""
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
        "gpu_ids": ["0"],
        "mounts": {"/data": "/srv/data"},
        "oom_score_adj": 0,
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


def test_boot_key_matches_a_gpu_sandboxs_driver_capabilities():
    """A GPU sandbox's NVIDIA_DRIVER_CAPABILITIES sets runsc's allowed driver
    capabilities at boot, so it is part of the boot key; elsewhere env stays
    per-create."""
    from ray.experimental.sandbox.config import SandboxConfig
    from ray.experimental.sandbox.runtime import _boot_key

    caps = {"NVIDIA_DRIVER_CAPABILITIES": "compute,utility,profiling"}
    gpu = {"image": "img", "gpu_ids": ["0"]}
    assert _boot_key(SandboxConfig(**gpu, env=caps)) != _boot_key(SandboxConfig(**gpu))
    assert _boot_key(SandboxConfig(**gpu, env={**caps, "A": "1"})) == _boot_key(
        SandboxConfig(**gpu, env=caps)
    )
    assert _boot_key(SandboxConfig(image="img", env=caps)) == _boot_key(
        SandboxConfig(image="img")
    )


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
    (slots,) = runtime._warm._slots.values()
    assert _wait_until(lambda: not any(slot.state == "booting" for slot in slots))
    # A boot still queued when the first one failed is skipped.
    attempts = len(boots)
    assert 1 <= attempts <= 2
    runtime._backend.create_sandbox = _CountingBackend.create_sandbox.__get__(
        runtime._backend
    )
    for _ in range(3):
        runtime.create(image="img", timeout_seconds=5)
    time.sleep(0.2)
    # The creates booted cold; the failed profile booted nothing more.
    assert len(boots) == attempts
    assert len(runtime._backend.configs) == 3
    runtime.close()


def test_warm_pool_boots_on_a_few_threads():
    """Filling a large pool runs _WARM_BOOT_CONCURRENCY boots at a time on
    as many threads, rather than parking a thread per missing sandbox."""
    from ray.experimental.sandbox.runtime import _WARM_BOOT_CONCURRENCY, _WarmPool

    release = threading.Event()

    class _SlowBackend(_CountingBackend):
        def __init__(self):
            super().__init__()
            self.running = 0
            self.most_running = 0
            self.count_lock = threading.Lock()

        def create_sandbox(self, config):
            with self.count_lock:
                self.running += 1
                self.most_running = max(self.most_running, self.running)
            release.wait(10)
            with self.count_lock:
                self.running -= 1
                self.configs.append(config)
                return f"ray-sandbox-{len(self.configs):04d}"

    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = _SlowBackend()
    runtime._warm = _WarmPool(runtime, [{"image": "img", "size": 24}])
    deadline = time.monotonic() + 5
    while (
        runtime._backend.running < _WARM_BOOT_CONCURRENCY
        and time.monotonic() < deadline
    ):
        time.sleep(0.01)
    assert len(runtime._warm._boots._threads) == _WARM_BOOT_CONCURRENCY
    assert not runtime.wait_for_warm_pool(timeout_seconds=0.1)
    release.set()
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    assert runtime._backend.most_running == _WARM_BOOT_CONCURRENCY
    assert len(runtime._backend.configs) == 24
    runtime.close()


def test_a_pooled_sandbox_that_died_is_deleted_not_handed_out():
    runtime = _pooled_runtime([{"image": "img", "size": 2}])
    # The boots run in parallel, so the pool's order is the order they ended.
    (pool,) = runtime._warm._ready.values()
    dead, alive = (slot.sandbox_id for slot in pool)
    runtime._backend.get_status = lambda sandbox_id: (
        SandboxStatus.TERMINATED if sandbox_id == dead else SandboxStatus.RUNNING
    )
    assert runtime.create(image="img", timeout_seconds=5) == alive
    assert _wait_until(lambda: dead in runtime._backend.deleted)
    # Only the dead sandbox's slot boots again; the taken one's stays claimed.
    assert _wait_until(lambda: len(runtime._backend.configs) == 3)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    time.sleep(0.1)
    assert len(runtime._backend.configs) == 3
    runtime.close()


class _PoolCountingBackend(_CountingBackend):
    """Counts the pool's sandboxes alive at once: those its boot threads made."""

    def __init__(self):
        super().__init__()
        self.pooled = set()
        self.live = set()
        self.most_pooled_live = 0

    def create_sandbox(self, config):
        instance_id = super().create_sandbox(config)
        with self.ids_lock:
            self.live.add(instance_id)
            if threading.current_thread().name.startswith("sandbox-warm-pool"):
                self.pooled.add(instance_id)
            self.most_pooled_live = max(
                self.most_pooled_live, len(self.pooled & self.live)
            )
        return instance_id

    def delete_sandbox(self, sandbox_id):
        super().delete_sandbox(sandbox_id)
        with self.ids_lock:
            self.live.discard(sandbox_id)


def test_a_warm_pool_never_holds_more_than_its_size():
    """Taking a sandbox boots nothing and deleting one boots its slot's next,
    so a profile's sandboxes, ready or in use, never outnumber its size.
    A create that finds none ready boots cold, outside the pool."""
    backend = _PoolCountingBackend()
    runtime = _pooled_runtime([{"image": "img", "size": 2}], backend=backend)
    first = runtime.create(image="img", timeout_seconds=5)
    second = runtime.create(image="img", timeout_seconds=5)
    assert {first, second} == backend.pooled
    cold = runtime.create(image="img", timeout_seconds=5)
    assert cold not in backend.pooled
    time.sleep(0.1)
    assert len(backend.configs) == 3
    # A cold sandbox has no slot to free.
    runtime.delete(cold)
    time.sleep(0.1)
    assert len(backend.configs) == 3
    runtime.delete(first)
    assert _wait_until(lambda: len(backend.configs) == 4)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    assert backend.most_pooled_live == 2
    runtime.close()


def test_concurrent_creates_and_deletes_keep_the_pool_at_its_size():
    backend = _PoolCountingBackend()
    runtime = _pooled_runtime([{"image": "img", "size": 4}], backend=backend)

    def churn():
        for _ in range(25):
            runtime.delete(runtime.create(image="img", timeout_seconds=5))

    threads = [threading.Thread(target=churn) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    # Everything the creates took is deleted; the pool holds its 4 again.
    assert len(backend.live) == 4 and backend.live <= backend.pooled
    assert backend.most_pooled_live <= 4
    runtime.close()
    assert not backend.live


def test_a_pooled_sandbox_whose_ttl_expires_frees_its_slot():
    runtime = _pooled_runtime([{"image": "img", "size": 1}])
    instance_id = runtime.create(image="img", ttl_seconds=1, timeout_seconds=5)
    assert _wait_until(lambda: instance_id in runtime._backend.deleted)
    assert _wait_until(lambda: len(runtime._backend.configs) == 2)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    runtime.close()


def test_a_checkpoint_that_stops_a_pooled_sandbox_frees_its_slot():
    runtime = _pooled_runtime([{"image": "img", "size": 1}])
    instance_id = runtime.create(image="img", timeout_seconds=5)
    runtime.checkpoint(instance_id, checkpoint_path="/tmp/ckpt", leave_running=False)
    assert _wait_until(lambda: len(runtime._backend.configs) == 2)
    runtime.close()


class _CheckpointFails(_CountingBackend):
    """Fails every checkpoint, leaving the sandbox deleted, stopped (but still
    tracked), or running."""

    def __init__(self, outcome):
        super().__init__()
        self.outcome = outcome
        self.stopped = set()

    def get_status(self, sandbox_id):
        if sandbox_id in self.deleted or sandbox_id in self.stopped:
            return SandboxStatus.TERMINATED
        return SandboxStatus.RUNNING

    def get_termination_reason(self, sandbox_id):
        if sandbox_id in self.stopped and sandbox_id not in self.deleted:
            return "Sandbox runtime stopped unexpectedly"
        return None

    def checkpoint_sandbox(self, sandbox_id, **kwargs):
        if self.outcome == "deleted":
            self.delete_sandbox(sandbox_id)
        elif self.outcome == "stopped":
            self.stopped.add(sandbox_id)
        raise RuntimeError("checkpoint failed")


@pytest.mark.parametrize("outcome", ["deleted", "stopped", "running"])
@pytest.mark.parametrize("leave_running", [False, True])
def test_a_failed_checkpoint_frees_the_slot_only_of_a_deleted_sandbox(
    outcome, leave_running
):
    """A checkpoint that fails after the backend deleted the sandbox frees its
    slot. One that leaves the sandbox stopped, but still holding its bundle,
    cgroup and image, or running, leaves the slot to it until it's deleted."""
    runtime = _pooled_runtime(
        [{"image": "img", "size": 1}], backend=_CheckpointFails(outcome)
    )
    instance_id = runtime.create(image="img", timeout_seconds=5, ttl_seconds=300)
    with pytest.raises(RuntimeError, match="checkpoint failed"):
        runtime.checkpoint(
            instance_id, checkpoint_path="/tmp/ckpt", leave_running=leave_running
        )
    if outcome == "deleted":
        assert instance_id not in runtime._ttl_timers
    else:
        time.sleep(0.1)
        assert len(runtime._backend.configs) == 1
        assert instance_id in runtime._ttl_timers
        runtime.delete(instance_id)
    assert _wait_until(lambda: len(runtime._backend.configs) == 2)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    runtime.close()


def test_close_leaves_taken_sandboxes_and_boots_nothing_after():
    runtime = _pooled_runtime([{"image": "img", "size": 2}])
    taken = runtime.create(image="img", timeout_seconds=5)
    runtime.close()
    (ready,) = runtime._backend.deleted
    assert ready != taken
    runtime.delete(taken)
    time.sleep(0.1)
    assert len(runtime._backend.configs) == 2


def test_a_pooled_sandbox_that_cant_be_handed_over_frees_its_slot():
    """If assign_config fails after take claimed a sandbox (it died in
    between, say), the create deletes it, which boots a new one in its slot,
    and boots cold itself instead of failing."""
    runtime = _pooled_runtime([{"image": "img", "size": 1}])
    (pooled,) = (
        slot.sandbox_id for pool in runtime._warm._ready.values() for slot in pool
    )

    def broken(sandbox_id, config):
        raise RuntimeError(f"Sandbox ID '{sandbox_id}' not found.")

    runtime._backend.assign_config = broken
    instance_id = runtime.create(image="img", timeout_seconds=5)
    assert instance_id != pooled
    assert pooled in runtime._backend.deleted
    # The cold boot, then the slot's new sandbox.
    assert _wait_until(lambda: len(runtime._backend.configs) == 3)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    runtime.close()


def test_a_boot_that_ends_after_close_is_deleted(monkeypatch):
    """A boot still running at close deletes its sandbox when it ends, and a
    failed delete is logged rather than lost in the boot thread."""
    from ray.experimental.sandbox import runtime as runtime_module
    from ray.experimental.sandbox.runtime import _WarmPool

    warnings = []
    # Ray's loggers don't propagate to the root logger that caplog watches.
    monkeypatch.setattr(
        runtime_module.logger, "warning", lambda msg, *args: warnings.append(msg % args)
    )

    booting = threading.Event()
    release = threading.Event()

    class _SlowBackend(_CountingBackend):
        def create_sandbox(self, config):
            booting.set()
            release.wait(10)
            return super().create_sandbox(config)

        def delete_sandbox(self, sandbox_id):
            super().delete_sandbox(sandbox_id)
            raise RuntimeError("runsc delete failed")

    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = _SlowBackend()
    runtime._warm = _WarmPool(runtime, [{"image": "img", "size": 1}])
    assert booting.wait(5)
    runtime.close()
    release.set()
    assert _wait_until(lambda: runtime._backend.deleted == ["ray-sandbox-0001"])
    assert _wait_until(lambda: any("runsc delete failed" in w for w in warnings))


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
