"""Resource enforcement, CPU choice, and cgroup cleanup without gVisor."""

import fcntl
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from ray.experimental.sandbox._internal import cgroup
from ray.experimental.sandbox.backend import gvisor
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxTimeoutError,
)


@pytest.fixture
def shared(tmp_path, monkeypatch):
    """A fake delegated cgroup v2 subtree on CPUs 0-3, which
    RAY_SANDBOX_CGROUP_PATH names."""
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "cgroup.subtree_control").write_text("cpu memory")
    (shared / "cgroup.procs").write_text("")
    (shared / "cgroup.kill").write_text("")
    (shared / "cpuset.cpus.effective").write_text("0-3")
    monkeypatch.setenv("RAY_SANDBOX_CGROUP_PATH", str(shared))
    monkeypatch.setattr(cgroup, "_is_cgroup2", lambda _: True)
    monkeypatch.setattr(cgroup.sys, "platform", "linux")
    original_mkdir = Path.mkdir

    def mkdir(path, *args, **kwargs):
        original_mkdir(path, *args, **kwargs)
        if path.parent == shared:
            for name in (
                "cpu.max",
                "memory.max",
                "memory.swap.max",
                "memory.oom.group",
                "cgroup.kill",
                "cgroup.procs",
                "cgroup.events",
            ):
                (path / name).write_text(
                    "populated 0" if name == "cgroup.events" else ""
                )

    monkeypatch.setattr(Path, "mkdir", mkdir)
    original_rmdir = Path.rmdir

    def rmdir(path):
        # Real cgroupfs removes the virtual controller files with the directory.
        for child in path.iterdir():
            child.unlink()
        original_rmdir(path)

    monkeypatch.setattr(Path, "rmdir", rmdir)
    return shared


@pytest.fixture
def manager(shared):
    return cgroup.SandboxCgroupManager()


@pytest.fixture
def bundles(tmp_path, monkeypatch):
    """An empty bundle directory, and a fake runsc that logs its arguments.

    Creates start no background reclaim, so only the test reclaims there.
    """
    monkeypatch.setattr(gvisor, "_RAY_SANDBOX_DIR", str(tmp_path / "sandbox"))
    monkeypatch.setattr(
        GVisorSandboxBackend, "_maybe_reclaim_unowned_bundles", lambda self: None
    )
    runsc = tmp_path / "bin" / "runsc"
    runsc.parent.mkdir()
    runsc.write_text(f'#!/bin/sh\necho "$@" >> {tmp_path / "runsc.log"}\n')
    runsc.chmod(0o755)
    monkeypatch.setenv("PATH", f"{runsc.parent}:{os.environ['PATH']}")
    return tmp_path / "sandbox"


def _register(backend):
    """Stand in for _start_sandbox: register a sandbox without starting it."""

    def start(config, sandbox_id, root_dir, owner_file, cgroup):
        backend._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "config": config,
            "cgroup": cgroup,
            "owner_file": owner_file,
        }

    return start


def _stick(path, monkeypatch):
    """Make the cgroup at ``path`` look populated and unkillable."""
    (path / "cgroup.events").write_text("populated 1")
    write = cgroup._write

    def fail(target, value):
        if target == path / "cgroup.kill":
            raise PermissionError("cgroup.kill denied")
        write(target, value)

    monkeypatch.setattr(cgroup, "_write", fail)

    def unstick():
        monkeypatch.setattr(cgroup, "_write", write)
        (path / "cgroup.events").write_text("populated 0")

    return unstick


def test_fractional_cpu_and_memory_are_enforced_together(manager):
    sandbox = manager.allocate("ray-sandbox-000000000001", 0.25, 512 * 1024**2, [])
    assert (sandbox.path / "cpu.max").read_text() == "25000 100000"
    assert (sandbox.path / "memory.max").read_text() == str(512 * 1024**2)
    assert (sandbox.path / "memory.swap.max").read_text() == "0"
    assert (sandbox.path / "memory.oom.group").read_text() == "1"
    (sandbox.path / "cgroup.procs").write_text("321\n")
    sandbox.verify(321)
    sandbox.release()
    assert not sandbox.path.exists()


def test_memory_limit_is_written_in_whole_pages(manager):
    """The kernel rounds memory.max down to whole pages, so a limit in SI
    units, such as 1GB, is written already rounded and verifies."""
    page = os.sysconf("SC_PAGE_SIZE")
    sandbox = manager.allocate("ray-sandbox-000000000001", 0, 10**9, [])
    assert (sandbox.path / "memory.max").read_text() == str(10**9 // page * page)
    (sandbox.path / "cgroup.procs").write_text("321\n")
    sandbox.verify(321)


def test_cpu_limits_choose_a_spread_cpu_subset(manager):
    # At least two CPUs each, least-used first.
    assert manager.choose_cpus(1, []) == [0, 1]
    assert manager.choose_cpus(0.5, [[0, 1]]) == [2, 3]
    assert manager.choose_cpus(1.5, [[0, 1], [2, 3]]) == [0, 1]
    assert manager.choose_cpus(3, [[0, 1]]) == [0, 2, 3]
    # A budget the size of the pool, or no CPU limit, stays unrestricted.
    assert manager.choose_cpus(4, []) == manager.choose_cpus(0, []) == []
    limited = manager.allocate("ray-sandbox-000000000001", 1, 0, [0, 1])
    assert limited.wrap(["runsc"])[4:] == ["--cpus", "0,1", "runsc"]
    whole = manager.allocate("ray-sandbox-000000000002", 4, 0, [])
    assert whole.wrap(["runsc"])[4:] == ["runsc"]


@pytest.mark.skipif(sys.platform != "linux", reason="Needs sched_setaffinity")
def test_launcher_joins_the_cgroup_and_restricts_cpus(tmp_path):
    (tmp_path / "cgroup.procs").write_text("")
    cpu = min(os.sched_getaffinity(0))
    launcher = Path(cgroup.__file__).with_name("cgroup_launcher.py")
    result = subprocess.run(
        [
            sys.executable,
            "-S",
            str(launcher),
            str(tmp_path),
            "--cpus",
            str(cpu),
            sys.executable,
            "-c",
            "import os; print(sorted(os.sched_getaffinity(0)), os.getpid())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    affinity, pid = result.stdout.rsplit(" ", 1)
    assert affinity == str([cpu])
    # The launcher wrote its own PID, which exec kept for the command.
    assert (tmp_path / "cgroup.procs").read_text() == pid.strip()


def test_only_live_owners_cpus_count(shared, bundles):
    """A sandbox's CPUs count while its owner holds the owner file's lock,
    and entries that aren't CPU numbers are ignored."""
    backend = GVisorSandboxBackend(image_manager=Mock())
    config = SandboxConfig("busybox", cpu=1)

    def own(name, cpus):
        (bundles / name).mkdir(parents=True)
        return backend._own_bundle(config, str(bundles / name), cpus)

    live = own("ray-sandbox-00000000000a", [0, 1])
    own("ray-sandbox-00000000000b", [2, 3]).close()  # Its owner died.
    planted = bundles / "ray-sandbox-00000000000c"
    planted.mkdir()
    (planted / ".owner").write_text(json.dumps({"cpus": [[1], "2", True, 3]}))
    try:
        with open(planted / ".owner") as holder:
            fcntl.flock(holder, fcntl.LOCK_EX)
            assert sorted(backend._cpus_in_use()) == [[0, 1], [3]]
        assert backend._cpus_in_use() == [[0, 1]]
    finally:
        live.close()
    assert backend._cpus_in_use() == []


def test_concurrent_creates_spread_over_the_pool(shared, bundles, monkeypatch):
    """Creates pick their CPUs under the node's lock and record them before
    it goes, so creates at the same time, in any process, pick apart."""
    backend = GVisorSandboxBackend(image_manager=Mock())
    monkeypatch.setattr(backend, "_start_sandbox", _register(backend))
    cpus_in_use = backend._cpus_in_use

    def slow_cpus_in_use():
        # Without the lock, the other create would read the same CPUs now.
        used = cpus_in_use()
        time.sleep(0.2)
        return used

    monkeypatch.setattr(backend, "_cpus_in_use", slow_cpus_in_use)
    config = SandboxConfig("busybox", cpu=1, memory="64Mi")
    created = []
    threads = [
        threading.Thread(target=lambda: created.append(backend.create_sandbox(config)))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    try:
        recorded = sorted(
            json.loads((bundles / sandbox_id / ".owner").read_text())["cpus"]
            for sandbox_id in created
        )
        assert recorded == [[0, 1], [2, 3]]
        for sandbox_id in created:
            meta = backend._sandbox_metadata[sandbox_id]
            assert meta["cgroup"].affinity in recorded
            assert (shared / sandbox_id / "cpu.max").read_text() == "100000 100000"
            assert (shared / sandbox_id / "memory.max").read_text() == str(64 << 20)
    finally:
        for sandbox_id in created:
            backend.delete_sandbox(sandbox_id)
    assert not any(path.name.startswith("ray-sandbox-") for path in shared.iterdir())
    assert sorted(os.listdir(bundles)) == ["cpus.lock"]


def test_reclaim_removes_only_a_dead_owners_cgroup(shared, bundles, monkeypatch):
    """The reclaim kills and removes a dead owner's cgroup with its bundle. A
    live owner's cgroup stays, and the owner file can't name another one."""
    images = Mock()
    backend = GVisorSandboxBackend(image_manager=images)
    manager = cgroup.SandboxCgroupManager()
    config = SandboxConfig("busybox", cpu=1)
    sandboxes = {}
    for name in ("ray-sandbox-00000000000a", "ray-sandbox-00000000000b"):
        (bundles / name).mkdir(parents=True)
        sandboxes[name] = (
            backend._own_bundle(config, str(bundles / name), [0, 1]),
            manager.allocate(name, 1, 0, [0, 1]),
        )
    live, live_cgroup = sandboxes["ray-sandbox-00000000000a"]
    dead, dead_cgroup = sandboxes["ray-sandbox-00000000000b"]
    owner = json.loads((bundles / "ray-sandbox-00000000000b" / ".owner").read_text())
    # A planted path for the reclaim to remove instead.
    owner["cgroup"] = str(live_cgroup.path)
    (bundles / "ray-sandbox-00000000000b" / ".owner").write_text(json.dumps(owner))
    dead.close()
    killed = []
    write = cgroup._write

    def recording_write(path, value):
        if path.name == "cgroup.kill":
            killed.append(path.parent.name)
        write(path, value)

    monkeypatch.setattr(cgroup, "_write", recording_write)
    try:
        assert backend._reclaim_unowned_bundles() == ["ray-sandbox-00000000000b"]
        assert killed == ["ray-sandbox-00000000000b"]
        assert not dead_cgroup.path.exists()
        assert live_cgroup.path.exists()
        assert os.listdir(bundles) == ["ray-sandbox-00000000000a"]
        images.release_image.assert_called_once_with(
            "busybox", "ray-sandbox-00000000000b"
        )
    finally:
        live.close()


@pytest.mark.parametrize("stage", ["failed create", "delete"])
def test_a_cgroup_that_cant_be_removed_is_left_to_the_reclaim(
    shared, bundles, monkeypatch, stage
):
    """A sandbox whose cgroup can't be removed keeps its bundle, owner file
    and image pin. Its owner lets go of the owner file, so a reclaim pass
    retries the cgroup, and the pass that removes it deletes the rest."""
    images = Mock()
    backend = GVisorSandboxBackend(image_manager=images)
    started, unstick = [], []

    def start(config, sandbox_id, root_dir, owner_file, cgroup):
        started.append(cgroup)
        if stage == "failed create":
            unstick.append(_stick(cgroup.path, monkeypatch))
            raise SandboxTimeoutError("never reached running")
        _register(backend)(config, sandbox_id, root_dir, owner_file, cgroup)

    monkeypatch.setattr(backend, "_start_sandbox", start)
    config = SandboxConfig("busybox", cpu=1)
    if stage == "failed create":
        with pytest.raises(SandboxTimeoutError, match="never reached running"):
            backend.create_sandbox(config)
    else:
        sandbox_id = backend.create_sandbox(config)
        unstick.append(_stick(started[0].path, monkeypatch))
        backend.delete_sandbox(sandbox_id)
        assert not backend._sandbox_metadata
    (sandbox_id,) = [name for name in os.listdir(bundles) if name != "cpus.lock"]
    assert started[0].path.exists()
    assert (bundles / sandbox_id / ".owner").exists()
    images.release_image.assert_not_called()

    # While it's stuck, a pass keeps everything.
    assert backend._reclaim_unowned_bundles() == []
    assert (bundles / sandbox_id / ".owner").exists()
    unstick[0]()
    assert backend._reclaim_unowned_bundles() == [sandbox_id]
    assert not started[0].path.exists()
    assert os.listdir(bundles) == ["cpus.lock"]
    images.release_image.assert_called_once_with("busybox", sandbox_id)


def test_failed_create_removes_its_cgroup(shared, bundles, monkeypatch):
    images = Mock()
    backend = GVisorSandboxBackend(image_manager=images)
    started = []

    def start(config, sandbox_id, root_dir, owner_file, cgroup):
        started.append(cgroup)
        raise SandboxTimeoutError("never reached running")

    monkeypatch.setattr(backend, "_start_sandbox", start)
    with pytest.raises(SandboxTimeoutError, match="never reached running"):
        backend.create_sandbox(SandboxConfig("busybox", cpu=1))
    assert not started[0].path.exists()
    assert os.listdir(bundles) == ["cpus.lock"]
    images.release_image.assert_called_once()


def test_failed_setup_removes_the_cgroup(manager, monkeypatch):
    write = cgroup._write

    def fail(path, value):
        if path.name == "memory.swap.max":
            raise PermissionError("no swap controller permission")
        write(path, value)

    monkeypatch.setattr(cgroup, "_write", fail)
    with pytest.raises(PermissionError):
        manager.allocate("ray-sandbox-000000000001", 1, 512, [])
    assert not (manager.shared / "ray-sandbox-000000000001").exists()


def test_missing_cgroup_kill_fails_the_check(manager):
    (manager.shared / "cgroup.kill").unlink()
    with pytest.raises(RuntimeError, match="requires cgroup.kill"):
        manager.check(1, 0)


def test_failed_kill_keeps_a_populated_cgroup(manager, monkeypatch):
    sandbox = manager.allocate("ray-sandbox-000000000001", 1, 0, [])
    _stick(sandbox.path, monkeypatch)
    with pytest.raises(PermissionError):
        sandbox.release()
    assert sandbox.path.exists()


def test_verify_rejects_changed_limits_and_wrong_process(manager):
    sandbox = manager.allocate("ray-sandbox-000000000001", 1, 0, [])
    with pytest.raises(RuntimeError, match="isn't in"):
        sandbox.verify(321)
    (sandbox.path / "cpu.max").write_text("max 100000")
    with pytest.raises(RuntimeError, match="cpu.max"):
        sandbox.verify(321)


@pytest.mark.parametrize("cpu", [-1, float("inf"), float("nan"), 0.005])
def test_cpu_limit_must_be_a_valid_quota(cpu):
    with pytest.raises(ValueError):
        SandboxConfig("busybox", cpu=cpu)


def test_backend_rejects_unenforceable_limits_before_touching_the_node(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("RAY_SANDBOX_CGROUP_PATH", raising=False)
    monkeypatch.setenv("RAY_SANDBOX_IGNORE_CGROUPS", "1")
    monkeypatch.setattr("shutil.which", lambda _: "/runsc")
    monkeypatch.setattr(gvisor, "_RAY_SANDBOX_DIR", str(tmp_path / "sandbox"))
    images = Mock()
    with pytest.raises(SandboxCreationError, match="delegated cgroup v2"):
        GVisorSandboxBackend(images).create_sandbox(SandboxConfig("busybox", cpu=1))
    images.pull_image.assert_not_called()
    assert not (tmp_path / "sandbox").exists()


def test_actor_honors_explicit_zero_cpu_limit(monkeypatch):
    from types import SimpleNamespace

    import ray
    from ray.experimental.sandbox.sandbox import Sandbox
    from ray.experimental.sandbox.tests.test_sandbox_runtime import _fake_runtime

    runtime = _fake_runtime()
    monkeypatch.setattr(
        ray,
        "get_runtime_context",
        lambda: SimpleNamespace(get_assigned_resources=lambda: {"CPU": 2}),
    )
    monkeypatch.setattr(
        "ray.experimental.sandbox.sandbox.SandboxRuntime", lambda: runtime
    )
    actor = Sandbox.__ray_metadata__.modified_class(image="fake:latest", cpu=0)
    assert runtime._backend.configs[0].cpu == 0
    actor.delete()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
