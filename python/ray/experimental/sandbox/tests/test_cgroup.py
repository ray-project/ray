"""Resource enforcement, allocation races, and crash cleanup without gVisor."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ray.experimental.sandbox._internal import cgroup
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxTimeoutError,
)

# The manager fixture stubs subprocess.Popen so allocations start no reaper.
_POPEN = subprocess.Popen


@pytest.fixture
def manager(tmp_path, monkeypatch):
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "cgroup.subtree_control").write_text("cpu memory")
    (shared / "cgroup.procs").write_text("")
    (shared / "cgroup.kill").write_text("")
    (shared / "cpuset.cpus.effective").write_text("0-3")
    monkeypatch.setattr(cgroup, "_is_cgroup2", lambda _: True)
    monkeypatch.setattr(cgroup.sys, "platform", "linux")
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "12345")
    monkeypatch.setattr(cgroup, "_boot_id", lambda: "boot-1")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: None)
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
    return cgroup.SandboxCgroupManager(
        shared_path=str(shared), state_path=str(tmp_path / "state")
    )


def test_fractional_cpu_and_memory_are_enforced_together(manager):
    lease = manager.allocate("ray-sandbox-000000000001", 0.25, 512 * 1024**2)
    assert (lease.path / "cpu.max").read_text() == "25000 100000"
    assert (lease.path / "memory.max").read_text() == str(512 * 1024**2)
    assert (lease.path / "memory.swap.max").read_text() == "0"
    assert (lease.path / "memory.oom.group").read_text() == "1"
    (lease.path / "cgroup.procs").write_text("321\n")
    lease.verify(321)
    lease.release()
    assert not lease.path.exists()


def test_shared_sandboxes_run_on_a_spread_cpu_subset(manager):
    first = manager.allocate("ray-sandbox-000000000001", 1, 0)
    # A second manager models an independent worker process on the same node.
    other_worker = cgroup.SandboxCgroupManager(
        shared_path=str(manager.shared), state_path=str(manager.state_dir.parent)
    )
    second = other_worker.allocate("ray-sandbox-000000000002", 0.5, 0)
    third = manager.allocate("ray-sandbox-000000000003", 1.5, 0)
    # At least two CPUs each, least-used first.
    assert (first.affinity, second.affinity, third.affinity) == ([0, 1], [2, 3], [0, 1])
    assert first.wrap(["runsc"])[4:] == ["--cpus", "0,1", "runsc"]
    # A budget the size of the pool, or no CPU limit, stays unrestricted.
    whole = manager.allocate("ray-sandbox-000000000004", 4, 0)
    memory_only = manager.allocate("ray-sandbox-000000000005", 0, 1 << 30)
    assert whole.affinity == memory_only.affinity == []
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


def test_reconcile_kills_orphan_before_reusing_its_cpus(manager, monkeypatch):
    first = manager.allocate("ray-sandbox-000000000001", 1, 0)
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "reused-pid")
    second = manager.allocate("ray-sandbox-000000000002", 1, 0)
    assert not first.path.exists()
    # The orphan's CPUs no longer count as used.
    assert second.affinity == first.affinity == [0, 1]


@pytest.fixture
def fake_runsc(tmp_path, monkeypatch):
    """A ``runsc`` first on PATH that appends its arguments to ``calls``."""
    bin_dir = tmp_path / "fake-runsc-bin"
    bin_dir.mkdir()
    calls = tmp_path / "runsc-calls"
    (bin_dir / "runsc").write_text(f'#!/bin/sh\necho "$@" >> {calls}\n')
    (bin_dir / "runsc").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    return calls


def test_reconcile_deletes_an_orphans_runsc_state_and_files(
    manager, monkeypatch, tmp_path, fake_runsc
):
    sandbox_id = "ray-sandbox-000000000001"
    bundle = tmp_path / "sandbox" / sandbox_id
    bundle.mkdir(parents=True)
    (bundle / ".gvisor.filestore").write_text("overlay")
    pin = tmp_path / "image" / ".users" / sandbox_id
    pin.parent.mkdir(parents=True)
    pin.write_text("")
    runsc_root = tmp_path / "runsc-root"
    runsc_root.mkdir(mode=0o711)
    cleanup = {
        "runsc_root": str(runsc_root),
        "rootless": True,
        "remove": [str(bundle), str(pin)],
    }
    manager.allocate(sandbox_id, 1, 0, cleanup=cleanup)
    monkeypatch.setattr(subprocess, "Popen", _POPEN)
    # A live owner keeps everything.
    with cgroup._locked(manager.state_dir):
        cgroup._reconcile(manager.state_dir)
    assert bundle.exists() and pin.exists() and not fake_runsc.exists()

    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "owner-gone")
    with cgroup._locked(manager.state_dir):
        cgroup._reconcile(manager.state_dir)
    assert fake_runsc.read_text().split() == [
        "--rootless",
        "--ignore-cgroups",
        "--root",
        str(runsc_root),
        "delete",
        "-force",
        sandbox_id,
    ]
    assert not bundle.exists() and not pin.exists()


def test_planted_lease_runs_no_command_and_deletes_only_its_sandbox(
    manager, monkeypatch, tmp_path, fake_runsc
):
    """Leases live under /tmp/ray, which Ray makes world-writable, and the
    reaper may run as root."""
    sandbox_id = "ray-sandbox-000000000001"
    victims = tmp_path / "victims"
    victims.mkdir()
    victim = victims / "victim"
    victim.write_text("keep me")
    marker = tmp_path / "planted-command-ran"
    shared_root = tmp_path / "shared-runsc-root"
    shared_root.mkdir()
    shared_root.chmod(0o777)
    cleanup = {
        "runsc_args": ["/bin/sh", "-c", f"touch {marker}"],
        "runsc_root": str(shared_root),
        "remove": [str(victim), str(victims)],
    }
    manager.allocate(sandbox_id, 1, 0, cleanup=cleanup)
    monkeypatch.setattr(subprocess, "Popen", _POPEN)
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "owner-gone")
    cgroup._reconcile(manager.state_dir)
    assert not marker.exists()
    # runsc never reads state from a directory that others can write.
    assert not fake_runsc.exists()
    assert victim.read_text() == "keep me"
    assert not list(cgroup._records(manager.state_dir))


def _reapers(state_dir) -> list:
    """PIDs of reapers watching ``state_dir``."""
    pids = []
    for entry in Path("/proc").iterdir():
        try:
            argv = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if b"--reap" in argv and str(state_dir).encode() in argv:
            pids.append(int(entry.name))
    return pids


@pytest.mark.skipif(sys.platform != "linux", reason="Needs Linux /proc")
def test_reaper_detaches_from_the_process_that_starts_it(tmp_path):
    """Ray kills a worker's children when the worker exits, which is exactly
    when its sandboxes need reaping, so the reaper mustn't be one of them."""
    state = tmp_path / "state"
    state.mkdir()
    command = [sys.executable, "-S", cgroup.__file__, "--reap", str(state)]
    try:
        subprocess.run([*command, "--detach"], check=True, timeout=10)
        deadline = time.monotonic() + 10
        while not _reapers(state) and time.monotonic() < deadline:
            time.sleep(0.05)
        (reaper,) = _reapers(state)
        ppid = int(
            Path(f"/proc/{reaper}/stat").read_text().rsplit(")", 1)[1].split()[1]
        )
        assert ppid != os.getpid()
        # With the lock held, another start exits at once instead of
        # forking a duplicate.
        subprocess.run([*command, "--detach"], check=True, timeout=10)
        assert _reapers(state) == [reaper]
    finally:
        # A reaper runs until node shutdown, so never leave one behind.
        for pid in _reapers(state):
            os.kill(pid, signal.SIGKILL)


def test_failed_setup_releases_allocation(manager, monkeypatch):
    write = cgroup._write

    def fail(path, value):
        if path.name == "memory.swap.max":
            raise PermissionError("no swap controller permission")
        write(path, value)

    monkeypatch.setattr(cgroup, "_write", fail)
    with pytest.raises(PermissionError):
        manager.allocate("ray-sandbox-000000000001", 1, 512)
    assert not list(cgroup._records(manager.state_dir))
    assert not (manager.shared / "ray-sandbox-000000000001").exists()


def test_missing_cgroup_kill_fails_before_allocating(manager):
    (manager.shared / "cgroup.kill").unlink()
    with pytest.raises(RuntimeError, match="requires cgroup.kill"):
        manager.allocate("ray-sandbox-000000000001", 1, 0)
    assert not manager.state_dir.exists()


def test_failed_kill_keeps_the_lease(manager, monkeypatch):
    lease = manager.allocate("ray-sandbox-000000000001", 1, 0)
    (lease.path / "cgroup.events").write_text("populated 1")
    monkeypatch.setattr(
        cgroup, "_write", lambda *a: (_ for _ in ()).throw(PermissionError())
    )
    with pytest.raises(PermissionError):
        lease.release()
    assert len(list(cgroup._records(manager.state_dir))) == 1


def test_unremovable_orphan_keeps_its_lease_without_blocking_others(
    manager, monkeypatch
):
    stuck = manager.allocate("ray-sandbox-000000000001", 1, 0)
    orphan = manager.allocate("ray-sandbox-000000000002", 1, 0)
    (stuck.path / "cgroup.events").write_text("populated 1")
    write = cgroup._write

    def fail(path, value):
        if path == stuck.path / "cgroup.kill":
            raise PermissionError("cgroup.kill denied")
        write(path, value)

    monkeypatch.setattr(cgroup, "_write", fail)
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "reused-pid")
    # Both owners died. Only the other orphan can be reclaimed, and the
    # stuck one still holds its CPUs.
    third = manager.allocate("ray-sandbox-000000000003", 1, 0)
    assert not orphan.path.exists()
    assert stuck.path.exists()
    assert third.affinity == [2, 3]


def test_verify_rejects_changed_limits_and_wrong_process(manager):
    lease = manager.allocate("ray-sandbox-000000000001", 1, 0)
    with pytest.raises(RuntimeError, match="isn't in"):
        lease.verify(321)
    (lease.path / "cpu.max").write_text("max 100000")
    with pytest.raises(RuntimeError, match="cpu.max"):
        lease.verify(321)


def test_live_owner_keeps_its_lease_after_its_cgroup_is_gone(
    manager, monkeypatch, tmp_path
):
    bundle = tmp_path / "ray-sandbox-000000000001"
    bundle.mkdir()
    lease = manager.allocate(
        "ray-sandbox-000000000001", 1, 0, cleanup={"remove": [str(bundle)]}
    )
    lease.path.rmdir()
    cgroup._reconcile(manager.state_dir)
    # The owner still deletes the sandbox's files, and its CPUs are free.
    assert bundle.exists()
    ((_, record),) = cgroup._records(manager.state_dir)
    assert record["affinity"] == []
    # An owner that dies without deleting the sandbox leaves the files to
    # the reaper.
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "owner-gone")
    cgroup._reconcile(manager.state_dir)
    assert not bundle.exists()


def test_dead_owner_files_are_deleted_after_its_cgroup_is_gone(
    manager, monkeypatch, tmp_path
):
    """The owner can die after release() removed its cgroup but before it
    deleted the bundle."""
    bundle = tmp_path / "ray-sandbox-000000000001"
    bundle.mkdir()
    lease = manager.allocate(
        "ray-sandbox-000000000001", 1, 0, cleanup={"remove": [str(bundle)]}
    )
    lease.path.rmdir()
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "owner-gone")
    cgroup._reconcile(manager.state_dir)
    assert not bundle.exists()
    assert not list(cgroup._records(manager.state_dir))


def test_reboot_ends_ownership_even_for_a_matching_pid_and_start_time(
    manager, monkeypatch, tmp_path
):
    bundle = tmp_path / "ray-sandbox-000000000001"
    bundle.mkdir()
    lease = manager.allocate(
        "ray-sandbox-000000000001", 1, 0, cleanup={"remove": [str(bundle)]}
    )
    # No cgroup survives a reboot, but the bundle can, in a persistent /tmp.
    lease.path.rmdir()
    monkeypatch.setattr(cgroup, "_boot_id", lambda: "boot-2")
    cgroup._reconcile(manager.state_dir)
    assert not bundle.exists()
    assert not list(cgroup._records(manager.state_dir))


@pytest.mark.parametrize("cpu", [-1, float("inf"), float("nan"), 0.005])
def test_cpu_limit_must_be_a_valid_quota(cpu):
    with pytest.raises(ValueError):
        SandboxConfig("busybox", cpu=cpu)


def test_backend_rejects_unenforceable_limits_before_image_pull(monkeypatch):
    from unittest.mock import Mock

    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend

    monkeypatch.delenv("RAY_SANDBOX_CGROUP_PATH", raising=False)
    monkeypatch.setenv("RAY_SANDBOX_IGNORE_CGROUPS", "1")
    monkeypatch.setattr("shutil.which", lambda _: "/runsc")
    images = Mock()
    with pytest.raises(SandboxCreationError, match="delegated cgroup v2"):
        GVisorSandboxBackend(images).create_sandbox(SandboxConfig("busybox", cpu=1))
    images.pull_image.assert_not_called()


def test_failed_create_reports_its_error_when_release_also_fails(tmp_path, monkeypatch):
    from unittest.mock import Mock

    from ray.experimental.sandbox.backend import gvisor

    lease = Mock()
    lease.release.side_effect = RuntimeError("Sandbox cgroup is still populated")
    monkeypatch.setattr(gvisor.SandboxCgroupManager, "allocate", lambda *_, **__: lease)
    monkeypatch.setattr(gvisor, "_RAY_SANDBOX_DIR", str(tmp_path))
    monkeypatch.setattr("shutil.which", lambda _: "/runsc")
    backend = gvisor.GVisorSandboxBackend(Mock())

    def start(config, sandbox_id, root_dir, cgroup):
        os.makedirs(root_dir)
        raise SandboxTimeoutError("never reached running")

    monkeypatch.setattr(backend, "_start_sandbox", start)
    with pytest.raises(SandboxTimeoutError, match="never reached running"):
        backend.create_sandbox(SandboxConfig("busybox", cpu=1))
    lease.release.assert_called_once()
    backend._image_manager.release_image.assert_called_once()
    assert not list(tmp_path.iterdir())


def test_failed_cgroup_release_still_frees_files_and_image(tmp_path, monkeypatch):
    from unittest.mock import Mock

    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend

    monkeypatch.setattr(subprocess, "run", Mock())
    backend = GVisorSandboxBackend(Mock())
    lease = Mock()
    lease.release.side_effect = RuntimeError("Sandbox cgroup is still populated")
    sandbox_id = "ray-sandbox-000000000001"
    root_dir = tmp_path / sandbox_id
    root_dir.mkdir()
    backend._sandbox_metadata[sandbox_id] = {
        "root_dir": str(root_dir),
        "config": SandboxConfig("busybox", cpu=1),
        "cgroup": lease,
    }
    with pytest.raises(RuntimeError, match="still populated"):
        backend.delete_sandbox(sandbox_id)
    assert not root_dir.exists()
    backend._image_manager.release_image.assert_called_once_with("busybox", sandbox_id)
    # The metadata survives, so a retry can still free the lease.
    lease.release.side_effect = None
    backend.delete_sandbox(sandbox_id)
    assert sandbox_id not in backend._sandbox_metadata


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


def test_process_identity_handles_spaces_and_zombies(tmp_path, monkeypatch):
    path = tmp_path / "stat"
    # comm may contain spaces and parentheses; field 22 is the start time.
    path.write_text("42 (worker (name)) S " + "0 " * 18 + "987654 0")
    original_read = cgroup._read
    monkeypatch.setattr(cgroup, "_read", lambda _: original_read(path))
    assert cgroup._process_identity(42) == "987654"
    path.write_text(path.read_text().replace(") S ", ") Z "))
    assert cgroup._process_identity(42) is None


def test_process_identity_treats_only_a_vanished_process_as_dead(monkeypatch):
    def fail(error):
        def read(_):
            raise error

        return read

    # The owner was reaped between open() and read().
    monkeypatch.setattr(cgroup, "_read", fail(ProcessLookupError(3, "ESRCH")))
    assert cgroup._process_identity(42) is None
    # EPERM under hidepid=1 doesn't show that the owner exited.
    monkeypatch.setattr(cgroup, "_read", fail(PermissionError(1, "EPERM")))
    with pytest.raises(PermissionError):
        cgroup._process_identity(42)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
