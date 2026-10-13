"""Resource enforcement, allocation races, and crash cleanup without gVisor."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from ray.experimental.sandbox._internal import cgroup
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxTimeoutError,
)


@pytest.fixture
def manager(tmp_path, monkeypatch):
    shared, exclusive = tmp_path / "shared", tmp_path / "exclusive"
    for root in (shared, exclusive):
        root.mkdir()
        (root / "cgroup.subtree_control").write_text("cpu memory cpuset")
        (root / "cgroup.procs").write_text("")
        (root / "cgroup.kill").write_text("")
        (root / "cpuset.cpus.effective").write_text("0-3")
        (root / "cpuset.mems.effective").write_text("0")
        (root / "cpuset.cpus.partition").write_text("root")
    monkeypatch.setattr(cgroup, "_is_cgroup2", lambda _: True)
    monkeypatch.setattr(cgroup.sys, "platform", "linux")
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "12345")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: None)
    monkeypatch.setattr(
        cgroup.SandboxCgroupManager,
        "_cores",
        staticmethod(lambda _: [{0, 2}, {1, 3}]),
    )
    original_mkdir = Path.mkdir

    def mkdir(path, *args, **kwargs):
        original_mkdir(path, *args, **kwargs)
        if path.parent in (shared, exclusive):
            for name in (
                "cpu.max",
                "memory.max",
                "memory.swap.max",
                "memory.oom.group",
                "cpuset.cpus",
                "cpuset.cpus.effective",
                "cpuset.mems",
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
        str(shared), str(exclusive), str(tmp_path / "state")
    )


def test_fractional_cpu_and_memory_are_enforced_together(manager):
    lease = manager.allocate(
        "ray-sandbox-000000000001", 0.25, 512 * 1024**2, "shared"
    )
    assert (lease.path / "cpu.max").read_text() == "25000 100000"
    assert (lease.path / "memory.max").read_text() == str(512 * 1024**2)
    assert (lease.path / "memory.swap.max").read_text() == "0"
    assert (lease.path / "memory.oom.group").read_text() == "1"
    (lease.path / "cgroup.procs").write_text("321\n")
    lease.verify(321)
    lease.release()
    assert not lease.path.exists()


def test_workers_reserve_all_smt_siblings_and_do_not_overlap(manager):
    first = manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    # A second manager models an independent worker process on the same node.
    second_manager = cgroup.SandboxCgroupManager(
        str(manager.shared), str(manager.exclusive), str(manager.state_dir.parent)
    )
    second = second_manager.allocate("ray-sandbox-000000000002", 1, 0, "exclusive")
    assert (first.path / "cpuset.cpus").read_text() == "0"
    assert (second.path / "cpuset.cpus").read_text() == "1"
    records = [r for _, r in cgroup._records(manager.state_dir)]
    assert records[0]["reserved_cpus"] == [0, 2]
    assert records[1]["reserved_cpus"] == [1, 3]
    with pytest.raises(RuntimeError, match="Not enough exclusive"):
        manager.allocate("ray-sandbox-000000000003", 1, 0, "exclusive")
    first.release()
    third = manager.allocate("ray-sandbox-000000000003", 1, 0, "exclusive")
    assert (third.path / "cpuset.cpus").read_text() == "0"


def test_reconcile_kills_orphan_before_reusing_its_cores(manager, monkeypatch):
    first = manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "reused-pid")
    second = manager.allocate("ray-sandbox-000000000002", 2, 0, "exclusive")
    assert not first.path.exists()
    assert (second.path / "cpuset.cpus").read_text() == "0,1"


def test_failed_setup_releases_allocation(manager, monkeypatch):
    write = cgroup._write

    def fail(path, value):
        if path.name == "memory.swap.max":
            raise PermissionError("no swap controller permission")
        write(path, value)

    monkeypatch.setattr(cgroup, "_write", fail)
    with pytest.raises(PermissionError):
        manager.allocate("ray-sandbox-000000000001", 1, 512, "exclusive")
    assert not list(cgroup._records(manager.state_dir))
    assert not (manager.exclusive / "ray-sandbox-000000000001").exists()


def test_missing_cgroup_kill_fails_before_allocating(manager):
    (manager.shared / "cgroup.kill").unlink()
    with pytest.raises(RuntimeError, match="requires cgroup.kill"):
        manager.allocate("ray-sandbox-000000000001", 1, 0, "shared")
    assert not manager.state_dir.exists()


def test_failed_kill_keeps_core_lease(manager, monkeypatch):
    lease = manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    (lease.path / "cgroup.events").write_text("populated 1")
    monkeypatch.setattr(
        cgroup, "_write", lambda *a: (_ for _ in ()).throw(PermissionError())
    )
    with pytest.raises(PermissionError):
        lease.release()
    assert len(list(cgroup._records(manager.state_dir))) == 1


def test_unremovable_orphan_keeps_its_cores_without_blocking_others(
    manager, monkeypatch
):
    stuck = manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    orphan = manager.allocate("ray-sandbox-000000000002", 1, 0, "exclusive")
    (stuck.path / "cgroup.events").write_text("populated 1")
    write = cgroup._write

    def fail(path, value):
        if path == stuck.path / "cgroup.kill":
            raise PermissionError("cgroup.kill denied")
        write(path, value)

    monkeypatch.setattr(cgroup, "_write", fail)
    monkeypatch.setattr(cgroup, "_process_identity", lambda _: "reused-pid")
    # Both owners died. Only the other orphan's core can be reclaimed.
    third = manager.allocate("ray-sandbox-000000000003", 1, 0, "exclusive")
    assert (third.path / "cpuset.cpus").read_text() == "1"
    assert not orphan.path.exists()
    assert stuck.path.exists()
    with pytest.raises(RuntimeError, match="Not enough exclusive"):
        manager.allocate("ray-sandbox-000000000004", 1, 0, "exclusive")


def test_verify_rejects_changed_limits_and_wrong_process(manager):
    lease = manager.allocate("ray-sandbox-000000000001", 1, 0, "shared")
    with pytest.raises(RuntimeError, match="isn't in"):
        lease.verify(321)
    (lease.path / "cpu.max").write_text("max 100000")
    with pytest.raises(RuntimeError, match="cpu.max"):
        lease.verify(321)


def test_exclusive_pool_must_be_valid_and_managed(manager):
    (manager.exclusive / "cpuset.cpus.partition").write_text("root invalid (parent)")
    with pytest.raises(RuntimeError, match="isn't a valid exclusive"):
        manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    (manager.exclusive / "cpuset.cpus.partition").write_text("root")
    # An unrelated process may use every CPU allowed by its parent.
    (manager.exclusive / "unmanaged").mkdir()
    with pytest.raises(RuntimeError, match="Unmanaged cgroups"):
        manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")


def test_invalidated_partition_terminates_existing_lease(manager):
    lease = manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    (manager.exclusive / "cpuset.cpus.partition").write_text("root invalid (hotplug)")
    cgroup._reconcile(manager.state_dir)
    assert not lease.path.exists()


def test_missing_cgroup_is_reconciled_even_when_owner_is_alive(manager):
    lease = manager.allocate("ray-sandbox-000000000001", 1, 0, "exclusive")
    lease.path.rmdir()
    cgroup._reconcile(manager.state_dir)
    assert not list(cgroup._records(manager.state_dir))


@pytest.mark.parametrize("cpu", [0, 0.25, -1, float("inf"), float("nan")])
def test_exclusive_cpu_requires_positive_integer(cpu):
    with pytest.raises(ValueError):
        SandboxConfig("busybox", cpu=cpu, cpu_isolation="exclusive")


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
    monkeypatch.setattr(gvisor.SandboxCgroupManager, "allocate", lambda *_: lease)
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


def test_gofer_cache_is_disabled_only_when_supported():
    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend

    backend = GVisorSandboxBackend()
    config = SandboxConfig("busybox", network="public")
    fresh = backend._build_run_command(config, "/tmp/bundle", "sb-1", fresh_gofer=True)
    assert "--gofer-network-namespace=new" in fresh[2]
    legacy = backend._build_run_command(config, "/tmp/bundle", "sb-1")
    assert "--gofer-network-namespace" not in legacy[2]


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
