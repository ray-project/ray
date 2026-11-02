import contextlib
import ctypes
import errno
import hashlib
import ipaddress
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import ray
from ray.actor import ActorHandle
from ray.experimental.sandbox import create
from ray.experimental.sandbox._internal import image_utils, overlayfs
from ray.experimental.sandbox._internal.image_utils import (
    DEFAULT_IMAGES_DIR,
    image_cache_name,
)
from ray.experimental.sandbox._internal.overlayfs import (
    ImageMountMode,
    UserNamespaceType,
)
from ray.experimental.sandbox.backend import gvisor
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import (
    _RESOLVER_PROBE,
    _RESOLVER_UNREACHABLE,
    GVisorSandboxBackend,
    _exec_processes,
    _holder_namespace_ids,
    _namespace_ids,
    _render_egress_ruleset,
    _write_egress_ruleset,
)
from ray.experimental.sandbox.config import (
    ALLOW_ALL_CIDRS,
    DEFAULT_PUBLIC_DNS,
    INTERNET_EXCLUDED_IPV4,
    GVisorSandboxConfig,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.runtime import SandboxRuntime

_BUSYBOX_CACHE = os.path.join(DEFAULT_IMAGES_DIR, image_cache_name("busybox:latest"))


def test_gvisor_backend_local_lifecycle_and_file_ops():
    backend = GVisorSandboxBackend()
    # Positive limits need a delegated cgroup v2 subtree. On a provisioned
    # node, file operations and execs also run through the cgroup launcher.
    limits = (
        {"cpu": 1.0, "memory": "512Mi"}
        if os.environ.get("RAY_SANDBOX_CGROUP_PATH")
        else {}
    )
    config = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        **limits,
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sandbox-")
    assert sandbox_id in backend._sandbox_metadata
    assert backend.get_status(sandbox_id) == SandboxStatus.RUNNING

    # Test file write and read
    backend.write_file(sandbox_id, "/workspace/script.py", "print('Hello gVisor')")
    content = backend.read_file(sandbox_id, "/workspace/script.py")
    assert content == b"print('Hello gVisor')"

    # Test exec command
    res = backend.exec_command(sandbox_id, "echo 'Process isolation'")
    assert res.exit_code == 0
    assert "Process isolation" in res.stdout

    # Test delete
    backend.delete_sandbox(sandbox_id)
    assert backend.get_status(sandbox_id) == SandboxStatus.TERMINATED
    assert sandbox_id not in backend._sandbox_metadata


def test_gvisor_backend_not_found():
    backend = GVisorSandboxBackend()
    with pytest.raises(SandboxNotFoundError):
        backend.exec_command("nonexistent-id", "echo 'hi'")


def test_create_sandbox_helper():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    sb = create("busybox:latest", workdir="/workspace", shell="/bin/sh")
    assert isinstance(sb, ActorHandle)
    res = ray.get(sb.exec.remote("echo 'Process isolation'"))
    assert res.exit_code == 0
    assert "Process isolation" in res.stdout
    assert res.duration_ms >= 0
    ray.get(sb.terminate.remote())


def test_gvisor_backend_container_image_support():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
    )
    sandbox_id = backend.create_sandbox(config)
    try:
        assert sandbox_id.startswith("ray-sandbox-")
        assert backend.get_status(sandbox_id) == SandboxStatus.RUNNING

        extracted_dir = _BUSYBOX_CACHE
        assert os.path.exists(extracted_dir)
        assert os.path.isdir(extracted_dir)
        assert os.path.exists(os.path.join(extracted_dir, ".extracted"))
        # Only the EROFS image is cached: no extracted tree, and no archive
        # doubling its footprint.
        assert os.path.isfile(os.path.join(extracted_dir, "rootfs.erofs"))
        assert not os.path.exists(os.path.join(extracted_dir, "rootfs"))
        assert not os.path.exists(_BUSYBOX_CACHE + ".tar")

        res = backend.exec_command(sandbox_id, "/bin/sh -c 'echo hello from busybox'")
        assert res.exit_code == 0
        assert "hello from busybox" in res.stdout
    finally:
        backend.delete_sandbox(sandbox_id)

    assert os.path.exists(_BUSYBOX_CACHE)


def test_gvisor_backend_image_required():
    with pytest.raises((TypeError, ValueError)):
        GVisorSandboxConfig(
            image=None,
            workdir="/workspace",
        )
    with pytest.raises((TypeError, ValueError)):
        GVisorSandboxConfig(
            workdir="/workspace",
        )


def test_gvisor_backend_invalid_image():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="nonexistent_invalid_image_12345:latest",
        workdir="/workspace",
    )
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(config)


def test_gvisor_backend_container_image_overlay_isolation():
    backend = GVisorSandboxBackend()
    cfg1 = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        readonly=False,
    )
    cfg2 = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        readonly=False,
    )

    sb1 = backend.create_sandbox(cfg1)
    sb2 = backend.create_sandbox(cfg2)
    try:
        # SB1 writes to rootfs
        res1 = backend.exec_command(
            sb1, "/bin/sh -c 'echo sb1_root > /overlay_test.txt'"
        )
        assert res1.exit_code == 0

        # SB2 writes to rootfs with different content
        res2 = backend.exec_command(
            sb2, "/bin/sh -c 'echo sb2_root > /overlay_test.txt'"
        )
        assert res2.exit_code == 0

        # Verify SB1 sees sb1_root
        read1 = backend.exec_command(sb1, "cat /overlay_test.txt")
        assert read1.exit_code == 0
        assert "sb1_root" in read1.stdout

        # Verify SB2 sees sb2_root
        read2 = backend.exec_command(sb2, "cat /overlay_test.txt")
        assert read2.exit_code == 0
        assert "sb2_root" in read2.stdout
    finally:
        backend.delete_sandbox(sb1)
        backend.delete_sandbox(sb2)

    # A newly created SB3 should not see /overlay_test.txt
    cfg3 = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        readonly=False,
    )
    sb3 = backend.create_sandbox(cfg3)
    try:
        read3 = backend.exec_command(sb3, "/bin/sh -c 'test -f /overlay_test.txt'")
        assert read3.exit_code != 0
    finally:
        backend.delete_sandbox(sb3)


# readonly=True with an explicit workdir boots from a kernel overlay, where
# runsc can create the workdir's mount point, so the rest of the rootfs stays
# read-only. A worker that can't mount one runs the sandbox on a private
# writable overlay instead.
def test_gvisor_backend_readonly_rootfs(ensure_overlayfs_mount):
    backend = GVisorSandboxBackend()
    # Default is readonly=True
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
    )
    assert cfg.readonly is True
    sandbox_id = backend.create_sandbox(cfg)
    try:
        # Writing to rootfs should fail because readonly=True by default
        res = backend.exec_command(
            sandbox_id, "/bin/sh -c 'echo test > /test_readonly.txt'"
        )
        assert res.exit_code != 0
        assert "Read-only file system" in res.stderr

        # Writing to /workspace should still succeed because it is mounted rw
        res_ws = backend.exec_command(
            sandbox_id,
            "/bin/sh -c 'echo ws_ok > /workspace/ws.txt && cat /workspace/ws.txt'",
        )
        assert res_ws.exit_code == 0
        assert "ws_ok" in res_ws.stdout
    finally:
        backend.delete_sandbox(sandbox_id)


def test_gvisor_backend_ignore_cgroups_flag():
    backend = GVisorSandboxBackend()
    cfg_default = GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh")
    orig_env = os.environ.pop("RAY_SANDBOX_IGNORE_CGROUPS", None)
    try:
        args_default = backend._runsc_base_args(cfg_default)
        assert "--ignore-cgroups" not in args_default

        cfg_ignored = GVisorSandboxConfig(
            image="busybox:latest", shell="/bin/sh", _ignore_cgroups=True
        )
        args_ignored = backend._runsc_base_args(cfg_ignored)
        assert "--ignore-cgroups" in args_ignored
    finally:
        if orig_env is not None:
            os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = orig_env


def test_string_exec_shell_configuration():
    """String commands run under config.shell (default /bin/bash) with a
    per-exec override; there is no auto-detection."""
    # busybox has /bin/sh but no /bin/bash: with the deterministic bash
    # default a string exec fails loudly instead of degrading to sh, so this
    # image configures the shell explicitly.
    runtime = SandboxRuntime()
    instance_id = runtime.create(
        image="busybox:latest", readonly=False, shell="/bin/sh"
    )
    try:
        result = runtime.exec(instance_id, "echo hello-$0")
        assert result.exit_code == 0
        assert "hello-" in result.stdout
        # Per-exec override beats the configured shell.
        result = runtime.exec(instance_id, "echo again", shell="/bin/sh")
        assert result.exit_code == 0
    finally:
        runtime.delete(instance_id)


def test_disk_caps_the_writable_overlay():
    runtime = SandboxRuntime()
    instance_id = runtime.create(
        image="busybox:latest", shell="/bin/sh", readonly=False, disk="64Mi"
    )
    try:
        result = runtime.exec(instance_id, "dd if=/dev/zero of=/fill bs=1M count=128")
        assert result.exit_code != 0
        assert "No space left on device" in result.stderr
        # Deleting frees the space, and the sandbox keeps working.
        result = runtime.exec(
            instance_id,
            "rm /fill && dd if=/dev/zero of=/again bs=1M count=32 && echo ok",
        )
        assert result.stdout.endswith("ok\n"), result.stderr
    finally:
        runtime.delete(instance_id)


def test_disk_caps_a_readonly_sandbox_with_a_workdir(ensure_overlayfs_mount):
    """A readonly sandbox's cap bounds its workdir, whose writes go to a file
    in the sandbox's directory on the node. The rest of the rootfs stays
    read-only."""
    runtime = SandboxRuntime()
    instance_id = runtime.create(
        image="busybox:latest", shell="/bin/sh", workdir="/workspace", disk="64Mi"
    )
    try:
        result = runtime.exec(
            instance_id, "dd if=/dev/zero of=/workspace/fill bs=1M count=128"
        )
        assert result.exit_code != 0
        assert "No space left on device" in result.stderr
        result = runtime.exec(instance_id, "dd if=/dev/zero of=/fill bs=1M count=8")
        assert result.exit_code != 0
        assert "Read-only file system" in result.stderr
    finally:
        runtime.delete(instance_id)


# A createContainer hook that writes a file into the sandbox's rootfs, read
# from the bundle path in the container state runsc passes it on stdin.
_WRITE_HOOKED_FILE = (
    'b=$(sed -n \'s/.*"bundle": *"\\([^"]*\\)".*/\\1/p\'); '
    'echo hooked > "$b/rootfs/hooked"'
)


def _add_createcontainer_hook(spec):
    hooks = spec.setdefault("hooks", {}).setdefault("createContainer", [])
    hooks.append({"path": "/bin/sh", "args": ["sh", "-c", _WRITE_HOOKED_FILE]})
    return spec


@pytest.mark.skipif(
    os.geteuid() != 0, reason="checkpoints need rootless=False, which needs root"
)
def test_checkpoint_restore_keeps_an_overlayfs_sandboxs_files(
    ensure_overlayfs_mount, tmp_path
):
    """An overlayfs sandbox's own writes come back from gVisor's checkpoint
    image, and files its hooks write come back because runsc reruns its hooks
    on restore, both when it's restored in place and into a new sandbox."""
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        rootless=False,
        readonly=False,
        _oci_spec_transform_fn=_add_createcontainer_hook,
    )
    sandbox_ids = [backend.create_sandbox(config)]
    try:
        assert backend._sandbox_metadata[sandbox_ids[0]]["rootfs_overlay"] is not None
        assert (
            backend.exec_command(sandbox_ids[0], "echo inside > /inside").exit_code == 0
        )
        backend.checkpoint_sandbox(
            sandbox_ids[0], checkpoint_path=str(tmp_path / "checkpoint")
        )
        sandbox_ids.append(
            backend.restore_sandbox(
                str(tmp_path / "checkpoint"),
                _oci_spec_transform_fn=_add_createcontainer_hook,
            )
        )
        for sandbox_id in sandbox_ids:
            result = backend.exec_command(sandbox_id, "cat /hooked /inside")
            assert result.stdout.split() == ["hooked", "inside"], result.stderr
    finally:
        for sandbox_id in sandbox_ids:
            backend.delete_sandbox(sandbox_id)


def _procfs_dump(*procs):
    """What ``runsc trace procfs`` prints for (pid, ppid, pgid, sid) tuples."""
    lines = ["PROCFS DUMP"]
    for pid, ppid, pgid, sid in procs:
        status, stat = {"pid": pid, "ppid": ppid}, {"pgid": pgid, "sid": sid}
        lines += ["", json.dumps({"status": status, "stat": stat})]
    return "\n".join(lines) + "\n"


def test_exec_processes_follows_the_session_and_children():
    dump = _procfs_dump(
        (1, 0, 1, 1),  # The sandbox's init.
        (2, 0, 2, 2),  # The command, a session leader.
        (3, 2, 2, 2),  # Its child,
        (4, 3, 4, 2),  # a grandchild in a group of its own,
        (5, 1, 2, 2),  # and reparented children, in the command's group
        (6, 1, 6, 2),  # or in a group of their own.
        (7, 3, 7, 7),  # A child that called setsid(), and its child.
        (8, 7, 7, 7),
        (9, 1, 9, 9),  # A daemon: it called setsid() and its parent exited.
        (10, 0, 10, 10),  # Another command and its child.
        (11, 10, 10, 10),
        (13, 1, 13, 12),  # The reparented child of a command that exited.
    )
    assert _exec_processes(dump, 2) == [2, 3, 4, 5, 6, 7, 8]
    assert _exec_processes(dump, 10) == [10, 11]
    assert _exec_processes(dump, 12) == [13]
    assert _exec_processes("PROCFS DUMP\n", 2) == []
    assert _exec_processes("loading sandbox: not found", 2) == []


def test_exec_timeout_kills_the_command_and_its_children():
    """A timed-out exec returns at its timeout and leaves nothing running.
    Killing only `runsc exec` used to leave the command running, and the
    exec returned only when it exited."""
    runtime = SandboxRuntime()
    instance_id = runtime.create(image="busybox:latest", shell="/bin/sh")
    try:
        start = time.monotonic()
        with pytest.raises(SandboxTimeoutError):
            runtime.exec(instance_id, "sleep 30; echo done", timeout=1)
        assert time.monotonic() - start < 10
        assert "sleep 30" not in runtime.exec(instance_id, "ps").stdout
        # The sandbox is still usable.
        assert runtime.exec(instance_id, "echo ok").stdout == "ok\n"
    finally:
        runtime.delete(instance_id)


def test_exec_timeout_kills_background_jobs_that_outlive_the_shell():
    """A background job holding the output pipes outlives the shell that
    started it, so the exec times out. The job is reparented to PID 1, and
    GNU timeout also moves to a process group of its own, but both stay in
    the command's session, and the timeout kills them."""
    runtime = SandboxRuntime()
    instance_id = runtime.create(image="python:3.10-slim")
    try:
        for command in ["sleep 1000 & exit", "timeout 1001 sleep 1001 & exit"]:
            with pytest.raises(SandboxTimeoutError):
                runtime.exec(instance_id, command, timeout=1)
        # The image has no ps. A killed process left as a zombie has an
        # empty cmdline.
        cmdlines = runtime.exec(instance_id, r"cat /proc/[0-9]*/cmdline | tr '\0' ' '")
        assert "sleep 100" not in cmdlines.stdout
    finally:
        runtime.delete(instance_id)


def test_exec_timeout_kills_a_setsid_child_of_the_command():
    """A child that left the command's session with setsid() is found only
    through its parent, so the processes are listed before any is killed."""
    runtime = SandboxRuntime()
    instance_id = runtime.create(image="python:3.10-slim")
    try:
        with pytest.raises(SandboxTimeoutError):
            runtime.exec(instance_id, "setsid sleep 1002 & sleep 1000", timeout=1)
        cmdlines = runtime.exec(instance_id, r"cat /proc/[0-9]*/cmdline | tr '\0' ' '")
        assert "sleep 100" not in cmdlines.stdout
    finally:
        runtime.delete(instance_id)


def test_exec_timeout_kills_processes_started_during_the_kill():
    """A child that keeps starting processes starts more between the listing
    of the command's processes and its own kill. Those are killed too."""
    runtime = SandboxRuntime()
    instance_id = runtime.create(image="busybox:latest", shell="/bin/sh")
    try:
        with pytest.raises(SandboxTimeoutError):
            runtime.exec(
                instance_id,
                "sh -c 'while :; do sleep 1000 & done' & wait",
                timeout=0.5,
            )
        # A killed process left as a zombie shows up as [sleep].
        assert "sleep 1000" not in runtime.exec(instance_id, "ps").stdout
    finally:
        runtime.delete(instance_id)


def test_exec_replaces_output_that_is_not_utf8():
    runtime = SandboxRuntime()
    instance_id = runtime.create(image="busybox:latest", shell="/bin/sh")
    try:
        result = runtime.exec(
            instance_id, r"printf 'ok \377\376\r\n'; printf '\377' >&2"
        )
        assert result.exit_code == 0
        # Like the bytes, line endings come back as printed.
        assert result.stdout == "ok ��\r\n"
        assert result.stderr == "�"
    finally:
        runtime.delete(instance_id)


def test_readonly_rootfs_with_workdir_runs_on_private_overlay(monkeypatch):
    """On a worker that can't mount an overlayfs rootfs, readonly + explicit
    workdir runs on a private writable overlay. The workdir works, and rootfs
    writes land in the overlay and are discarded with the sandbox rather
    than rejected."""
    monkeypatch.setattr(overlayfs, "can_mount_rootfs_overlay", lambda: False)
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="busybox:latest", shell="/bin/sh", workdir="/workspace"
        )
    )
    try:
        res = backend.exec_command(
            sb,
            "echo ws_ok > /workspace/ws.txt && cat /workspace/ws.txt && "
            "echo root_ok > /probe.txt && cat /probe.txt",
            timeout=30,
        )
        assert res.exit_code == 0, res.stderr
        assert res.stdout.split() == ["ws_ok", "root_ok"]
    finally:
        backend.delete_sandbox(sb)
    # The write went to the sandbox's overlay, not the shared image cache.
    assert not os.path.exists(os.path.join(_BUSYBOX_CACHE, "probe.txt"))


def test_workdir_writability_matrix():
    """readonly=True + workdir=None -> nothing writable; explicit workdir is
    the only writable path; readonly=False -> everything writable."""
    runtime = SandboxRuntime()

    # Default (readonly=True, workdir=None): the rootfs is not writable.
    # (Standard tmpfs mounts like /tmp are, as in any container runtime.)
    instance_id = runtime.create(image="busybox:latest", shell="/bin/sh")
    try:
        assert runtime.exec(instance_id, "touch /probe").exit_code != 0
        assert runtime.exec(instance_id, "touch /etc/probe").exit_code != 0
    finally:
        runtime.delete(instance_id)

    # readonly=True, explicit workdir: the workdir is writable and is the
    # cwd. Whether the rest of the root stays read-only depends on whether
    # this worker can mount a kernel overlay.
    instance_id = runtime.create(
        image="busybox:latest", workdir="/data", shell="/bin/sh"
    )
    try:
        assert runtime.exec(instance_id, "touch /data/probe").exit_code == 0
        assert runtime.exec(instance_id, "pwd").stdout.strip() == "/data"
    finally:
        runtime.delete(instance_id)

    # readonly=False: everything is writable, with or without a workdir.
    instance_id = runtime.create(
        image="busybox:latest", readonly=False, shell="/bin/sh"
    )
    try:
        assert runtime.exec(instance_id, "touch /etc/probe").exit_code == 0
    finally:
        runtime.delete(instance_id)


def test_image_workdir_sets_cwd_without_becoming_writable():
    """The image's own WORKDIR is inherited as the process cwd only — its
    content stays visible and it is never silently made writable."""
    runtime = SandboxRuntime()
    # golang:alpine sets WORKDIR /go and ships /go/bin and /go/src.
    instance_id = runtime.create(image="golang:1.22-alpine", shell="/bin/sh")
    try:
        assert runtime.exec(instance_id, "pwd").stdout.strip() == "/go"
        listing = runtime.exec(instance_id, "ls /go").stdout
        assert "bin" in listing and "src" in listing
        # Inherited WORKDIR is not a scratch mount: still readonly.
        assert runtime.exec(instance_id, "touch /go/probe").exit_code != 0
    finally:
        runtime.delete(instance_id)

    # With a writable rootfs the same path is writable and unshadowed.
    instance_id = runtime.create(
        image="golang:1.22-alpine", readonly=False, shell="/bin/sh"
    )
    try:
        assert "bin" in runtime.exec(instance_id, "ls /go").stdout
        assert runtime.exec(instance_id, "touch /go/probe").exit_code == 0
    finally:
        runtime.delete(instance_id)


def _public_config() -> GVisorSandboxConfig:
    return GVisorSandboxConfig(
        image="busybox:latest", shell="/bin/sh", network="public"
    )


def _run_argv(
    network: str, rootless: bool = True, cidr_allowlist=None, **backend_kwargs
) -> list:
    """`_build_run_command` over fixed paths — pure argv, no side effects."""
    backend = GVisorSandboxBackend(**backend_kwargs)
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        network=network,
        rootless=rootless,
        cidr_allowlist=cidr_allowlist,
    )
    return backend._build_run_command(cfg, "/tmp/rd", "sb-1")


def test_build_run_command_has_no_overlay_flag():
    """The rootfs and its overlay come from the bundle's gVisor annotations,
    never from a runsc --overlay2 flag."""
    cmd = _run_argv("none")
    assert not any(a.startswith("--overlay2") for a in cmd)
    assert cmd[-4:] == ["run", "--bundle", "/tmp/rd", "sb-1"]


def _owned_busybox_tar(tar_path: str) -> None:
    """A busybox-based local image tar with baked non-root ownership, modeled
    on the mailman image (0700 uid=101 spool, 02710 setgid dir, a setuid tool)."""
    import io
    import tarfile
    import tempfile

    from ray.experimental.sandbox._internal.image_utils import _extract_image_layers

    # Flatten busybox into a plain directory to re-pack it.
    work = tempfile.mkdtemp(prefix="owned-busybox-")
    busybox_rootfs = os.path.join(work, "rootfs")
    os.makedirs(busybox_rootfs)
    _extract_image_layers("busybox:latest", busybox_rootfs, work, 120.0, work)

    def _as_root(ti):
        ti.uid = ti.gid = 0
        ti.uname = ti.gname = ""
        return ti

    with tarfile.open(tar_path, "w") as tar:
        tar.add(busybox_rootfs, arcname=".", filter=_as_root)
        for name, typ, uid, gid, mode, data in (
            ("./var/spool/testq", tarfile.DIRTYPE, 101, 0, 0o700, None),
            (
                "./var/spool/testq/inner.txt",
                tarfile.REGTYPE,
                101,
                0,
                0o600,
                b"queued\n",
            ),
            ("./var/spool/public", tarfile.DIRTYPE, 101, 104, 0o2710, None),
            (
                "./usr/local/bin/suidtool",
                tarfile.REGTYPE,
                0,
                0,
                0o4755,
                b"#!/bin/sh\nid -u\n",
            ),
        ):
            ti = tarfile.TarInfo(name)
            ti.type = typ
            ti.uid, ti.gid, ti.mode = uid, gid, mode
            if data is not None:
                ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))
            else:
                tar.addfile(ti)


def test_erofs_baked_ownership_and_chown(tmp_path):
    """The image's owners survive without any host id mapping, root traverses
    another user's 0700 directory, a named user reads only its own files,
    setuid works, and chown to arbitrary uids succeeds."""
    from ray.experimental.sandbox.config import DOCKER_DEFAULT_CAPABILITIES

    tar_path = str(tmp_path / "owned-busybox.tar")
    _owned_busybox_tar(tar_path)
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image=tar_path,
            shell="/bin/sh",
            network="none",
            readonly=False,
            capabilities=list(DOCKER_DEFAULT_CAPABILITIES),
            env={"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
        )
    )
    try:
        assert os.path.isfile(backend._image_manager.get_rootfs_image(tar_path))
        res = backend.exec_command(
            sb,
            "stat -c %u:%g:%a /var/spool/testq /var/spool/public && "
            "cat /var/spool/testq/inner.txt && "
            "touch /probe && chown 38:38 /probe && stat -c %u:%g /probe && "
            "adduser -D -u 1234 alice && mkdir -p /srv/lists && touch /srv/lists/cfg && "
            "chown -R alice:alice /srv/lists && stat -c %u:%g /srv/lists/cfg && "
            "mkdir /srv/shared && chown 0:104 /srv/shared && chmod 2770 /srv/shared && "
            "touch /srv/shared/post && stat -c %g /srv/shared/post",
            timeout=60,
        )
        assert res.exit_code == 0, res.stderr
        assert res.stdout.split() == [
            "101:0:700",
            "101:104:2710",
            "queued",
            "38:38",
            "1234:1234",
            "104",
        ]
        # Ownership is enforced for other users, and setuid still elevates.
        denied = (
            backend.exec_command(
                sb, "cat /var/spool/testq/inner.txt", user="1000", timeout=30
            )
            if "user" in backend.exec_command.__code__.co_varnames
            else None
        )
        if denied is not None:
            assert denied.exit_code != 0
    finally:
        backend.delete_sandbox(sb)


def test_erofs_readonly_rootfs():
    """readonly=True keeps / read-only while /tmp stays writable, and the
    sandbox still boots (its mount points were seeded into the image)."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh", network="none")
    )
    try:
        res = backend.exec_command(
            sb,
            "touch /x 2>/dev/null; test ! -e /x && echo ro && echo hi > /tmp/x && cat /tmp/x",
            timeout=30,
        )
        assert res.exit_code == 0, res.stderr
        assert res.stdout.split() == ["ro", "hi"]
    finally:
        backend.delete_sandbox(sb)


def _host_ip() -> str:
    """The worker's primary IPv4."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]


def _slirp4netns_pids() -> set:
    """PIDs of running slirp4netns processes."""
    pids = set()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv0 = (entry / "cmdline").read_bytes().split(b"\0", 1)[0]
        except OSError:
            continue
        if os.path.basename(argv0) == b"slirp4netns":
            pids.add(entry.name)
    return pids


def test_build_run_command_public_wraps_with_slirp4netns():
    """The slirp4netns flags are the isolation property and the chain's shape
    is the topology, so pin both: holder namespaces first, slirp4netns attached
    from the pod side in the foreground, runsc entered as mapped root (never
    --rootless)."""
    cmd = _run_argv("public")
    assert cmd[:2] == ["bash", "-c"]
    script = cmd[2]

    # The shell, and so everything it starts, ignores a Ctrl-C sent to its
    # owner's process group. It stays the parent of every helper and reaps
    # them as it exits.
    assert script.startswith(
        "trap '' INT QUIT; "
        "trap 'kill -KILL ${NSPID:-$HOLDER} $SLIRP 2>/dev/null; wait' EXIT; "
        "setpriv --pdeathsig KILL -- "
        "unshare --user --map-root-user --net --fork --kill-child "
    )
    assert script.endswith("run --bundle /tmp/rd sb-1 & wait $!")
    for fragment in (
        "/tmp/rd/netns.pid",
        # slirp4netns stays in the foreground under the launch shell, and
        # its ready file, written once the tap is configured, gates the
        # runsc start.
        "setpriv --pdeathsig KILL -- "
        "slirp4netns --configure --cidr=198.18.0.0/24 --mtu=65520 --disable-host-loopback "
        "--disable-dns --enable-seccomp --ready-fd=3 "
        "--netns-type=path /proc/$NSPID/ns/net tap0 "
        "--userns-path /proc/$NSPID/ns/user 3>/tmp/rd/slirp4netns.ready &",
        "kill -0 $SLIRP",
        "[ -s /tmp/rd/slirp4netns.ready ] ||",
        # The holder wait fast-fails if the holder dies and refuses an
        # empty NSPID (which would resolve to /proc//ns/net).
        "kill -0 $HOLDER",
        '[ -n "$NSPID" ]',
        "nsenter --preserve-credentials -U -n -t $NSPID -- "
        "setpriv --pdeathsig KILL -- runsc",
        "--network host",
    ):
        assert fragment in script, fragment
    assert "--rootless" not in script


def test_build_run_command_public_gives_the_gofer_its_own_netns():
    """runsc runs in the network="public" script's user namespace, whose gofer
    can't join the network namespace runsc shares between gofers under --root
    when a sandbox outside it created that one."""
    assert "--gofer-network-namespace=new" in _run_argv("public")[2]
    assert "--gofer-network-namespace=new" not in _run_argv("none")


def test_build_run_command_public_keeps_rootless_cgroup_tolerance(monkeypatch):
    """Dropping --rootless must not make runsc start configuring cgroups: the
    wrapper forces --ignore-cgroups for rootless configs, and only for them."""
    monkeypatch.delenv("RAY_SANDBOX_IGNORE_CGROUPS", raising=False)

    script = _run_argv("public")[2]
    assert "--ignore-cgroups" in script
    assert "--rootless" not in script

    privileged = _run_argv("public", rootless=False)[2]
    assert "--ignore-cgroups" not in privileged

    assert "--ignore-cgroups" not in _run_argv("none")


@pytest.mark.parametrize(
    "network,rootless", [("none", True), ("host", True), ("sandbox", False)]
)
def test_build_run_command_other_modes_exec_runsc(network, rootless):
    """Every mode but "public" runs runsc itself: a shell makes it ignore a
    Ctrl-C sent to its owner's process group, then execs it."""
    cmd = _run_argv(network, rootless=rootless)
    assert cmd[:5] == ["sh", "-c", "trap '' INT QUIT; exec \"$@\"", "sh", "runsc"]
    assert "slirp4netns" not in cmd
    assert cmd[cmd.index("--network") + 1] == network
    assert cmd[-4:] == ["run", "--bundle", "/tmp/rd", "sb-1"]


_ID_MAPS = overlayfs.IdMaps(
    uid_map=((0, 1000, 1), (1, 100000, 65536)),
    gid_map=((0, 1000, 1), (1, 100000, 65536)),
)


def _rootfs_overlay_for(in_userns: bool):
    return overlayfs.RootfsOverlay(
        image="/cache/rootfs.erofs",
        mountpoint="/tmp/rd/rootfs",
        tmpfs_dir="/tmp/rd/overlayfs-tmpfs",
        userns=UserNamespaceType.PRIVATE if in_userns else UserNamespaceType.HOST,
        image_mount_mode=ImageMountMode.FUSE if in_userns else ImageMountMode.KERNEL,
        id_maps=_ID_MAPS if in_userns else None,
    )


# wrap's own argv is pinned in test_overlayfs, so these tests only check the
# command _build_run_command wraps, behind this marker.
_WRAPPED = "<wrapped by the rootfs overlay>"


def _launched(cmd: list) -> list:
    """The command the launch shell that ignores SIGINT and SIGQUIT execs."""
    assert cmd[:4] == ["sh", "-c", "trap '' INT QUIT; exec \"$@\"", "sh"]
    return cmd[4:]


def _mark_overlay_wrap(monkeypatch):
    monkeypatch.setattr(
        overlayfs.RootfsOverlay, "wrap", lambda self, command: [_WRAPPED, *command]
    )


def _boot_from_overlays(monkeypatch, overlay=True):
    """Have every sandbox created from here on need a kernel overlay, or
    none, whatever its spec."""
    monkeypatch.setattr(overlayfs, "needs_rootfs_overlay", lambda spec: overlay)


def _overlayfs_run_argv(
    network: str,
    in_userns: bool,
    rootless: bool = True,
    writable: bool = True,
    disk: str = "0",
) -> list:
    """`_build_run_command` for an overlayfs sandbox over fixed paths."""
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        network=network,
        rootless=rootless,
        readonly=not writable,
        disk=disk,
    )
    return GVisorSandboxBackend()._build_run_command(
        cfg, "/tmp/rd", "sb-1", rootfs_overlay=_rootfs_overlay_for(in_userns)
    )


@pytest.mark.parametrize("in_userns", [True, False])
@pytest.mark.parametrize(
    "network,rootless",
    [("none", True), ("none", False), ("host", True), ("sandbox", False)],
)
def test_build_run_command_overlayfs_sandbox_wraps_runsc_in_mount_namespace(
    monkeypatch, in_userns, network, rootless
):
    """An overlayfs sandbox's runsc starts in a private mount namespace that
    first mounts the sandbox's kernel overlay, under the launch shell that
    ignores interrupts. gVisor keeps writes made inside the sandbox in its
    --overlay2 layer."""
    _mark_overlay_wrap(monkeypatch)
    cmd = _launched(_overlayfs_run_argv(network, in_userns, rootless=rootless))

    assert cmd[0] == _WRAPPED
    runsc = cmd[1:]
    assert runsc[0] == "runsc"
    assert runsc[runsc.index("--overlay2") + 1] == "root:dir=/tmp/rd/runsc-overlay2"
    assert runsc[runsc.index("--network") + 1] == network
    assert runsc[-4:] == ["run", "--bundle", "/tmp/rd", "sb-1"]


@pytest.mark.parametrize("in_userns", [True, False])
@pytest.mark.parametrize("rootless", [True, False])
@pytest.mark.parametrize("network", ["none", "public"])
def test_build_run_command_overlayfs_userns_drops_rootless(
    monkeypatch, in_userns, rootless, network
):
    """Inside a user namespace, runsc runs as mapped root without --rootless
    and keeps rootless mode's cgroup tolerance. Without one, its flags stay
    as they are, for network="public" too."""
    # --ignore-cgroups below must come from rootless mode alone.
    monkeypatch.delenv("RAY_SANDBOX_IGNORE_CGROUPS", raising=False)
    cmd = _overlayfs_run_argv(network, in_userns, rootless=rootless)

    # With network="public", runsc's argv is inside a bash script.
    words = " ".join(cmd).split()
    runsc = words[words.index("runsc") :]
    if in_userns:
        assert "--rootless" not in runsc
        assert ("--ignore-cgroups" in runsc) is rootless
    else:
        assert ("--rootless" in runsc) is rootless
        assert "--ignore-cgroups" not in runsc


@pytest.mark.parametrize("in_userns", [True, False])
@pytest.mark.parametrize("network", ["none", "public"])
def test_build_run_command_overlayfs_userns_gets_its_own_gofer_netns(
    in_userns, network
):
    """In a private user namespace, runsc's gofer gets a network namespace of
    its own, since it can't join the one runsc shares between gofers under
    --root when a sandbox outside that user namespace created it."""
    cmd = _overlayfs_run_argv(network, in_userns)

    words = " ".join(cmd).split()
    runsc = words[words.index("runsc") :]
    assert ("--gofer-network-namespace=new" in runsc) is in_userns


def test_build_run_command_disk_caps_an_overlayfs_sandboxs_overlay2():
    """disk caps a writable overlayfs sandbox's --overlay2 layer, as the rootfs
    annotations cap an EROFS sandbox's overlay."""
    cmd = _overlayfs_run_argv("none", in_userns=True, disk="64Mi")
    assert "--overlay2 root:dir=/tmp/rd/runsc-overlay2,size=67108864" in " ".join(cmd)


@pytest.mark.parametrize("overlayfs_sandbox", [True, False])
def test_build_run_command_disk_caps_a_readonly_sandboxs_workdir(overlayfs_sandbox):
    """A readonly sandbox's cap applies to its workdir, so gVisor's overlay
    covers that bind mount too, whether the sandbox boots from a kernel
    overlay or, where none can mount, from its EROFS image."""
    cfg = GVisorSandboxConfig(image="busybox:latest", workdir="/work", disk="64Mi")
    rootfs_overlay = _rootfs_overlay_for(True) if overlayfs_sandbox else None
    cmd = GVisorSandboxBackend()._build_run_command(
        cfg, "/tmp/rd", "sb-1", rootfs_overlay=rootfs_overlay
    )
    assert "--overlay2 all:dir=/tmp/rd/runsc-overlay2,size=67108864" in " ".join(cmd)


def test_build_run_command_readonly_overlayfs_sandbox_has_overlay2():
    """A readonly overlayfs sandbox gets --overlay2 too, which runsc ignores
    for a read-only root."""
    cmd = _overlayfs_run_argv("none", in_userns=True, writable=False)
    assert "--overlay2 root:dir=/tmp/rd/runsc-overlay2" in " ".join(cmd)


@pytest.mark.parametrize("in_userns", [True, False])
def test_build_run_command_public_overlayfs_sandbox_runs_in_its_overlay(
    monkeypatch, in_userns
):
    """With network="public", an overlayfs sandbox's whole namespace setup
    runs inside its overlay's namespaces. If those include a user namespace,
    the holder only adds a network namespace. Otherwise the holder adds a
    user namespace to own it, which slirp4netns and nft enter to get
    CAP_NET_ADMIN there, while runsc joins only the network namespace and
    keeps the overlay's owners."""
    _mark_overlay_wrap(monkeypatch)
    cmd = _launched(_overlayfs_run_argv("public", in_userns))

    assert cmd[0] == _WRAPPED
    inner = cmd[1:]
    assert inner[:2] == ["bash", "-c"]
    script = inner[2]
    holder_userns = "" if in_userns else "--user --map-root-user "
    assert (
        f"setpriv --pdeathsig KILL -- unshare {holder_userns}--net --fork "
        "--kill-child bash -c 'echo $$ > /tmp/rd/netns.pid; exec sleep infinity' & "
    ) in script
    nft_userns = "" if in_userns else "-U "
    assert (
        f"nsenter --preserve-credentials {nft_userns}-n -t $NSPID -- "
        "nft -f /tmp/rd/egress.nft"
    ) in script
    assert (
        "nsenter --preserve-credentials -n -t $NSPID -- "
        "setpriv --pdeathsig KILL -- runsc"
    ) in script
    assert ("--userns-path /proc/$NSPID/ns/user" in script) is not in_userns
    assert "--overlay2 root:dir=/tmp/rd/runsc-overlay2" in script


class _StopBeforeRun(Exception):
    pass


class _BundleRecordingImageManager:
    """Records what create_sandbox asks of it, and writes a bundle whose
    root.path is relative, as an _oci_spec_transform_fn may leave it."""

    def __init__(self, image):
        self.image = image
        self.bundle_kwargs = None
        self.rootfs_image_calls = []
        self.released = []

    def pull_image(self, image, **kwargs):
        return None

    def release_image(self, image, instance_id):
        self.released.append(image)

    def get_workdir(self, image):
        return None

    def get_rootfs_image(self, image):
        self.rootfs_image_calls.append(image)
        return self.image

    def prepare_oci_bundle(self, root_dir, **kwargs):
        self.bundle_kwargs = kwargs
        spec = {"root": {"path": "rootfs"}}
        transform = kwargs.get("_oci_spec_transform_fn")
        if transform is not None:
            spec = transform(spec) or spec
        path = os.path.join(root_dir, "config.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(spec, f)
        return path


def _stub_overlay_mount(monkeypatch, userns, image_mount_mode):
    """Have overlayfs sandboxes mount their rootfs this way, without probing."""
    monkeypatch.setattr(overlayfs.UserNamespaceType, "detect", lambda: userns)
    monkeypatch.setattr(
        overlayfs.ImageMountMode, "detect", lambda userns: image_mount_mode
    )
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: _ID_MAPS)


def _create_until_run(
    tmp_path,
    monkeypatch,
    overlay,
    userns=UserNamespaceType.PRIVATE,
    image_mount_mode=ImageMountMode.FUSE,
    readonly=False,
):
    """Run create_sandbox with a recording image manager up to the point it
    would start runsc, and return what _build_run_command got and the
    manager."""
    monkeypatch.setattr(gvisor, "_RAY_SANDBOX_DIR", str(tmp_path / "sandboxes"))
    monkeypatch.setattr("shutil.which", lambda name: f"/usr/bin/{name}")
    _stub_overlay_mount(monkeypatch, userns, image_mount_mode)
    _boot_from_overlays(monkeypatch, overlay)
    captured = {}

    def _capture(config, root_dir, sandbox_id, restore=None, rootfs_overlay=None):
        # The failed create deletes its bundle, so check its directories now.
        captured.update(
            root_dir=root_dir,
            rootfs_overlay=rootfs_overlay,
            has_tmpfs_dir=rootfs_overlay is not None
            and os.path.isdir(rootfs_overlay.tmpfs_dir),
            has_overlay2_dir=os.path.isdir(os.path.join(root_dir, "runsc-overlay2")),
        )
        raise _StopBeforeRun()

    manager = _BundleRecordingImageManager(image=str(tmp_path / "rootfs.erofs"))
    backend = GVisorSandboxBackend(image_manager=manager)
    monkeypatch.setattr(backend, "_build_run_command", _capture)
    with pytest.raises(SandboxCreationError) as err:
        backend.create_sandbox(
            GVisorSandboxConfig(image="busybox:latest", readonly=readonly)
        )
    assert isinstance(err.value.__cause__, _StopBeforeRun)
    return captured, manager


@pytest.mark.parametrize(
    "userns, image_mount_mode",
    [
        (UserNamespaceType.HOST, ImageMountMode.KERNEL),
        (UserNamespaceType.HOST, ImageMountMode.FUSE),
        (UserNamespaceType.PRIVATE, ImageMountMode.FUSE),
    ],
)
def test_create_sandbox_prepares_overlayfs_sandbox(
    tmp_path, monkeypatch, userns, image_mount_mode
):
    """A sandbox that needs a kernel overlay boots from one over its cached
    EROFS image, which create_sandbox mounts at root.path resolved against
    the bundle."""
    captured, manager = _create_until_run(
        tmp_path, monkeypatch, True, userns, image_mount_mode
    )

    overlay = captured["rootfs_overlay"]
    assert manager.rootfs_image_calls == ["busybox:latest"]
    assert overlay.image == str(tmp_path / "rootfs.erofs")
    # The fake bundle's root.path is relative, so this checks it resolves
    # against the bundle.
    assert overlay.mountpoint == os.path.join(captured["root_dir"], "rootfs")
    assert overlay.userns is userns
    assert overlay.image_mount_mode is image_mount_mode
    assert captured["has_tmpfs_dir"]
    assert captured["has_overlay2_dir"]


def test_create_sandbox_readonly_overlayfs_sandbox_has_overlay2_dir(
    tmp_path, monkeypatch
):
    """A readonly overlayfs sandbox gets a directory for gVisor's --overlay2
    layer too, which stays empty."""
    captured, _ = _create_until_run(tmp_path, monkeypatch, True, readonly=True)
    assert captured["has_overlay2_dir"]


def test_create_sandbox_erofs_sandbox_skips_overlay(tmp_path, monkeypatch):
    """A sandbox that doesn't need a kernel overlay boots straight from its
    EROFS image."""
    captured, manager = _create_until_run(tmp_path, monkeypatch, False)

    assert captured["rootfs_overlay"] is None
    assert manager.rootfs_image_calls == []


def _create_overlayfs_sandbox(tmp_path, monkeypatch, manager):
    """Run create_sandbox for an overlayfs sandbox with ``manager``, which is
    expected to fail before runsc starts, release the image, and remove the
    sandbox's bundle directory."""
    sandboxes_dir = tmp_path / "sandboxes"
    monkeypatch.setattr(gvisor, "_RAY_SANDBOX_DIR", str(sandboxes_dir))
    _boot_from_overlays(monkeypatch)
    backend = GVisorSandboxBackend(image_manager=manager)
    try:
        backend.create_sandbox(GVisorSandboxConfig(image="busybox:latest"))
    finally:
        assert manager.released == ["busybox:latest"]
        assert not any(sandboxes_dir.glob("ray-sandbox-*"))


def test_create_sandbox_overlayfs_fails_where_overlays_cant_mount(
    tmp_path, monkeypatch
):
    """Where the overlay can't mount, an overlayfs sandbox fails with the
    reason the test mount gave."""
    fuse_error = "bash: line 1: erofsfuse: command not found"
    monkeypatch.setattr(overlayfs, "_has_mount_privileges", lambda: False)
    monkeypatch.setattr(overlayfs.IdMaps, "detect", lambda: _ID_MAPS)
    monkeypatch.setattr(
        overlayfs, "_probe_mount", lambda userns, image: (False, fuse_error)
    )
    manager = _BundleRecordingImageManager(image=str(tmp_path / "rootfs.erofs"))
    with pytest.raises(SandboxCreationError, match=fuse_error):
        _create_overlayfs_sandbox(tmp_path, monkeypatch, manager)


def test_overlayfs_mount_keeps_the_image_owners(tmp_path, overlay_mount):
    """Through the overlay, a file a user other than root owns in the image
    keeps its owner, whichever way the image is mounted, unless a worker
    running as a user other than root has no subordinate ids to map it to in a
    private user namespace."""
    tree = tmp_path / "tree"
    (tree / "home" / "app").mkdir(parents=True)
    (tree / "home" / "app" / "g").write_text("app")
    image = str(tmp_path / "rootfs.erofs")
    owners = {"home/app": (1000, 1000), "home/app/g": (1000, 1000)}
    image_utils.build_erofs_image(str(tree), owners, image)

    bundle = tmp_path / "bundle"
    overlay = overlayfs.prepare(
        image=image,
        mountpoint=str(bundle / "rootfs"),
        bundle_dir=str(bundle),
    )
    stat = ["stat", "-c", "%u:%g", os.path.join(overlay.mountpoint, "home/app/g")]
    res = subprocess.run(overlay.wrap(stat), capture_output=True, text=True, timeout=60)
    assert res.returncode == 0, res.stderr
    maps_only_root = (
        overlay.userns is UserNamespaceType.PRIVATE
        and overlay.id_maps is None
        and os.getuid() != 0
    )
    if maps_only_root:
        assert res.stdout.strip() == "65534:65534"
    else:
        assert res.stdout.strip() == "1000:1000"


def _pids_running(argv: list) -> list:
    """The pids of the processes whose command line is ``argv``."""
    target = "\0".join(argv) + "\0"
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline") as f:
                if f.read() == target:
                    pids.append(int(entry))
        except OSError:
            pass
    return pids


def test_killing_the_overlay_wrapper_stops_its_command(tmp_path, overlay_mount):
    """Killing the process that wraps a command in its kernel overlay, as a
    failed create or a wedged teardown does, stops the command too, rather
    than leaving it running with its mounts."""
    tree = tmp_path / "tree"
    tree.mkdir()
    image = str(tmp_path / "rootfs.erofs")
    image_utils.build_erofs_image(str(tree), {}, image)
    bundle = tmp_path / "bundle"
    overlay = overlayfs.prepare(
        image=image,
        mountpoint=str(bundle / "rootfs"),
        bundle_dir=str(bundle),
    )
    command = ["sleep", f"{1000 + os.getpid() % 1000}"]
    wrapper = subprocess.Popen(overlay.wrap(command))
    try:
        deadline = time.monotonic() + 30
        while not _pids_running(command):
            assert wrapper.poll() is None, "the wrapper exited early"
            assert time.monotonic() < deadline, "the command never started"
            time.sleep(0.1)
    finally:
        wrapper.kill()
        wrapper.wait()
    deadline = time.monotonic() + 10
    while _pids_running(command):
        assert time.monotonic() < deadline, "the command outlived its wrapper"
        time.sleep(0.1)


def _file_digest(path: str) -> str:
    """The SHA-256 of the file at ``path``."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_overlayfs_sandbox_leaves_cached_image_untouched(monkeypatch, overlay_mount):
    """Overlayfs sandboxes boot from an overlay over the cached EROFS image.
    Their writes never reach the image or another sandbox. The overlay's root
    matches the image's."""
    backend = GVisorSandboxBackend()
    manager = backend._image_manager
    manager.pull_image("busybox:latest", instance_id="image-snapshot")
    try:
        image = manager.get_rootfs_image("busybox:latest")
        before = _file_digest(image)
        erofs = backend.create_sandbox(
            GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh")
        )
        _boot_from_overlays(monkeypatch)
        writable = backend.create_sandbox(
            GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh", readonly=False)
        )
        readonly = backend.create_sandbox(
            GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh")
        )
        try:
            assert backend.exec_command(writable, "touch /etc/probe").exit_code == 0
            assert backend.exec_command(readonly, "touch /etc/probe").exit_code != 0
            assert backend.exec_command(readonly, "test -e /etc/probe").exit_code != 0
            stat_root_mode = "stat -c %a /"
            assert (
                backend.exec_command(writable, stat_root_mode).stdout
                == backend.exec_command(erofs, stat_root_mode).stdout
            )
        finally:
            for sb in (erofs, readonly, writable):
                backend.delete_sandbox(sb)
        assert _file_digest(image) == before
    finally:
        manager.release_image("busybox:latest", "image-snapshot")


def test_overlayfs_sandbox_with_host_network_can_write_rootfs(
    monkeypatch, overlay_mount
):
    """A network="host" overlayfs sandbox boots and can write its rootfs."""
    _boot_from_overlays(monkeypatch)
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="busybox:latest",
            shell="/bin/sh",
            network="host",
            readonly=False,
        )
    )
    try:
        res = backend.exec_command(sb, "touch /probe && echo ok")
        assert res.exit_code == 0, res.stderr
    finally:
        backend.delete_sandbox(sb)


def test_overlayfs_sandbox_disk_caps_its_writes(monkeypatch, overlay_mount):
    """disk caps the writes made inside a writable overlayfs sandbox. Writes
    past it fail with ENOSPC, and the sandbox keeps working."""
    _boot_from_overlays(monkeypatch)
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="busybox:latest", shell="/bin/sh", readonly=False, disk="64Mi"
        )
    )
    try:
        res = backend.exec_command(sb, "dd if=/dev/zero of=/fill bs=1M count=128")
        assert res.exit_code != 0
        assert "No space left on device" in res.stderr
        # Deleting frees the space, and the sandbox keeps working.
        res = backend.exec_command(
            sb, "rm /fill && dd if=/dev/zero of=/again bs=1M count=32 && echo ok"
        )
        assert res.stdout.endswith("ok\n"), res.stderr
    finally:
        backend.delete_sandbox(sb)


def test_overlayfs_sandbox_with_public_network(
    monkeypatch, ensure_slirp4netns, overlay_mount
):
    """A network="public" overlayfs sandbox boots with a writable rootfs and
    its tap device."""
    _boot_from_overlays(monkeypatch)
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="busybox:latest",
            shell="/bin/sh",
            network="public",
            readonly=False,
        )
    )
    try:
        res = backend.exec_command(sb, "touch /probe && ip addr show tap0")
        assert res.exit_code == 0, res.stderr
    finally:
        backend.delete_sandbox(sb)


def _with_prestart_hook(spec):
    """Add an OCI hook that runs on the host before the sandbox starts, and
    writes /hook-marker into its rootfs, as hooks that set up a sandbox do.
    runsc passes the hook the sandbox's state, whose bundle holds the rootfs."""
    script = (
        'bundle=$(sed -n \'s/.*"bundle":"\\([^"]*\\)".*/\\1/p\'); '
        'echo hook > "$bundle/rootfs/hook-marker"'
    )
    hook = {"path": "/bin/sh", "args": ["sh", "-c", script]}
    spec.setdefault("hooks", {})["createRuntime"] = [hook]
    return spec


def test_overlayfs_sandbox_runs_prestart_hooks_in_its_overlay(overlay_mount):
    """Sandboxes with hooks that run before they start boot from an overlay
    over the cached EROFS image, which the hooks write into. The hooks' writes
    never reach the image or a sandbox without them."""
    backend = GVisorSandboxBackend()
    manager = backend._image_manager
    manager.pull_image("busybox:latest", instance_id="image-snapshot")
    try:
        image = manager.get_rootfs_image("busybox:latest")
        before = _file_digest(image)
        writable = backend.create_sandbox(
            GVisorSandboxConfig(
                image="busybox:latest",
                shell="/bin/sh",
                readonly=False,
                _oci_spec_transform_fn=_with_prestart_hook,
            )
        )
        readonly = backend.create_sandbox(
            GVisorSandboxConfig(
                image="busybox:latest",
                shell="/bin/sh",
                _oci_spec_transform_fn=_with_prestart_hook,
            )
        )
        erofs = backend.create_sandbox(
            GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh")
        )
        try:
            for sb in (writable, readonly):
                res = backend.exec_command(sb, "cat /hook-marker")
                assert res.stdout.strip() == "hook", res.stderr
            assert backend.exec_command(erofs, "test -e /hook-marker").exit_code != 0
            assert backend.exec_command(readonly, "touch /etc/probe").exit_code != 0
        finally:
            for sb in (erofs, readonly, writable):
                backend.delete_sandbox(sb)
        assert _file_digest(image) == before
    finally:
        manager.release_image("busybox:latest", "image-snapshot")


@pytest.mark.parametrize("binary", ["slirp4netns", "nsenter", "setpriv"])
def test_create_sandbox_requires_netns_binaries(monkeypatch, binary):
    """A missing helper fails fast, before the image pull, with remediation."""

    class _NoPullImageManager:
        def pull_image(self, *args, **kwargs):
            raise AssertionError(f"image pull must not run when {binary} is missing")

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which",
        lambda name: None if name == binary else f"/usr/bin/{name}",
    )
    backend = GVisorSandboxBackend(image_manager=_NoPullImageManager())
    with pytest.raises(SandboxCreationError) as err:
        backend.create_sandbox(_public_config())
    assert binary in str(err.value)


def test_terminate_tree_kills_only_a_launch_process_that_hangs(monkeypatch):
    """The launch process shares its owner's process group, so teardown never
    signals a group. It reaps a launch process that exits on its own, and
    SIGKILLs only one that hangs."""
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor._TREE_EXIT_GRACE_SECONDS", 0.2
    )
    exits = subprocess.Popen(["true"])
    hangs = subprocess.Popen(["sleep", "60"])
    signaled_groups = []
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: signaled_groups.append(pgid))
    backend = GVisorSandboxBackend()
    backend._terminate_tree(exits)
    backend._terminate_tree(hangs)
    assert signaled_groups == []
    assert (exits.returncode, hangs.returncode) == (0, -signal.SIGKILL)


def test_render_egress_ruleset_pins_the_policy():
    """The ruleset is the enforcement, so pin its exact text: default drop;
    loopback and DNS to the resolvers on port 53 first; then the denylist;
    then one accept per allowed network in its family."""
    text = _render_egress_ruleset(
        ["52.0.0.0/8", "10.0.1.5/24", "2001:db8::/32"],
        ["8.8.8.8"],
        denied=["169.254.0.0/16", "127.0.0.1/32", "127.0.0.0/8", "fe80::/10"],
    )
    assert text == (
        "flush ruleset\n"
        "table inet ray_sandbox_egress {\n"
        "    chain output {\n"
        "        type filter hook output priority 0; policy drop;\n"
        '        oif "lo" accept\n'
        "        ip daddr 8.8.8.8 udp dport 53 accept\n"
        "        ip daddr 8.8.8.8 tcp dport 53 accept\n"
        "        ip daddr { 127.0.0.0/8, 169.254.0.0/16 } drop\n"
        "        ip6 daddr { fe80::/10 } drop\n"
        "        ip daddr 52.0.0.0/8 accept\n"
        "        ip daddr 10.0.1.0/24 accept\n"
        "        ip6 daddr 2001:db8::/32 accept\n"
        "    }\n"
        "}\n"
    )


def test_render_egress_ruleset_ports_and_internet():
    """A port entry accepts only that port, for TCP and UDP, after the
    denylist. "internet" is every address outside the non-public IPv4
    networks, plus IPv6 global unicast."""
    text = _render_egress_ruleset(
        [
            "10.0.0.5:8000",
            "[2001:db8::1]:443-444",
            "169.254.169.254:80",
            "10.0.0.0/8:22",
            "internet",
        ],
        ["8.8.8.8"],
        denied=["169.254.0.0/16"],
    )
    rules = [line.strip() for line in text.splitlines()[5:-2]]
    not_public = " ".join(f"ip daddr != {net}" for net in INTERNET_EXCLUDED_IPV4)
    assert rules == [
        "ip daddr 8.8.8.8 udp dport 53 accept",
        "ip daddr 8.8.8.8 tcp dport 53 accept",
        "ip daddr { 169.254.0.0/16 } drop",
        "ip daddr 10.0.0.5/32 tcp dport 8000 accept",
        "ip daddr 10.0.0.5/32 udp dport 8000 accept",
        "ip6 daddr 2001:db8::1/128 tcp dport 443-444 accept",
        "ip6 daddr 2001:db8::1/128 udp dport 443-444 accept",
        # The denylist's drop above already took it.
        "ip daddr 169.254.169.254/32 tcp dport 80 accept",
        "ip daddr 169.254.169.254/32 udp dport 80 accept",
        "ip daddr 10.0.0.0/8 tcp dport 22 accept",
        "ip daddr 10.0.0.0/8 udp dport 22 accept",
        f"{not_public} accept",
        "ip6 daddr 2000::/3 accept",
    ]
    assert "ip daddr != 10.0.0.0/8" in not_public
    assert "ip daddr != 198.18.0.0/15" in not_public


def test_render_egress_ruleset_empty_allowlist_keeps_only_dns():
    text = _render_egress_ruleset([], ["8.8.8.8", "1.1.1.1"], denied=["169.254.0.0/16"])
    assert "ip daddr 8.8.8.8 udp dport 53 accept" in text
    assert "ip daddr 1.1.1.1 tcp dport 53 accept" in text
    # Loopback plus two resolvers times two protocols: nothing else.
    assert text.count(" accept") == 5
    with pytest.raises(ValueError, match="dns"):
        _render_egress_ruleset([], ["resolver.internal"], denied=[])


def test_write_egress_ruleset_defaults_to_public_dns_and_adds_the_guard(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    path = _write_egress_ruleset(str(tmp_path), ["10.0.0.0/8"], None)
    assert path == str(tmp_path / "egress.nft")
    text = (tmp_path / "egress.nft").read_text()
    assert "ip daddr 10.0.0.0/8 accept" in text
    for server in DEFAULT_PUBLIC_DNS:
        assert f"ip daddr {server} udp dport 53 accept" in text
    (always_denied,) = [line for line in text.splitlines() if "169.254" in line]
    assert "10.96.0.1/32" in always_denied and always_denied.endswith(" drop")
    # Written atomically: no temp file remains.
    assert [p.name for p in tmp_path.iterdir()] == ["egress.nft"]


def test_write_egress_ruleset_writes_nothing_with_a_bad_denylist(tmp_path, monkeypatch):
    """No ruleset is written when RAY_SANDBOX_EGRESS_DENY can't be read, so a
    typo there never starts a sandbox without the operator's denylist."""
    monkeypatch.setenv("RAY_SANDBOX_EGRESS_DENY", "apiserver.internal")
    with pytest.raises(ValueError, match="apiserver.internal"):
        _write_egress_ruleset(str(tmp_path), ["internet"], None)
    assert list(tmp_path.iterdir()) == []


def test_build_run_command_public_installs_ruleset_before_runsc():
    """Every public sandbox gets its ruleset once the tap is up and before
    runsc starts, and a failed install aborts the start instead of leaving
    egress open."""
    install = (
        "nsenter --preserve-credentials -U -n -t $NSPID -- nft -f /tmp/rd/egress.nft || "
        '{ echo "egress ruleset install failed" >&2; exit 1; }; '
    )
    for cidr_allowlist in (None, ["10.0.0.0/8"]):
        script = _run_argv("public", cidr_allowlist=cidr_allowlist)[2]
        assert install in script
        tap_ready = script.index("[ -s /tmp/rd/slirp4netns.ready ] ||")
        runsc_start = script.index(
            "nsenter --preserve-credentials -U -n -t $NSPID -- "
            "setpriv --pdeathsig KILL -- runsc"
        )
        assert tap_ready < script.index(install) < runsc_start


def test_create_sandbox_requires_nft_for_public(monkeypatch):
    """A missing nft fails a public sandbox fast, before the image pull, with
    remediation, whether or not it has an allowlist."""

    class _NoPullImageManager:
        def pull_image(self, *args, **kwargs):
            raise AssertionError("image pull must not run when nft is missing")

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which",
        lambda name: None if name == "nft" else f"/usr/bin/{name}",
    )
    backend = GVisorSandboxBackend(image_manager=_NoPullImageManager())
    with pytest.raises(SandboxCreationError, match="nft"):
        backend.create_sandbox(_public_config())


class _StubImageManager:
    """Just enough of an image manager to reach the run command."""

    def __init__(self):
        self.released = []

    def pull_image(self, *args, **kwargs):
        return None

    def get_workdir(self, image):
        return None

    def prepare_oci_bundle(self, root_dir, **kwargs):
        path = os.path.join(root_dir, "config.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"root": {"path": "rootfs"}}, f)
        return path

    def release_image(self, image, instance_id):
        self.released.append(instance_id)


def test_create_public_sandbox_installs_the_default_allowlist(tmp_path, monkeypatch):
    """A public sandbox created without an allowlist runs under the default,
    the public internet behind the guard; if the guard can't be read, the
    sandbox doesn't start."""
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor._RAY_SANDBOX_DIR", str(tmp_path)
    )
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )

    class _Started(Exception):
        pass

    rulesets = []

    def fake_popen(args, **kwargs):
        # The failed create deletes its bundle, so read the ruleset now.
        (root_dir,) = tmp_path.iterdir()
        rulesets.append((root_dir / "egress.nft").read_text())
        raise _Started(args)

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.Popen", fake_popen
    )
    backend = GVisorSandboxBackend(image_manager=_StubImageManager())
    with pytest.raises(SandboxCreationError) as err:
        backend.create_sandbox(_public_config())
    assert isinstance(err.value.__cause__, _Started)
    (text,) = rulesets
    not_public = " ".join(f"ip daddr != {net}" for net in INTERNET_EXCLUDED_IPV4)
    assert f"{not_public} accept" in text
    assert "169.254.0.0/16" in text

    monkeypatch.setenv("RAY_SANDBOX_EGRESS_DENY", "apiserver.internal")
    images = _StubImageManager()
    backend = GVisorSandboxBackend(image_manager=images)
    bundles = set(tmp_path.iterdir())
    with pytest.raises(SandboxCreationError, match="apiserver.internal"):
        backend.create_sandbox(_public_config())
    assert len(images.released) == 1
    # The failed create deletes its bundle rather than leaving it locked.
    assert set(tmp_path.iterdir()) <= bundles


def test_set_egress_allowlist_needs_a_public_sandbox(tmp_path):
    """Only a network="public" sandbox has an allowlist, and the change never
    runs without the namespace holder (`flush ruleset` must not run anywhere
    but inside the sandbox's own namespace)."""
    backend = GVisorSandboxBackend(image_manager=object())
    with pytest.raises(SandboxNotFoundError):
        backend.set_egress_allowlist("nope", ["10.0.0.0/8"])

    backend._sandbox_metadata["sb-none"] = {
        "root_dir": str(tmp_path),
        "config": GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh"),
        "egress_allowlist": None,
    }
    with pytest.raises(SandboxError, match="only network='public'"):
        backend.set_egress_allowlist("sb-none", ["10.0.0.0/8"])

    backend._sandbox_metadata["sb-public"] = {
        "root_dir": str(tmp_path),
        "config": _public_config(),
        "egress_allowlist": ["internet"],
    }
    with pytest.raises(ValueError, match="example.com"):
        backend.set_egress_allowlist("sb-public", ["example.com"])
    with pytest.raises(SandboxError, match="namespace holder"):
        backend.set_egress_allowlist("sb-public", ["10.0.0.0/8"])
    assert not (tmp_path / "egress.nft").exists()


# Tests that use this process as a sandbox's namespace holder.
_needs_proc_ns = pytest.mark.skipif(
    not os.path.isdir("/proc/self/ns"), reason="needs Linux /proc/<pid>/ns"
)


def _fd_namespace_ids(fds) -> list:
    return [(os.fstat(fd).st_dev, os.fstat(fd).st_ino) for fd in fds]


@_needs_proc_ns
def test_set_egress_allowlist_reinstalls_ruleset_in_the_namespace(
    tmp_path, monkeypatch
):
    """The update rewrites the ruleset and loads it through fds of the
    holder's namespaces, closed afterwards; a failed load keeps the file
    describing the rules in force."""
    (tmp_path / "netns.pid").write_text(f"{os.getpid()}\n")
    holder_ns = _namespace_ids(os.getpid())
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        network="public",
        cidr_allowlist=["0.0.0.0/0"],
        dns=["10.0.0.2"],
    )
    _write_egress_ruleset(str(tmp_path), cfg.cidr_allowlist, cfg.dns)
    backend = GVisorSandboxBackend(image_manager=object())
    backend._sandbox_metadata["sb-1"] = {
        "root_dir": str(tmp_path),
        "config": cfg,
        "egress_allowlist": ["0.0.0.0/0"],
        "netns_ids": holder_ns,
    }

    class _Completed:
        def __init__(self, returncode, stderr=b""):
            self.returncode = returncode
            self.stderr = stderr

    calls = []

    def fake_run(args, pass_fds=(), **kwargs):
        calls.append((args, list(pass_fds), _fd_namespace_ids(pass_fds)))
        return _Completed(0)

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run", fake_run
    )
    backend.set_egress_allowlist("sb-1", ["52.0.0.1"])
    ((args, (user_fd, net_fd), passed_ns),) = calls
    assert args == [
        "nsenter",
        "--preserve-credentials",
        f"--user=/proc/self/fd/{user_fd}",
        f"--net=/proc/self/fd/{net_fd}",
        "--",
        "nft",
        "-f",
        str(tmp_path / "egress.nft"),
    ]
    assert passed_ns == holder_ns
    for fd in (user_fd, net_fd):
        # Closed (or reused for something else): no namespace fd leaks.
        try:
            assert _fd_namespace_ids([fd])[0] not in holder_ns
        except OSError:
            pass
    text = (tmp_path / "egress.nft").read_text()
    assert "ip daddr 52.0.0.1/32 accept" in text
    assert "0.0.0.0/0" not in text
    assert "ip daddr 10.0.0.2 udp dport 53 accept" in text
    assert backend._sandbox_metadata["sb-1"]["egress_allowlist"] == ["52.0.0.1/32"]

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run",
        lambda args, **kwargs: _Completed(1, b"Error: Protocol not supported"),
    )
    with pytest.raises(SandboxError, match="Protocol not supported"):
        backend.set_egress_allowlist("sb-1", ["10.0.0.0/8"])
    assert "ip daddr 52.0.0.1/32 accept" in (tmp_path / "egress.nft").read_text()
    assert backend._sandbox_metadata["sb-1"]["egress_allowlist"] == ["52.0.0.1/32"]


@_needs_proc_ns
def test_set_egress_allowlist_refuses_a_reused_holder_pid(tmp_path, monkeypatch):
    """Once the holder exits, its pid can name any process. An update must
    then be refused before nft runs, whatever that process is."""
    # A live process outside the sandbox's namespaces: this one.
    (tmp_path / "netns.pid").write_text(f"{os.getpid()}\n")
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        network="public",
        cidr_allowlist=["1.1.1.1"],
    )
    rules = Path(_write_egress_ruleset(str(tmp_path), cfg.cidr_allowlist, cfg.dns))
    before = rules.read_text()
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run",
        lambda *args, **kwargs: pytest.fail("nft ran outside the sandbox"),
    )
    backend = GVisorSandboxBackend(image_manager=object())
    # The namespaces recorded at creation, which this process isn't in; then
    # a sandbox whose namespaces are unknown.
    for recorded in ([(0, 1), (0, 2)], None):
        backend._sandbox_metadata["sb-1"] = {
            "root_dir": str(tmp_path),
            "config": cfg,
            "egress_allowlist": ["1.1.1.1/32"],
            "netns_ids": recorded,
        }
        with pytest.raises(SandboxError, match="namespace holder"):
            backend.set_egress_allowlist("sb-1", list(ALLOW_ALL_CIDRS))
        assert rules.read_text() == before
        assert backend._sandbox_metadata["sb-1"]["egress_allowlist"] == ["1.1.1.1/32"]


@_needs_proc_ns
def test_holder_namespace_ids_reject_a_pid_in_the_workers_namespaces(tmp_path):
    """Creation records the holder's namespaces only if netns.pid names a
    process outside this one's, so a later update can't be pointed here."""
    assert _holder_namespace_ids(str(tmp_path)) is None
    for pid in (str(os.getpid()), "not-a-pid"):
        (tmp_path / "netns.pid").write_text(f"{pid}\n")
        assert _holder_namespace_ids(str(tmp_path)) is None


def test_holder_namespace_ids_accept_a_holder_in_the_workers_user_namespace(
    tmp_path, monkeypatch
):
    """An overlayfs sandbox on a worker with mount privileges keeps the
    worker's user namespace, so only a holder in the worker's network
    namespace is taken for a pid that isn't the holder."""
    own = [(1, 10), (1, 20)]
    holder = [(1, 10), (1, 21)]
    monkeypatch.setattr(
        gvisor, "_namespace_ids", lambda pid: holder if pid == 12345 else own
    )
    (tmp_path / "netns.pid").write_text("12345\n")
    assert _holder_namespace_ids(str(tmp_path)) == holder


@_needs_proc_ns
def test_set_egress_allowlist_skips_the_workers_own_user_namespace(
    tmp_path, monkeypatch
):
    """A sandbox in this process's user namespace has its ruleset reloaded in
    its network namespace alone, since nsenter can't enter the user
    namespace it's already in."""
    (tmp_path / "netns.pid").write_text(f"{os.getpid()}\n")
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        network="public",
        cidr_allowlist=["0.0.0.0/0"],
    )
    _write_egress_ruleset(str(tmp_path), cfg.cidr_allowlist, cfg.dns)
    backend = GVisorSandboxBackend(image_manager=object())
    backend._sandbox_metadata["sb-1"] = {
        "root_dir": str(tmp_path),
        "config": cfg,
        "egress_allowlist": ["0.0.0.0/0"],
        "netns_ids": _namespace_ids(os.getpid()),
        "enter_userns": False,
    }
    calls = []

    class _Completed:
        returncode = 0
        stderr = b""

    def fake_run(args, **kwargs):
        calls.append(args)
        return _Completed()

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run", fake_run
    )
    backend.set_egress_allowlist("sb-1", ["52.0.0.1"])
    ((args),) = calls
    assert not any(a.startswith("--user=") for a in args)
    assert any(a.startswith("--net=") for a in args)


@_needs_proc_ns
def test_set_egress_allowlist_serializes_concurrent_updates(tmp_path, monkeypatch):
    """Overlapping updates run one at a time, so the ruleset loaded last is
    the list the backend records."""
    (tmp_path / "netns.pid").write_text(f"{os.getpid()}\n")
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        network="public",
        cidr_allowlist=["0.0.0.0/0"],
        dns=["10.0.0.2"],
    )
    _write_egress_ruleset(str(tmp_path), cfg.cidr_allowlist, cfg.dns)
    backend = GVisorSandboxBackend(image_manager=object())
    backend._sandbox_metadata["sb-1"] = {
        "root_dir": str(tmp_path),
        "config": cfg,
        "egress_allowlist": ["0.0.0.0/0"],
        "netns_ids": _namespace_ids(os.getpid()),
    }
    loaded = []
    inside = [0, 0]  # [current, peak]
    counter_lock = threading.Lock()

    class _Completed:
        returncode = 0
        stderr = b""

    def slow_nft(args, **kwargs):
        with counter_lock:
            inside[0] += 1
            inside[1] = max(inside[1], inside[0])
        loaded.append((tmp_path / "egress.nft").read_text())
        time.sleep(0.05)
        with counter_lock:
            inside[0] -= 1
        return _Completed()

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run", slow_nft
    )
    threads = [
        threading.Thread(target=backend.set_egress_allowlist, args=("sb-1", [cidr]))
        for cidr in ("52.0.0.1", "52.0.0.2", "52.0.0.3")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert inside[1] == 1
    (recorded,) = backend._sandbox_metadata["sb-1"]["egress_allowlist"]
    assert f"ip daddr {recorded} accept" in loaded[-1]
    assert f"ip daddr {recorded} accept" in (tmp_path / "egress.nft").read_text()


def _fake_public_sandbox(root_dir: Path, cidrs: list) -> dict:
    """Metadata of a public sandbox whose namespace holder is this process."""
    root_dir.mkdir()
    (root_dir / "netns.pid").write_text(f"{os.getpid()}\n")
    cfg = GVisorSandboxConfig(
        image="busybox:latest", shell="/bin/sh", network="public", cidr_allowlist=cidrs
    )
    _write_egress_ruleset(str(root_dir), cfg.cidr_allowlist, cfg.dns)
    return {
        "root_dir": str(root_dir),
        "config": cfg,
        "egress_allowlist": cfg.cidr_allowlist,
        "netns_ids": _namespace_ids(os.getpid()),
    }


def test_resolver_probe_names_the_first_unreachable_resolver():
    """The probe sends each resolver one datagram and names the first whose
    send fails. A broadcast address fails on any host, because the probe's
    socket doesn't set SO_BROADCAST."""
    probe = [sys.executable, "-I", "-c", _RESOLVER_PROBE]
    reached = subprocess.run(
        probe + ["127.0.0.1"], capture_output=True, text=True, timeout=30
    )
    assert reached.returncode == 0, reached.stderr
    failed = subprocess.run(
        probe + ["127.0.0.1", "255.255.255.255"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert failed.returncode == _RESOLVER_UNREACHABLE, failed.stderr
    server, reason = failed.stdout.strip().split("\t")
    assert server == "255.255.255.255"
    assert reason


@_needs_proc_ns
def test_unreachable_resolver_probes_inside_the_sandbox(tmp_path, monkeypatch):
    """The probe runs in the sandbox's network namespace, entered through an
    fd, and enters its user namespace only if this process isn't root. Only
    its own verdict counts, so a probe that can't run blocks nothing."""
    backend = GVisorSandboxBackend(image_manager=object())
    backend._sandbox_metadata["sb-1"] = _fake_public_sandbox(
        tmp_path / "sb", ["internet"]
    )
    results = []
    calls = []

    def fake_run(args, pass_fds=(), **kwargs):
        calls.append((args, _fd_namespace_ids(pass_fds)))
        result = results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run", fake_run
    )
    results.append(
        subprocess.CompletedProcess(
            [], _RESOLVER_UNREACHABLE, "10.16.0.10\tOperation not permitted\n", ""
        )
    )
    assert backend._unreachable_resolver("sb-1", ["10.16.0.10"]) == (
        "10.16.0.10",
        "Operation not permitted",
    )
    ((args, passed_ns),) = calls
    assert args[:2] == ["nsenter", "--preserve-credentials"]
    assert args[-5:] == [sys.executable, "-I", "-c", _RESOLVER_PROBE, "10.16.0.10"]
    assert passed_ns == _namespace_ids(os.getpid())
    entered = [arg.split("=")[0] for arg in args if arg.startswith("--") and "=" in arg]
    assert entered == (["--net"] if os.geteuid() == 0 else ["--user", "--net"])

    for result in (
        subprocess.CompletedProcess([], 0, "", ""),
        subprocess.CompletedProcess([], 1, "", "nsenter: reassociate failed"),
        subprocess.TimeoutExpired("nsenter", 10),
    ):
        results.append(result)
        assert backend._unreachable_resolver("sb-1", ["10.16.0.10"]) is None


def test_netns_concurrent_same_port_bind_and_isolation(ensure_slirp4netns):
    """Two "public" sandboxes both bind 0.0.0.0:2222 (the terminal-bench QEMU
    hostfwd contract): each reaches its own listener, the bind never surfaces in
    the worker's namespace, and neither sandbox can reach the other's."""
    backend = GVisorSandboxBackend()
    sb1, sb2 = (backend.create_sandbox(_public_config()) for _ in range(2))
    tokens = {sb1: "SB1-TOKEN", sb2: "SB2-TOKEN"}
    try:
        for sb, token in tokens.items():
            # /tmp stays a writable tmpfs on the readonly rootfs.
            backend.write_file(sb, "/tmp/www/token", token)
            # busybox httpd daemonizes; sharing one netns, the second bind
            # would fail with EADDRINUSE.
            res = backend.exec_command(sb, "httpd -p 2222 -h /tmp/www", timeout=30)
            assert res.exit_code == 0, res.stderr

        for sb, token in tokens.items():
            res = backend.exec_command(
                sb, "wget -q -T 5 -O - http://127.0.0.1:2222/token", timeout=30
            )
            assert res.exit_code == 0, res.stderr
            assert token in res.stdout

        # The worker's own namespace must see nothing on 2222.
        host_ip = _host_ip()
        for target in ("127.0.0.1", host_ip):
            with pytest.raises(OSError):
                socket.create_connection((target, 2222), timeout=3).close()

        # No address names one sandbox from another: every sandbox is
        # 198.18.0.100 in its own namespace, and the worker's IP reaches the
        # worker, which has nothing on 2222.
        res = backend.exec_command(
            sb2, f"wget -q -T 3 -O - http://{host_ip}:2222/token", timeout=30
        )
        assert tokens[sb1] not in res.stdout
        assert res.exit_code != 0 or tokens[sb2] in res.stdout
    finally:
        for sb in (sb1, sb2):
            backend.delete_sandbox(sb)


def test_netns_egress_and_dns(ensure_slirp4netns):
    """Egress and the generated resolv.conf work from inside the netns."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(_public_config())
    try:
        res = backend.exec_command(
            sb, "wget -q -T 15 -O - http://example.com", timeout=60
        )
        assert res.exit_code == 0, res.stderr
        assert "Example" in res.stdout
    finally:
        backend.delete_sandbox(sb)


# Exit 0 when a TCP connection to 1.1.1.1:80 succeeds; the connect times out
# (exit 1) when the egress filter drops the SYN.
_CONNECT_PROBE = (
    'python3 -c "import socket; '
    "socket.create_connection(('1.1.1.1', 80), timeout=5).close()\""
)


def test_netns_egress_allowlist_enforced(ensure_slirp4netns):
    """Only allowlisted destinations are reachable, DNS to the resolvers stays
    open although they are not listed, and everything else is dropped."""
    backend = GVisorSandboxBackend()
    cfg = GVisorSandboxConfig(
        image="python:3.10-slim", network="public", cidr_allowlist=["1.1.1.1/32"]
    )
    sb = backend.create_sandbox(cfg)
    try:
        res = backend.exec_command(sb, _CONNECT_PROBE, timeout=60)
        assert res.exit_code == 0, res.stderr

        # An unlisted address: the SYN is dropped, so the connect times out.
        # Widening to it afterwards proves the policy, not the network,
        # blocked it.
        other = _CONNECT_PROBE.replace("1.1.1.1", "1.0.0.1")
        res = backend.exec_command(sb, other, timeout=60)
        assert res.exit_code != 0
        assert "timed out" in res.stderr, res.stderr
        backend.set_egress_allowlist(sb, ["1.1.1.1", "1.0.0.1"])
        res = backend.exec_command(sb, other, timeout=60)
        assert res.exit_code == 0, res.stderr

        res = backend.exec_command(
            sb,
            "python3 -c \"import socket; print(socket.gethostbyname('example.com'))\"",
            timeout=60,
        )
        assert res.exit_code == 0, res.stderr
        assert res.stdout.strip().count(".") == 3
    finally:
        backend.delete_sandbox(sb)


def _probe(host: str, port: int) -> str:
    """A command that prints how a TCP connect from the sandbox ends."""
    return (
        'python3 -c "import socket\n'
        "try:\n"
        f"    socket.create_connection(('{host}', {port}), timeout=5).close(); print('open')\n"
        "except socket.timeout:\n"
        "    print('timeout')\n"
        "except OSError as e:\n"
        "    print(type(e).__name__)\n"
        '"'
    )


def test_netns_egress_ports_and_internet(ensure_slirp4netns):
    """A port entry opens one port on an address that is otherwise closed, and
    "internet" reaches public addresses but not the node's own network."""
    host = _host_ip()
    listeners = [socket.create_server((host, 0)) for _ in range(2)]
    allowed, other = (s.getsockname()[1] for s in listeners)
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="python:3.10-slim",
            network="public",
            cidr_allowlist=["internet", f"{host}:{allowed}"],
        )
    )
    try:
        reach = {
            name: backend.exec_command(sb, _probe(h, p), timeout=60).stdout.strip()
            for name, (h, p) in {
                "allowed port": (host, allowed),
                "other port": (host, other),
                "public address": ("1.1.1.1", 443),
            }.items()
        }
        assert reach == {
            "allowed port": "open",
            "other port": "timeout",
            "public address": "open",
        }
    finally:
        backend.delete_sandbox(sb)
        for listener in listeners:
            listener.close()


def test_netns_default_policy_leaves_out_internal_addresses(
    ensure_slirp4netns, monkeypatch
):
    """A public sandbox created without an allowlist reaches the internet but
    not the worker's own address, where Ray's ports listen, because it's an
    internal address; nor instance metadata or the Kubernetes API, which the
    denylist closes. Ray doesn't track its nodes, so an allowlist that covers
    internal networks reaches the worker, until RAY_SANDBOX_EGRESS_DENY
    lists it."""
    host = _host_ip()
    if ipaddress.ip_address(host).is_global:
        pytest.skip("the worker's address is public, so internet covers it")
    listener = socket.create_server((host, 0))
    port = listener.getsockname()[1]
    targets = {
        "worker": (host, port),
        "metadata": ("169.254.169.254", 80),
        "internet": ("1.1.1.1", 443),
    }
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        targets["kubernetes"] = (
            os.environ["KUBERNETES_SERVICE_HOST"],
            int(os.environ.get("KUBERNETES_SERVICE_PORT", "443")),
        )
    closed = {name: "timeout" for name in targets if name != "internet"}
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(image="python:3.10-slim", network="public")
    )

    def reach() -> dict:
        return {
            name: backend.exec_command(sb, _probe(h, p), timeout=60).stdout.strip()
            for name, (h, p) in targets.items()
        }

    try:
        assert reach() == {**closed, "internet": "open"}
        # A cluster that rewrites Service IPs at connect time, as Cilium does,
        # sends the Kubernetes API Service to the API server's own address,
        # which allow-all opens unless RAY_SANDBOX_EGRESS_DENY lists it.
        targets.pop("kubernetes", None)
        closed.pop("kubernetes", None)
        backend.set_egress_allowlist(sb, list(ALLOW_ALL_CIDRS))
        assert reach() == {**closed, "worker": "open", "internet": "open"}
        monkeypatch.setenv("RAY_SANDBOX_EGRESS_DENY", f"{host}/32")
        backend.set_egress_allowlist(sb, list(ALLOW_ALL_CIDRS))
        assert reach() == {**closed, "internet": "open"}
    finally:
        backend.delete_sandbox(sb)
        listener.close()


def test_netns_egress_allowlist_runtime_update(ensure_slirp4netns):
    """A sandbox created with an allowlist can be widened and narrowed while
    it runs; each change applies to the next connection."""
    backend = GVisorSandboxBackend()
    cfg = GVisorSandboxConfig(
        image="python:3.10-slim", network="public", cidr_allowlist=[]
    )
    sb = backend.create_sandbox(cfg)
    try:
        assert backend.exec_command(sb, _CONNECT_PROBE, timeout=60).exit_code != 0
        backend.set_egress_allowlist(sb, ["1.1.1.1"])
        res = backend.exec_command(sb, _CONNECT_PROBE, timeout=60)
        assert res.exit_code == 0, res.stderr
        backend.set_egress_allowlist(sb, [])
        assert backend.exec_command(sb, _CONNECT_PROBE, timeout=60).exit_code != 0
    finally:
        backend.delete_sandbox(sb)


def test_netns_unreachable_dns_resolver_fails_the_create(ensure_slirp4netns):
    """A dns resolver the sandbox can't reach fails the create with the
    reason, rather than every lookup in the sandbox. Sandboxes have no IPv6
    route, so an IPv6 resolver stands in for a Service IP that the cluster's
    network translates and the egress filter then drops."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="busybox:latest",
            shell="/bin/sh",
            network="public",
            dns=["1.1.1.1", "8.8.8.8"],
        )
    )
    backend.delete_sandbox(sb)

    before = _slirp4netns_pids()
    with pytest.raises(
        SandboxCreationError, match="2001:4860:4860::8888 .* Network is unreachable"
    ):
        backend.create_sandbox(
            GVisorSandboxConfig(
                image="busybox:latest",
                shell="/bin/sh",
                network="public",
                dns=["1.1.1.1", "2001:4860:4860::8888"],
            )
        )
    assert backend._sandbox_metadata == {}
    assert _slirp4netns_pids() == before


def _nft_ruleset(*nsenter_target: str) -> tuple:
    """``nft list ruleset`` here, or in the namespaces nsenter's args name."""
    enter = ["nsenter", "--preserve-credentials", *nsenter_target, "--"]
    proc = subprocess.run(
        (enter if nsenter_target else []) + ["nft", "list", "ruleset"],
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout


def test_netns_egress_allowlist_update_refuses_a_reused_holder_pid(
    ensure_slirp4netns,
):
    """Once the namespace holder exits, its pid can name another process. An
    update must then be refused, leaving that process's ruleset alone, whether
    it shares the worker's namespaces (the pod's ruleset, for a root worker)
    or has its own, as another sandbox's holder does. The real holder still
    takes updates afterwards."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="python:3.10-slim", network="public", cidr_allowlist=["1.1.1.1"]
        )
    )
    pidfile = Path(backend._sandbox_metadata[sb]["root_dir"]) / "netns.pid"
    holder = pidfile.read_text().strip()
    worker_peer = subprocess.Popen(["sleep", "300"])
    other_holder = subprocess.Popen(
        ["unshare", "--user", "--map-root-user", "--net"]
        + ["sh", "-c", "echo up; exec sleep 300"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert other_holder.stdout.readline() == "up\n"
        rulesets = {
            "worker's namespaces": (worker_peer.pid, lambda: _nft_ruleset()),
            "own namespaces": (
                other_holder.pid,
                lambda: _nft_ruleset("-U", "-n", "-t", str(other_holder.pid)),
            ),
        }
        outcomes = {}
        for name, (pid, ruleset) in rulesets.items():
            before = ruleset()
            pidfile.write_text(f"{pid}\n")
            try:
                # Open egress, so a regression can't cut off the namespace it
                # lands in.
                backend.set_egress_allowlist(sb, list(ALLOW_ALL_CIDRS))
                refused = False
            except SandboxError as err:
                refused = "namespace holder" in str(err)
            outcomes[name] = {"refused": refused, "untouched": ruleset() == before}
        assert outcomes == {
            name: {"refused": True, "untouched": True} for name in rulesets
        }
        assert backend._sandbox_metadata[sb]["egress_allowlist"] == ["1.1.1.1/32"]
        assert "ip daddr 1.1.1.1 accept" in _nft_ruleset("-U", "-n", "-t", holder)[1]

        pidfile.write_text(f"{holder}\n")
        backend.set_egress_allowlist(sb, ["1.0.0.1"])
        assert "ip daddr 1.0.0.1 accept" in _nft_ruleset("-U", "-n", "-t", holder)[1]
        reach = backend.exec_command(sb, _probe("1.0.0.1", 443), timeout=60)
        assert reach.stdout.strip() == "open", reach.stderr
    finally:
        for process in (worker_peer, other_holder):
            process.kill()
            process.communicate()
        backend.delete_sandbox(sb)


def test_netns_teardown_reaps_slirp4netns(ensure_slirp4netns):
    """delete_sandbox ends the slirp4netns process tree and removes all state."""
    before = _slirp4netns_pids()
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(_public_config())
    meta = backend._sandbox_metadata[sb]
    assert meta["proc"].poll() is None
    assert _slirp4netns_pids() > before

    backend.delete_sandbox(sb)
    assert meta["proc"].poll() is not None
    assert _slirp4netns_pids() == before
    assert not os.path.exists(meta["root_dir"])
    assert sb not in backend._sandbox_metadata


_PR_SET_CHILD_SUBREAPER = 36


def _stat(pid) -> tuple:
    """(state, ppid, comm) of a process, or None once it's fully gone."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    comm = stat[stat.index("(") + 1 : stat.rindex(")")]
    state, ppid = stat[stat.rindex(")") + 2 :].split()[:2]
    return state, int(ppid), comm


def _zombie_children() -> dict:
    """{pid: comm} of this process's zombie children."""
    zombies = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            info = _stat(entry.name)
            if info and info[0] == "Z" and info[1] == os.getpid():
                zombies[int(entry.name)] = info[2]
    return zombies


@contextlib.contextmanager
def _subreaper():
    """Adopt orphaned descendants of this test process.

    Whatever the container's init does with orphans, a helper the backend
    fails to reap then shows up as this process's zombie. The context reaps
    them on exit.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        pytest.skip("prctl(PR_SET_CHILD_SUBREAPER) isn't available")
    try:
        yield
    finally:
        libc.prctl(_PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0)
        for pid in _zombie_children():
            os.waitpid(pid, os.WNOHANG)


def _netns_helpers(root_dir: str) -> dict:
    """{pid: comm} of a public sandbox's namespace holder and slirp4netns."""
    nspid = int(Path(root_dir, "netns.pid").read_text())
    helpers = {nspid: "sleep", _stat(nspid)[1]: "unshare"}
    for entry in Path("/proc").iterdir():
        try:
            argv = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if os.path.basename(argv[0]) == b"slirp4netns" and (
            f"/proc/{nspid}/ns/net".encode() in argv
        ):
            helpers[int(entry.name)] = "slirp4netns"
    return helpers


def test_netns_teardown_leaves_no_zombies(ensure_slirp4netns):
    """The launch shell reaps the holder and slirp4netns. When it exec'd
    runsc, they outlived their parent and became zombies of the pod's init."""
    backend = GVisorSandboxBackend()
    with _subreaper():
        before = _zombie_children()
        for _ in range(3):
            sb = backend.create_sandbox(_public_config())
            helpers = _netns_helpers(backend._sandbox_metadata[sb]["root_dir"])
            assert sorted(helpers.values()) == ["sleep", "slirp4netns", "unshare"]
            backend.delete_sandbox(sb)
            assert all(_stat(pid) is None for pid in helpers), helpers
        assert _zombie_children() == before


def test_netns_launch_shell_death_stops_the_sandbox(ensure_slirp4netns):
    """Ray kills a dead worker's child processes, which for a public sandbox
    is only the launch shell. Everything the shell started must die with it."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(_public_config())
    meta = backend._sandbox_metadata[sb]
    try:
        with _subreaper():
            helpers = _netns_helpers(meta["root_dir"])
            sandbox_pids = [
                int(entry.name)
                for entry in Path("/proc").iterdir()
                if entry.name.isdigit()
                and sb.encode() in _cmdline(entry)
                and int(entry.name) != meta["proc"].pid
            ]
            assert sandbox_pids
            os.kill(meta["proc"].pid, signal.SIGKILL)
            meta["proc"].wait()
            deadline = time.monotonic() + 10
            alive = None
            while time.monotonic() < deadline:
                alive = {
                    pid
                    for pid in [*helpers, *sandbox_pids]
                    if (_stat(pid) or ("Z",))[0] != "Z"
                }
                if not alive:
                    break
                time.sleep(0.2)
            assert not alive, alive
    finally:
        backend.delete_sandbox(sb)


def _cmdline(entry: Path) -> bytes:
    try:
        return (entry / "cmdline").read_bytes()
    except OSError:
        return b""


class _RecordingImageManager:
    def __init__(self):
        self.released = []

    def pull_image(self, *args, **kwargs):
        raise RuntimeError("pull failed")

    def release_image(self, image, instance_id):
        self.released.append((image, instance_id))


@pytest.fixture
def private_bundles(tmp_path, monkeypatch):
    """Point the backend at an empty bundle directory, with a fake runsc that
    logs its arguments to ``runsc.log`` in the returned directory. Creates
    start no background reclaim, so only the test reclaims there."""
    monkeypatch.setattr(gvisor, "_RAY_SANDBOX_DIR", str(tmp_path / "sandbox"))
    monkeypatch.setattr(
        GVisorSandboxBackend, "_maybe_reclaim_unowned_bundles", lambda self: None
    )
    runsc = tmp_path / "runsc"
    runsc.write_text('#!/bin/sh\necho "$@" >> "$(dirname "$0")/runsc.log"\n')
    runsc.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    return tmp_path


def test_reclaim_takes_only_bundles_whose_owner_is_gone(private_bundles, monkeypatch):
    """A bundle whose owner file nobody holds loses its runsc state, its files
    and its image pin. A live owner's bundle, one still being created, one an
    earlier Ray made, and anything not named like a bundle stay."""
    monkeypatch.delenv("RAY_SANDBOX_IGNORE_CGROUPS")
    images = _RecordingImageManager()
    backend = GVisorSandboxBackend(image_manager=images)
    bundles = Path(gvisor._RAY_SANDBOX_DIR)

    def bundle(name, image=None, **config):
        root = bundles / name
        root.mkdir(parents=True)
        (root / "overlay").write_bytes(b"\0" * 1024)
        if image:
            config = GVisorSandboxConfig(image=image, **config)
            return backend._own_bundle(config, str(root))

    live = bundle("ray-sandbox-00000000000a", "busybox:latest")
    bundle("ray-sandbox-00000000000b", "python:3.12-slim", rootless=False).close()
    bundle("ray-sandbox-00000000000c")  # Being created, or from an earlier Ray.
    bundle("ray-sandbox-other", "busybox:latest").close()
    try:
        assert backend._reclaim_unowned_bundles() == ["ray-sandbox-00000000000b"]
        assert sorted(os.listdir(bundles)) == [
            "ray-sandbox-00000000000a",
            "ray-sandbox-00000000000c",
            "ray-sandbox-other",
        ]
    finally:
        live.close()
    assert images.released == [("python:3.12-slim", "ray-sandbox-00000000000b")]
    # With the owner's flags, and a kill first: an owner that wasn't a Ray
    # worker may have left its sandbox running.
    assert (private_bundles / "runsc.log").read_text().splitlines() == [
        "--root /tmp/runsc kill ray-sandbox-00000000000b SIGKILL",
        "--root /tmp/runsc delete -force ray-sandbox-00000000000b",
    ]

    # The owner of the first bundle closed its file, so it goes next.
    assert backend._reclaim_unowned_bundles() == ["ray-sandbox-00000000000a"]


def test_reclaim_never_runs_a_command_from_the_owner_file(private_bundles):
    """The bundle directory may be writable by others, and so the owner file:
    reclaiming runs runsc from PATH with known flags only."""
    root = Path(gvisor._RAY_SANDBOX_DIR, "ray-sandbox-00000000000d")
    root.mkdir(parents=True)
    marker = private_bundles / "ran"
    (root / ".owner").write_text(
        json.dumps(
            {
                "image": "busybox:latest",
                "runsc_args": ["/bin/sh", "-c", f"touch {marker}", "--"],
            }
        )
    )
    backend = GVisorSandboxBackend(image_manager=_RecordingImageManager())
    assert backend._reclaim_unowned_bundles() == ["ray-sandbox-00000000000d"]
    assert not marker.exists()
    assert (private_bundles / "runsc.log").read_text().splitlines()[0] == (
        "--root /tmp/runsc kill ray-sandbox-00000000000d SIGKILL"
    )


def test_reclaim_deletes_the_owner_file_last(private_bundles, monkeypatch):
    """A reclaim pass runs in a daemon thread, so it can be cut short, say with
    the process that runs it. Everything but the owner file goes first, so a
    later pass still finds the bundle and finishes."""
    root = Path(gvisor._RAY_SANDBOX_DIR, "ray-sandbox-00000000000e")
    (root / "workdir").mkdir(parents=True)
    (root / "workdir" / "file").write_text("data")
    (root / "overlay").write_bytes(b"\0" * 1024)
    images = _RecordingImageManager()
    backend = GVisorSandboxBackend(image_manager=images)
    config = GVisorSandboxConfig(image="busybox:latest")
    backend._own_bundle(config, str(root)).close()
    rmtree = shutil.rmtree

    def cut_short(path, *args, **kwargs):
        if Path(path) == root:
            raise RuntimeError("the pass was cut short")
        rmtree(path, *args, **kwargs)

    monkeypatch.setattr(shutil, "rmtree", cut_short)
    assert backend._reclaim_unowned_bundles() == []
    assert os.listdir(root) == [".owner"]

    monkeypatch.setattr(shutil, "rmtree", rmtree)
    assert backend._reclaim_unowned_bundles() == ["ray-sandbox-00000000000e"]
    assert not root.exists()
    assert ("busybox:latest", "ray-sandbox-00000000000e") in images.released


@pytest.mark.parametrize("failure", ["pull", "owner file", "oci bundle"])
def test_failed_create_leaves_no_bundle(private_bundles, monkeypatch, failure):
    """A create that fails before its sandbox starts deletes its bundle, even
    one without an owner file for a reclaim to find, and releases its image."""
    images = _RecordingImageManager()
    backend = GVisorSandboxBackend(image_manager=images)
    if failure == "owner file":

        def no_space(*args):
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

        monkeypatch.setattr(backend, "_own_bundle", no_space)
    elif failure == "oci bundle":

        def bad_spec(**kwargs):
            raise ValueError("bad OCI spec")

        monkeypatch.setattr(images, "pull_image", lambda *args, **kwargs: "/image")
        monkeypatch.setattr(images, "get_workdir", lambda image: None, raising=False)
        monkeypatch.setattr(images, "prepare_oci_bundle", bad_spec, raising=False)
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(GVisorSandboxConfig(image="busybox:latest"))
    assert os.listdir(gvisor._RAY_SANDBOX_DIR) == []
    assert [image for image, _ in images.released] == ["busybox:latest"]


def test_reclaim_runs_in_the_background(monkeypatch):
    """A create starts a reclaim pass without waiting for it, at most once a
    minute per process."""
    started, release = threading.Event(), threading.Event()
    passes = []

    def slow_reclaim(self):
        passes.append(self)
        started.set()
        release.wait(10)
        return []

    monkeypatch.setattr(GVisorSandboxBackend, "_reclaim_unowned_bundles", slow_reclaim)
    monkeypatch.setattr(gvisor, "_last_reclaim", float("-inf"))
    backend = GVisorSandboxBackend(image_manager=_RecordingImageManager())
    start = time.monotonic()
    backend._maybe_reclaim_unowned_bundles()
    backend._maybe_reclaim_unowned_bundles()
    try:
        assert time.monotonic() - start < 1
        assert started.wait(5)
        assert len(passes) == 1
    finally:
        release.set()


# Creates a sandbox, prints what the test needs to find it, and waits.
_OWNER = r"""
import json, signal, sys, time
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig

backend = GVisorSandboxBackend()
sandbox_id = backend.create_sandbox(
    GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh", network=sys.argv[1])
)
# Like a Jupyter kernel, the owner survives an interrupt.
signal.signal(signal.SIGINT, lambda *args: None)
root_dir = backend._sandbox_metadata[sandbox_id]["root_dir"]
print(json.dumps({"id": sandbox_id, "root_dir": root_dir}), flush=True)
time.sleep(600)
"""


def _sandbox_processes(sandbox: dict, network: str) -> set:
    """PIDs of runsc's processes for a sandbox and, for a public one, of its
    namespace holder and slirp4netns."""
    pids = {
        int(entry.name)
        for entry in Path("/proc").iterdir()
        if entry.name.isdigit() and sandbox["id"].encode() in _cmdline(entry)
    }
    if network == "public":
        pids |= set(_netns_helpers(sandbox["root_dir"]))
    return pids


def _running(pids) -> set:
    return {pid for pid in pids if (_stat(pid) or ("Z",))[0] != "Z"}


def _wait_until_stopped(pids, timeout: float = 10) -> set:
    deadline = time.monotonic() + timeout
    while _running(pids) and time.monotonic() < deadline:
        time.sleep(0.2)
    return _running(pids)


def _check_owner_death(network: str, whole_group: bool) -> None:
    """Kill the process that owns a sandbox, with its process group as the
    raylet does for a worker, or alone, as a driver can die. Either way the
    sandbox must stop, by then or once a reclaim runs, and the reclaim must
    delete its runsc state, bundle and image pin."""
    backend = GVisorSandboxBackend()
    with _subreaper():
        # A Ray worker leads its own process group.
        owner = subprocess.Popen(
            [sys.executable, "-c", _OWNER, network],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            sandbox = json.loads(owner.stdout.readline() or "null")
            assert sandbox, "the owner failed to create its sandbox"
            pin = Path(
                backend._image_manager.get_image_dir("busybox:latest"),
                ".users",
                sandbox["id"],
            )
            assert pin.exists()
            pids = _sandbox_processes(sandbox, network)
            assert len(pids) >= 2, pids

            if whole_group:
                os.killpg(owner.pid, signal.SIGKILL)
                owner.wait()
                assert not _wait_until_stopped(pids)
            else:
                owner.kill()
                owner.wait()
                time.sleep(2)
                assert _running(pids) == pids
            assert sandbox["id"] in backend._reclaim_unowned_bundles()
            assert not _wait_until_stopped(pids)
            assert not os.path.exists(sandbox["root_dir"])
            assert not pin.exists()
            listed = subprocess.run(
                ["runsc", "--root", "/tmp/runsc", "list", "-quiet"],
                capture_output=True,
                text=True,
            )
            assert sandbox["id"] not in listed.stdout.split()
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(owner.pid, signal.SIGKILL)
            owner.wait()
            backend._reclaim_unowned_bundles()


def test_owner_group_kill_stops_its_sandbox():
    _check_owner_death("none", whole_group=True)


def test_reclaim_stops_a_sandbox_whose_owner_died_alone():
    _check_owner_death("none", whole_group=False)


def test_netns_owner_group_kill_stops_its_sandbox(ensure_slirp4netns):
    _check_owner_death("public", whole_group=True)


def test_netns_reclaim_stops_a_sandbox_whose_owner_died_alone(ensure_slirp4netns):
    _check_owner_death("public", whole_group=False)


def _check_owner_interrupt(network: str) -> None:
    """A Ctrl-C in a driver's terminal, or a Jupyter kernel interrupt, sends
    SIGINT to the owner's whole process group, and the owner survives it. Its
    sandbox must keep running, network and all, and still stop when the group
    gets SIGTERM, which is what the raylet sends a dead worker's group first."""
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(image="busybox:latest", network=network)
    with _subreaper():
        owner = subprocess.Popen(
            [sys.executable, "-c", _OWNER, network],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            sandbox = json.loads(owner.stdout.readline() or "null")
            assert sandbox, "the owner failed to create its sandbox"
            pids = _sandbox_processes(sandbox, network)

            os.killpg(owner.pid, signal.SIGINT)
            time.sleep(2)
            assert owner.poll() is None
            assert _running(pids) == pids
            command = "echo Example"
            if network == "public":
                command = "wget -q -T 15 -O - http://example.com"
            res = subprocess.run(
                [*backend._runsc_base_args(config), "exec", sandbox["id"]]
                + ["sh", "-c", command],
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert "Example" in res.stdout, res.stderr

            os.killpg(owner.pid, signal.SIGTERM)
            owner.wait()
            assert not _wait_until_stopped(pids)
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(owner.pid, signal.SIGKILL)
            owner.wait()
            backend._reclaim_unowned_bundles()


def test_owner_interrupt_leaves_its_sandbox_running():
    _check_owner_interrupt("none")


def test_netns_owner_interrupt_leaves_its_sandbox_running(ensure_slirp4netns):
    _check_owner_interrupt("public")


_UDP_CLIENT = r"""
import socket, struct, sys, time

name, sport, resolver = sys.argv[1], int(sys.argv[2]), sys.argv[3]
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", sport))
s.settimeout(0.5)


def query(qname):
    labels = b"".join(bytes([len(p)]) + p.encode() for p in qname.split("."))
    header = struct.pack("!HHHHHH", 0x1234, 0x0100, 1, 0, 0, 0)
    return header + labels + b"\x00" + struct.pack("!HH", 1, 1)


def qname_of(resp):
    off, parts = 12, []
    while resp[off]:
        n = resp[off]
        parts.append(resp[off + 1 : off + 1 + n].decode())
        off += 1 + n
    return ".".join(parts)


own = foreign = 0
deadline = time.time() + 8
i = 0
while time.time() < deadline:
    s.sendto(query(f"{name}{i}.example.com"), (resolver, 53))
    i += 1
    try:
        while True:
            data, _ = s.recvfrom(4096)
            if qname_of(data).startswith(name):
                own += 1
            else:
                foreign += 1
    except socket.timeout:
        pass
print(f"own={own} foreign={foreign}")
"""


def test_netns_udp_flows_do_not_cross(ensure_slirp4netns):
    """Two sandboxes sending UDP from the same source port to the same server
    must never receive each other's replies (pasta did: it preserved the
    source port on the host with SO_REUSEADDR, so the kernel handed one
    sandbox's replies to the other)."""
    backend = GVisorSandboxBackend()
    cfg = GVisorSandboxConfig(image="python:3.10-slim", network="public")
    sandboxes = [backend.create_sandbox(cfg) for _ in range(2)]
    try:
        for sb in sandboxes:
            backend.write_file(sb, "/tmp/client.py", _UDP_CLIENT)
        names = ("aa", "bb")
        results = {}

        def run(sb, name):
            try:
                results[name] = backend.exec_command(
                    sb, f"python3 /tmp/client.py {name} 5000 8.8.8.8", timeout=60
                )
            except Exception as exc:  # surfaced in the main thread below
                results[name] = exc

        threads = [
            threading.Thread(target=run, args=(sb, name))
            for sb, name in zip(sandboxes, names)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert set(results) == set(names), results
        for name in names:
            res = results[name]
            if isinstance(res, Exception):
                raise res
            assert res.exit_code == 0, res.stderr
            counts = dict(kv.split("=") for kv in res.stdout.split())
            assert counts["foreign"] == "0", (name, res.stdout)
            assert int(counts["own"]) > 0, (name, res.stdout)
    finally:
        for sb in sandboxes:
            backend.delete_sandbox(sb)


def test_netns_create_failure_leaves_no_slirp4netns(ensure_slirp4netns):
    """A failed create (bad image) leaves no slirp4netns process behind."""
    before = _slirp4netns_pids()
    backend = GVisorSandboxBackend()
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(
            GVisorSandboxConfig(
                image="nonexistent_invalid_image_12345:latest", network="public"
            )
        )
    assert _slirp4netns_pids() == before


def test_exec_as_named_user_resolves_inside_the_sandbox():
    """A user name resolves against the running sandbox's own passwd, so a
    user the image ships and one added after boot both work and agree with
    their numeric ids."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh", readonly=False)
    )
    try:
        by_name = backend.exec_command(sb, "id -u", user="nobody", timeout=30)
        assert by_name.exit_code == 0, by_name.stderr
        by_id = backend.exec_command(sb, "id -u", user="65534", timeout=30)
        assert by_name.stdout.strip() == by_id.stdout.strip() == "65534"

        res = backend.exec_command(sb, "adduser -D -u 4321 alice", timeout=30)
        assert res.exit_code == 0, res.stderr
        res = backend.exec_command(sb, "id -u && id -g", user="alice", timeout=30)
        assert res.exit_code == 0, res.stderr
        assert res.stdout.split() == ["4321", "4321"]

        with pytest.raises(SandboxExecError, match="nosuch"):
            backend.exec_command(sb, "true", user="nosuch", timeout=30)
    finally:
        backend.delete_sandbox(sb)


def test_resolve_exec_user(monkeypatch):
    """Numeric users pass through untouched; names resolve via the passwd and
    group files read from inside the sandbox, so no host copy of the root
    filesystem is involved."""
    backend = GVisorSandboxBackend()
    files = {
        "/etc/passwd": (
            b"root:x:0:0:root:/root:/bin/bash\n"
            b"postfix:x:102:104::/var/spool/postfix:/usr/sbin/nologin\n"
            b"short:x:7\n"
        ),
        "/etc/group": b"mail:x:8:\n",
    }
    reads = []

    def fake_read_file(sandbox_id, path):
        assert sandbox_id == "sb-1"
        reads.append(path)
        return files[path]

    monkeypatch.setattr(backend, "read_file", fake_read_file)

    assert backend._resolve_exec_user("sb-1", "1000") == "1000"
    assert backend._resolve_exec_user("sb-1", "1000:1000") == "1000:1000"
    assert reads == []  # numeric ids never touch the sandbox
    assert backend._resolve_exec_user("sb-1", "postfix") == "102:104"
    assert reads == ["/etc/passwd"]  # the group file is read only for a group name
    assert backend._resolve_exec_user("sb-1", "postfix:8") == "102:8"
    assert backend._resolve_exec_user("sb-1", "postfix:mail") == "102:8"
    assert backend._resolve_exec_user("sb-1", "1000:mail") == "1000:8"
    # A truncated passwd line has no login group: uid only.
    assert backend._resolve_exec_user("sb-1", "short") == "7"
    with pytest.raises(SandboxExecError, match="'nosuch' not found"):
        backend._resolve_exec_user("sb-1", "nosuch")
    with pytest.raises(SandboxExecError, match="'nosuch' not found"):
        backend._resolve_exec_user("sb-1", "postfix:nosuch")

    # An account file the sandbox cannot serve is an exec error, not a crash.
    def unreadable(sandbox_id, path):
        raise SandboxError("cat: not found")

    monkeypatch.setattr(backend, "read_file", unreadable)
    with pytest.raises(SandboxExecError, match="cannot read /etc/passwd"):
        backend._resolve_exec_user("sb-1", "postfix")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
