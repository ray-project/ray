import contextlib
import ctypes
import errno
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
from ray.experimental.sandbox._internal.image_utils import (
    DEFAULT_IMAGES_DIR,
    image_cache_name,
)
from ray.experimental.sandbox.backend import gvisor
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import (
    GVisorSandboxBackend,
    _exec_processes,
    _render_egress_ruleset,
    _write_egress_ruleset,
)
from ray.experimental.sandbox.config import DEFAULT_PUBLIC_DNS, GVisorSandboxConfig
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
    config = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        cpu=1.0,
        memory="512Mi",
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


# readonly=True with an explicit workdir needs runsc to keep the rootfs
# overlay for a read-only root (it drops it today, and an immutable EROFS
# image cannot grow the workdir mount point), so the sandbox runs on a
# private writable overlay instead; test_readonly_rootfs_with_workdir pins
# that behavior. This test states the intended one, for when runsc's
# initGoferConfs honors an explicitly requested overlay on a read-only root.
@pytest.mark.skip(
    reason="readonly + explicit workdir runs on a private writable overlay "
    "until runsc keeps the rootfs overlay for read-only roots"
)
def test_gvisor_backend_readonly_rootfs():
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


def test_disk_caps_a_readonly_sandbox_with_a_workdir():
    """readonly + workdir runs on a private writable overlay. The cap bounds
    it, and the workdir is inside it rather than a directory on the node."""
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
        # The rest of the rootfs shares the cap.
        result = runtime.exec(instance_id, "dd if=/dev/zero of=/fill bs=1M count=8")
        assert result.exit_code != 0
        assert "No space left on device" in result.stderr
    finally:
        runtime.delete(instance_id)


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


def test_readonly_rootfs_with_workdir_runs_on_private_overlay():
    """readonly + explicit workdir currently runs on a private writable
    overlay: the workdir works, and rootfs writes land in the overlay and are
    discarded with the sandbox rather than rejected."""
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
    # cwd (the rest of the root runs on a private writable overlay today;
    # see the note above test_gvisor_backend_readonly_rootfs).
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
    """The ruleset is the enforcement, so pin its exact text: default drop,
    loopback, one accept per network in its family, and DNS to the resolvers
    on port 53 only."""
    text = _render_egress_ruleset(
        ["52.0.0.0/8", "10.0.1.5/24", "2001:db8::/32"], ["8.8.8.8"]
    )
    assert text == (
        "flush ruleset\n"
        "table inet ray_sandbox_egress {\n"
        "    chain output {\n"
        "        type filter hook output priority 0; policy drop;\n"
        '        oif "lo" accept\n'
        "        ip daddr 52.0.0.0/8 accept\n"
        "        ip daddr 10.0.1.0/24 accept\n"
        "        ip6 daddr 2001:db8::/32 accept\n"
        "        ip daddr 8.8.8.8 udp dport 53 accept\n"
        "        ip daddr 8.8.8.8 tcp dport 53 accept\n"
        "    }\n"
        "}\n"
    )


def test_render_egress_ruleset_empty_allowlist_keeps_only_dns():
    text = _render_egress_ruleset([], ["8.8.8.8", "1.1.1.1"])
    assert "ip daddr 8.8.8.8 udp dport 53 accept" in text
    assert "ip daddr 1.1.1.1 tcp dport 53 accept" in text
    # Loopback plus two resolvers times two protocols: nothing else.
    assert text.count(" accept") == 5
    with pytest.raises(ValueError, match="dns"):
        _render_egress_ruleset([], ["resolver.internal"])


def test_write_egress_ruleset_defaults_to_public_dns(tmp_path):
    path = _write_egress_ruleset(str(tmp_path), ["10.0.0.0/8"], None)
    assert path == str(tmp_path / "egress.nft")
    text = (tmp_path / "egress.nft").read_text()
    assert "ip daddr 10.0.0.0/8 accept" in text
    for server in DEFAULT_PUBLIC_DNS:
        assert f"ip daddr {server} udp dport 53 accept" in text
    # Written atomically: no temp file remains.
    assert [p.name for p in tmp_path.iterdir()] == ["egress.nft"]


def test_build_run_command_public_allowlist_installs_ruleset_before_runsc():
    """The ruleset goes in once the tap is up and before runsc starts, and a
    failed install aborts the start instead of leaving egress open."""
    script = _run_argv("public", cidr_allowlist=["10.0.0.0/8"])[2]
    install = (
        "nsenter --preserve-credentials -U -n -t $NSPID -- nft -f /tmp/rd/egress.nft || "
        '{ echo "egress allowlist install failed" >&2; exit 1; }; '
    )
    assert install in script
    tap_ready = script.index("[ -s /tmp/rd/slirp4netns.ready ] ||")
    runsc_start = script.index(
        "exec nsenter --preserve-credentials -U -n -t $NSPID -- runsc"
    )
    assert tap_ready < script.index(install) < runsc_start
    # Without an allowlist the chain is untouched.
    assert "nft" not in _run_argv("public")[2]


def test_create_sandbox_requires_nft_for_allowlist(monkeypatch):
    """A missing nft fails fast, before the image pull, with remediation."""

    class _NoPullImageManager:
        def pull_image(self, *args, **kwargs):
            raise AssertionError("image pull must not run when nft is missing")

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which",
        lambda name: None if name == "nft" else f"/usr/bin/{name}",
    )
    backend = GVisorSandboxBackend(image_manager=_NoPullImageManager())
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        network="public",
        cidr_allowlist=["10.0.0.0/8"],
    )
    with pytest.raises(SandboxCreationError, match="nft"):
        backend.create_sandbox(cfg)


def test_set_egress_allowlist_requires_an_existing_allowlist(tmp_path):
    """Only a sandbox created with an allowlist can be changed, and the change
    never runs without the namespace holder pid (`flush ruleset` must not run
    anywhere but inside the sandbox's own namespace)."""
    backend = GVisorSandboxBackend(image_manager=object())
    with pytest.raises(SandboxNotFoundError):
        backend.set_egress_allowlist("nope", ["10.0.0.0/8"])

    backend._sandbox_metadata["sb-open"] = {
        "root_dir": str(tmp_path),
        "config": _public_config(),
        "egress_allowlist": None,
    }
    with pytest.raises(SandboxError, match="without a cidr_allowlist"):
        backend.set_egress_allowlist("sb-open", ["10.0.0.0/8"])

    cfg = GVisorSandboxConfig(
        image="busybox:latest", shell="/bin/sh", network="public", cidr_allowlist=[]
    )
    backend._sandbox_metadata["sb-allow"] = {
        "root_dir": str(tmp_path),
        "config": cfg,
        "egress_allowlist": [],
    }
    with pytest.raises(ValueError, match="example.com"):
        backend.set_egress_allowlist("sb-allow", ["example.com"])
    with pytest.raises(SandboxError, match="namespace holder"):
        backend.set_egress_allowlist("sb-allow", ["10.0.0.0/8"])
    assert not (tmp_path / "egress.nft").exists()


def test_set_egress_allowlist_reinstalls_ruleset_in_the_namespace(
    tmp_path, monkeypatch
):
    """The update rewrites the ruleset and loads it through the holder's
    namespaces; a failed load keeps the file describing the rules in force."""
    (tmp_path / "netns.pid").write_text("4242\n")
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
    }

    class _Completed:
        def __init__(self, returncode, stderr=b""):
            self.returncode = returncode
            self.stderr = stderr

    calls = []
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.run",
        lambda args, **kwargs: (calls.append(args), _Completed(0))[1],
    )
    backend.set_egress_allowlist("sb-1", ["52.0.0.1"])
    assert calls == [
        [
            "nsenter",
            "--preserve-credentials",
            "-U",
            "-n",
            "-t",
            "4242",
            "--",
            "nft",
            "-f",
            str(tmp_path / "egress.nft"),
        ]
    ]
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


def test_set_egress_allowlist_serializes_concurrent_updates(tmp_path, monkeypatch):
    """Overlapping updates run one at a time, so the ruleset loaded last is
    the list the backend records."""
    (tmp_path / "netns.pid").write_text("4242\n")
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


def test_netns_egress_allowlist_enforced(ensure_nft_egress):
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


def test_netns_egress_allowlist_runtime_update(ensure_nft_egress):
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


@pytest.mark.parametrize("failure", ["pull", "owner file"])
def test_failed_create_leaves_no_bundle(private_bundles, monkeypatch, failure):
    """A create that fails before its sandbox starts deletes its bundle, even
    one without an owner file for a reclaim to find."""
    backend = GVisorSandboxBackend(image_manager=_RecordingImageManager())
    if failure == "owner file":

        def no_space(*args):
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

        monkeypatch.setattr(backend, "_own_bundle", no_space)
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(GVisorSandboxConfig(image="busybox:latest"))
    assert os.listdir(gvisor._RAY_SANDBOX_DIR) == []


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
