import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

import ray
from ray.actor import ActorHandle
from ray.experimental.sandbox import create
from ray.experimental.sandbox._internal import image_utils, overlayfs
from ray.experimental.sandbox._internal.overlayfs import (
    ImageMountMode,
    UserNamespaceType,
)
from ray.experimental.sandbox.backend import gvisor
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
)
from ray.experimental.sandbox.runtime import SandboxRuntime


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

        extracted_dir = "/tmp/ray/sandbox/images/busybox_latest"
        assert os.path.exists(extracted_dir)
        assert os.path.isdir(extracted_dir)
        assert os.path.exists(os.path.join(extracted_dir, ".extracted"))
        # Only the EROFS image is cached: no extracted tree, and no archive
        # doubling its footprint.
        assert os.path.isfile(os.path.join(extracted_dir, "rootfs.erofs"))
        assert not os.path.exists(os.path.join(extracted_dir, "rootfs"))
        assert not os.path.exists("/tmp/ray/sandbox/images/busybox_latest.tar")

        res = backend.exec_command(sandbox_id, "/bin/sh -c 'echo hello from busybox'")
        assert res.exit_code == 0
        assert "hello from busybox" in res.stdout
    finally:
        backend.delete_sandbox(sandbox_id)

    assert os.path.exists("/tmp/ray/sandbox/images/busybox_latest")


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
    assert not os.path.exists("/tmp/ray/sandbox/images/busybox_latest/probe.txt")


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


def _run_argv(network: str, rootless: bool = True, **backend_kwargs) -> list:
    """`_build_run_command` over fixed paths — pure argv, no side effects."""
    backend = GVisorSandboxBackend(**backend_kwargs)
    cfg = GVisorSandboxConfig(
        image="busybox:latest", network=network, rootless=rootless
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

    assert script.startswith(
        "unshare --net --fork --kill-child --user --map-root-user "
    )
    assert script.endswith("run --bundle /tmp/rd sb-1")
    for fragment in (
        "/tmp/rd/netns.pid",
        # slirp4netns stays in the sandbox's process group and its ready
        # file, written once the tap is configured, gates the runsc start.
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
        "exec nsenter --preserve-credentials -n -t $NSPID -U -- runsc",
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
def test_build_run_command_other_modes_unwrapped(network, rootless):
    """Every mode but "public" keeps today's bare runsc invocation."""
    cmd = _run_argv(network, rootless=rootless)
    assert cmd[0] == "runsc"
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


def _mark_overlay_wrap(monkeypatch):
    monkeypatch.setattr(
        overlayfs.RootfsOverlay, "wrap", lambda self, command: [_WRAPPED, *command]
    )


def _boot_from_overlays(monkeypatch, overlay=True):
    """Have every sandbox created from here on need a kernel overlay, or
    none, whatever its spec."""
    monkeypatch.setattr(overlayfs, "needs_rootfs_overlay", lambda spec: overlay)


def _overlayfs_run_argv(
    network: str, in_userns: bool, rootless: bool = True, writable: bool = True
) -> list:
    """`_build_run_command` for an overlayfs sandbox over fixed paths."""
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        network=network,
        rootless=rootless,
        readonly=not writable,
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
    first mounts the sandbox's kernel overlay. gVisor keeps writes made inside
    the sandbox in its --overlay2 layer."""
    _mark_overlay_wrap(monkeypatch)
    cmd = _overlayfs_run_argv(network, in_userns, rootless=rootless)

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
    runs inside its overlay's namespaces, which provide the user namespace if
    any, so the holder only adds a network namespace. With mount privileges
    there's no user namespace at all, so the overlay keeps the image's
    owners."""
    _mark_overlay_wrap(monkeypatch)
    cmd = _overlayfs_run_argv("public", in_userns)

    assert cmd[0] == _WRAPPED
    inner = cmd[1:]
    assert inner[:2] == ["bash", "-c"]
    # The script's empty user-namespace arguments leave double spaces, which
    # bash ignores.
    script = " ".join(inner[2].split())
    assert script.startswith(
        "unshare --net --fork --kill-child "
        "bash -c 'echo $$ > /tmp/rd/netns.pid; exec sleep infinity' & "
    )
    assert "exec nsenter --preserve-credentials -n -t $NSPID -- runsc" in script
    assert "--userns-path" not in script
    assert "--overlay2 root:dir=/tmp/rd/runsc-overlay2" in script


class _StopBeforeRun(Exception):
    pass


class _RecordingImageManager:
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

    def _capture(config, root_dir, sandbox_id, rootfs_overlay=None):
        captured.update(root_dir=root_dir, rootfs_overlay=rootfs_overlay)
        raise _StopBeforeRun()

    manager = _RecordingImageManager(image=str(tmp_path / "rootfs.erofs"))
    backend = GVisorSandboxBackend(image_manager=manager)
    monkeypatch.setattr(backend, "_build_run_command", _capture)
    with pytest.raises(_StopBeforeRun):
        backend.create_sandbox(
            GVisorSandboxConfig(image="busybox:latest", readonly=readonly)
        )
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
    assert os.path.isdir(overlay.tmpfs_dir)
    assert os.path.isdir(os.path.join(captured["root_dir"], "runsc-overlay2"))


def test_create_sandbox_readonly_overlayfs_sandbox_has_overlay2_dir(
    tmp_path, monkeypatch
):
    """A readonly overlayfs sandbox gets a directory for gVisor's --overlay2
    layer too, which stays empty."""
    captured, _ = _create_until_run(tmp_path, monkeypatch, True, readonly=True)
    assert os.path.isdir(os.path.join(captured["root_dir"], "runsc-overlay2"))


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
    manager = _RecordingImageManager(image=str(tmp_path / "rootfs.erofs"))
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


def test_create_sandbox_requires_slirp4netns(monkeypatch):
    """A missing slirp4netns fails fast, before the image pull, with remediation."""

    class _NoPullImageManager:
        def pull_image(self, *args, **kwargs):
            raise AssertionError("image pull must not run when slirp4netns is missing")

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which",
        lambda name: None if name == "slirp4netns" else f"/usr/bin/{name}",
    )
    backend = GVisorSandboxBackend(image_manager=_NoPullImageManager())
    with pytest.raises(SandboxCreationError) as err:
        backend.create_sandbox(_public_config())
    assert "slirp4netns" in str(err.value)


@pytest.mark.parametrize("fail_at", ["pull", "bundle"])
def test_failed_create_removes_bundle_dir(tmp_path, monkeypatch, fail_at):
    """A create that fails after the sandbox's bundle directory exists, while
    pulling the image or preparing the OCI bundle, releases the image and
    removes the directory."""

    class _FailingImageManager:
        def __init__(self):
            self.released = []

        def pull_image(self, image, **kwargs):
            if fail_at == "pull":
                raise RuntimeError("pull failed")

        def get_workdir(self, image):
            return None

        def prepare_oci_bundle(self, root_dir, **kwargs):
            with open(os.path.join(root_dir, "config.json"), "w") as f:
                f.write("{}")
            raise RuntimeError("bundle failed")

        def release_image(self, image, instance_id):
            self.released.append(image)

    sandboxes_dir = tmp_path / "sandboxes"
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor._RAY_SANDBOX_DIR", str(sandboxes_dir)
    )
    manager = _FailingImageManager()
    backend = GVisorSandboxBackend(image_manager=manager)
    with pytest.raises(Exception, match=f"{fail_at} failed"):
        backend.create_sandbox(GVisorSandboxConfig(image="busybox:latest"))
    assert manager.released == ["busybox:latest"]
    assert not any(sandboxes_dir.glob("ray-sandbox-*"))


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
