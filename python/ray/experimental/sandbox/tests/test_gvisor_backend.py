import os
import socket
import sys
import threading
from pathlib import Path

import pytest

import ray
from ray.actor import ActorHandle
from ray.experimental.sandbox import create
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
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
        # Only the extracted rootfs is cached; no archive doubles its footprint.
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

        # Base image rootfs must not contain /overlay_test.txt
        extracted_dir = "/tmp/ray/sandbox/images/busybox_latest"
        assert not os.path.exists(os.path.join(extracted_dir, "overlay_test.txt"))
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


def _erofs_layout_active() -> bool:
    from ray.experimental.sandbox._internal.image_utils import rootfs_layout

    return rootfs_layout() == "erofs"


# readonly=True with an explicit workdir needs runsc to keep the rootfs
# overlay for a read-only root (it drops it today, and an immutable EROFS
# image cannot grow the workdir mount point), so on EROFS the sandbox runs on
# a private writable overlay instead; test_readonly_rootfs_erofs_fallback
# covers that behavior.
@pytest.mark.skipif(
    os.environ.get("TEST_SANDBOX") == "1" and _erofs_layout_active(),
    reason="EROFS rootfs: readonly + explicit workdir falls back to a writable overlay",
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


def test_readonly_rootfs_erofs_fallback(ensure_mkfs_erofs):
    """On EROFS, readonly + explicit workdir currently runs on a private
    writable overlay: the workdir works, and rootfs writes are discarded with
    the sandbox rather than rejected."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(
        GVisorSandboxConfig(
            image="busybox:latest", shell="/bin/sh", workdir="/workspace"
        )
    )
    try:
        res = backend.exec_command(
            sb, "echo ws_ok > /workspace/ws.txt && cat /workspace/ws.txt", timeout=30
        )
        assert res.exit_code == 0, res.stderr
        assert "ws_ok" in res.stdout
    finally:
        backend.delete_sandbox(sb)


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

    # readonly=True, explicit workdir: it is the only writable path (on an
    # EROFS rootfs the root falls back to a writable overlay; see
    # the note above test_gvisor_backend_readonly_rootfs).
    instance_id = runtime.create(
        image="busybox:latest", workdir="/data", shell="/bin/sh"
    )
    try:
        assert runtime.exec(instance_id, "touch /data/probe").exit_code == 0
        if not _erofs_layout_active():
            assert runtime.exec(instance_id, "touch /etc/probe").exit_code != 0
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
    return backend._build_run_command(cfg, "/tmp/rd", "/tmp/rd/overlay", "sb-1")


def test_build_run_command_erofs_skips_overlay_flag():
    """An EROFS rootfs gets its overlay from the bundle annotations."""
    backend = GVisorSandboxBackend()
    cfg = GVisorSandboxConfig(image="busybox:latest", network="none")
    cmd = backend._build_run_command(
        cfg, "/tmp/rd", "/tmp/rd/overlay", "sb-1", erofs=True
    )
    assert not any(a.startswith("--overlay2") for a in cmd)
    assert cmd[-4:] == ["run", "--bundle", "/tmp/rd", "sb-1"]
    assert any(a.startswith("--overlay2") for a in _run_argv("none"))


def _owned_busybox_tar(tar_path: str) -> None:
    """A busybox-based local image tar with baked non-root ownership, modeled
    on the mailman image (0700 uid=101 spool, 02710 setgid dir, a setuid tool)."""
    import io
    import tarfile

    from ray.experimental.sandbox._internal import image_utils
    from ray.experimental.sandbox.image_manager import ImageManager

    # Extract busybox as a plain directory to re-pack it.
    layout = os.environ.get("RAY_SANDBOX_ROOTFS")
    os.environ["RAY_SANDBOX_ROOTFS"] = "dir"
    try:
        busybox_rootfs = os.path.join(
            ImageManager(images_dir="/tmp/ray/sandbox/images-dir").pull_image(
                "busybox:latest"
            ),
            "rootfs",
        )
    finally:
        if layout is None:
            os.environ.pop("RAY_SANDBOX_ROOTFS", None)
        else:
            os.environ["RAY_SANDBOX_ROOTFS"] = layout
    del image_utils

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


@pytest.fixture
def ensure_mkfs_erofs():
    from ray.experimental.sandbox._internal.image_utils import mkfs_erofs_path

    if os.environ.get("RAY_SANDBOX_ROOTFS") == "dir" or mkfs_erofs_path() is None:
        pytest.skip("mkfs.erofs with --tar support is not available")


def test_erofs_baked_ownership_and_chown(ensure_mkfs_erofs, tmp_path):
    """On an EROFS rootfs the image's owners survive without any host id
    mapping, root traverses another user's 0700 directory, a named user reads
    only its own files, setuid works, and chown to arbitrary uids succeeds."""
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
        assert backend._image_manager.get_rootfs_image(tar_path) is not None
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


def test_erofs_readonly_rootfs(ensure_mkfs_erofs):
    """readonly=True keeps / read-only on an EROFS rootfs while /tmp stays
    writable, and the sandbox still boots (mountpoints come from the overlay)."""
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
        "unshare --user --map-root-user --net --fork --kill-child "
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
        "exec nsenter --preserve-credentials -U -n -t $NSPID -- runsc",
        "--network host",
        "--overlay2=root:dir=/tmp/rd/overlay",
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
def test_build_run_command_other_modes_unwrapped(network, rootless):
    """Every mode but "public" keeps today's bare runsc invocation."""
    cmd = _run_argv(network, rootless=rootless)
    assert cmd[0] == "runsc"
    assert "slirp4netns" not in cmd
    assert cmd[cmd.index("--network") + 1] == network
    assert cmd[-4:] == ["run", "--bundle", "/tmp/rd", "sb-1"]


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
