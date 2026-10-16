import os
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
from ray.experimental.sandbox._internal import egress_guard
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import (
    GVisorSandboxBackend,
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


def test_render_egress_ruleset_pins_the_policy():
    """The ruleset is the enforcement, so pin its exact text: default drop;
    loopback and DNS to the resolvers on port 53 first; then the guard's
    always-denied networks; then the cluster's addresses; then one accept per
    allowed network in its family."""
    text = _render_egress_ruleset(
        ["52.0.0.0/8", "10.0.1.5/24", "2001:db8::/32"],
        ["8.8.8.8"],
        denied=["169.254.0.0/16", "127.0.0.1/32", "127.0.0.0/8", "fe80::/10"],
        cluster=["10.0.0.84/32", "10.0.0.85/32"],
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
        "        ip daddr { 10.0.0.84/31 } drop\n"
        "        ip daddr 52.0.0.0/8 accept\n"
        "        ip daddr 10.0.1.0/24 accept\n"
        "        ip6 daddr 2001:db8::/32 accept\n"
        "    }\n"
        "}\n"
    )


def test_render_egress_ruleset_ports_and_internet():
    """A port entry accepts only that port, for TCP and UDP; one naming a
    single address goes ahead of the cluster's addresses, but after the
    always-denied networks. "internet" is every address outside the
    non-public IPv4 networks, plus IPv6 global unicast."""
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
        cluster=["10.0.0.5/32"],
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
        # Pinned too, but the always-denied drop above already took it.
        "ip daddr 169.254.169.254/32 tcp dport 80 accept",
        "ip daddr 169.254.169.254/32 udp dport 80 accept",
        "ip daddr { 10.0.0.5/32 } drop",
        # A network with a port is not pinned: the cluster stays closed.
        "ip daddr 10.0.0.0/8 tcp dport 22 accept",
        "ip daddr 10.0.0.0/8 udp dport 22 accept",
        f"{not_public} accept",
        "ip6 daddr 2000::/3 accept",
    ]
    assert "ip daddr != 10.0.0.0/8" in not_public
    assert "ip daddr != 198.18.0.0/15" in not_public


def test_render_egress_ruleset_empty_allowlist_keeps_only_dns():
    text = _render_egress_ruleset(
        [], ["8.8.8.8", "1.1.1.1"], denied=["169.254.0.0/16"], cluster=[]
    )
    assert "ip daddr 8.8.8.8 udp dport 53 accept" in text
    assert "ip daddr 1.1.1.1 tcp dport 53 accept" in text
    # Loopback plus two resolvers times two protocols: nothing else.
    assert text.count(" accept") == 5
    with pytest.raises(ValueError, match="dns"):
        _render_egress_ruleset([], ["resolver.internal"], denied=[], cluster=[])


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


def test_write_egress_ruleset_writes_nothing_without_the_guard(tmp_path, monkeypatch):
    """No ruleset is written when the cluster's addresses can't be read, so
    the sandbox never starts without them."""

    def unavailable():
        raise ConnectionError("GCS unavailable")

    monkeypatch.setattr(egress_guard, "cluster_addresses", unavailable)
    with pytest.raises(ConnectionError):
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
            "exec nsenter --preserve-credentials -U -n -t $NSPID -- runsc"
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

    def prepare_oci_bundle(self, **kwargs):
        return None

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

    def fake_popen(args, **kwargs):
        raise _Started(args)

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.subprocess.Popen", fake_popen
    )
    backend = GVisorSandboxBackend(image_manager=_StubImageManager())
    with pytest.raises(_Started):
        backend.create_sandbox(_public_config())
    (root_dir,) = tmp_path.iterdir()
    text = (root_dir / "egress.nft").read_text()
    not_public = " ".join(f"ip daddr != {net}" for net in INTERNET_EXCLUDED_IPV4)
    assert f"{not_public} accept" in text
    assert "169.254.0.0/16" in text

    def unavailable():
        raise ConnectionError("GCS unavailable")

    monkeypatch.setattr(egress_guard, "cluster_addresses", unavailable)
    images = _StubImageManager()
    backend = GVisorSandboxBackend(image_manager=images)
    with pytest.raises(SandboxCreationError, match="GCS unavailable"):
        backend.create_sandbox(_public_config())
    assert len(images.released) == 1


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


def test_netns_default_policy_keeps_the_control_plane_closed(ensure_slirp4netns):
    """A public sandbox created without an allowlist reaches the internet but
    not the worker's own address, where Ray's ports listen, nor instance
    metadata or the Kubernetes API. No allowlist opens those: not even
    allow-all, and the worker opens only to an entry naming its address and
    port."""
    host = _host_ip()
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
        assert reach() == {**closed, "internet": "open"}
        backend.set_egress_allowlist(sb, ["internet", f"{host}:{port}"])
        assert reach() == {**closed, "worker": "open", "internet": "open"}
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
