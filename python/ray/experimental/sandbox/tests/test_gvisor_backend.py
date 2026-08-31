import os
import socket
import sys
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
        assert os.path.exists("/tmp/ray/sandbox/images/busybox_latest.tar")

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

    # readonly=True, explicit workdir: it is the only writable path.
    instance_id = runtime.create(
        image="busybox:latest", workdir="/data", shell="/bin/sh"
    )
    try:
        assert runtime.exec(instance_id, "touch /data/probe").exit_code == 0
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


def test_resolve_exec_user(tmp_path, monkeypatch):
    """Numeric users pass through; names resolve via the image's passwd."""
    backend = GVisorSandboxBackend()
    img_dir = tmp_path / "img"
    (img_dir / "rootfs" / "etc").mkdir(parents=True)
    (img_dir / "rootfs" / "etc" / "passwd").write_text(
        "root:x:0:0:root:/root:/bin/bash\n"
        "postfix:x:102:104::/var/spool/postfix:/usr/sbin/nologin\n"
    )
    monkeypatch.setattr(
        backend._image_manager, "get_image_dir", lambda image: str(img_dir)
    )

    assert backend._resolve_exec_user("1000", "img") == "1000"
    assert backend._resolve_exec_user("1000:1000", "img") == "1000:1000"
    assert backend._resolve_exec_user("postfix", "img") == "102:104"
    with pytest.raises(SandboxExecError):
        backend._resolve_exec_user("nosuch", "img")


_PUBLIC_HOST_NETNS_ENV = "RAY_SANDBOX_PUBLIC_HOST_NETNS"


@pytest.fixture
def no_host_netns(monkeypatch):
    """Default posture: the shared-host-network kill switch is unset."""
    monkeypatch.delenv(_PUBLIC_HOST_NETNS_ENV, raising=False)


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


def _host_ip() -> str:
    """The worker's primary IPv4, which pasta copies onto each sandbox's tap."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]


def _pasta_pids() -> set:
    """PIDs of running pasta processes."""
    pids = set()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv0 = (entry / "cmdline").read_bytes().split(b"\0", 1)[0]
        except OSError:
            continue
        if os.path.basename(argv0) == b"pasta":
            pids.add(entry.name)
    return pids


def test_build_run_command_public_wraps_with_pasta(no_host_netns):
    """The pasta flags are the isolation property and the chain's shape is the
    topology, so pin both: holder namespaces first, pasta attached from the pod
    side, runsc entered as mapped root (never --rootless)."""
    cmd = _run_argv("public")
    assert cmd[:2] == ["bash", "-c"]
    script = cmd[2]

    assert script.startswith(
        "unshare --user --map-root-user --net --fork --kill-child "
    )
    assert script.endswith("run --bundle /tmp/rd sb-1")
    for fragment in (
        "/tmp/rd/netns.pid",
        "pasta --config-net -t none -u none -T none -U none --no-map-gw -4 "
        "--netns /proc/$NSPID/ns/net --userns /proc/$NSPID/ns/user",
        "exec nsenter --preserve-credentials -U -n -t $NSPID -- runsc",
        "--network host",
        "--overlay2=root:dir=/tmp/rd/overlay",
    ):
        assert fragment in script, fragment
    assert "--rootless" not in script


@pytest.mark.parametrize(
    "network,rootless", [("none", True), ("host", True), ("sandbox", False)]
)
def test_build_run_command_other_modes_unwrapped(no_host_netns, network, rootless):
    """Every mode but "public" keeps today's bare runsc invocation."""
    cmd = _run_argv(network, rootless=rootless)
    assert cmd[0] == "runsc"
    assert "pasta" not in cmd
    assert cmd[cmd.index("--network") + 1] == network
    assert cmd[-4:] == ["run", "--bundle", "/tmp/rd", "sb-1"]


def test_public_host_netns_kill_switch(monkeypatch):
    """The env kill switch restores the pre-netns shared-host behavior."""
    monkeypatch.setenv(_PUBLIC_HOST_NETNS_ENV, "1")
    cmd = _run_argv("public")
    assert cmd[0] == "runsc"
    assert "pasta" not in cmd
    assert cmd[cmd.index("--network") + 1] == "host"


def test_create_sandbox_requires_pasta(no_host_netns, monkeypatch):
    """A missing pasta fails fast — before the image pull — with remediation."""

    class _NoPullImageManager:
        def pull_image(self, *args, **kwargs):
            raise AssertionError("image pull must not run when pasta is missing")

    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which",
        lambda name: None if name == "pasta" else f"/usr/bin/{name}",
    )
    backend = GVisorSandboxBackend(image_manager=_NoPullImageManager())
    with pytest.raises(SandboxCreationError) as err:
        backend.create_sandbox(_public_config())
    assert all(
        hint in str(err.value)
        for hint in ("pasta", "auto_install_pasta", _PUBLIC_HOST_NETNS_ENV)
    )


def test_netns_concurrent_same_port_bind_and_isolation(ensure_pasta):
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

        # No address names one sandbox from another: pasta --config-net gives
        # every sandbox the worker's own IP, so from sb2 that IP is sb2 itself.
        res = backend.exec_command(
            sb2, f"wget -q -T 3 -O - http://{host_ip}:2222/token", timeout=30
        )
        assert tokens[sb1] not in res.stdout
        assert res.exit_code != 0 or tokens[sb2] in res.stdout
    finally:
        for sb in (sb1, sb2):
            backend.delete_sandbox(sb)


def test_netns_egress_and_dns(ensure_pasta):
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


def test_netns_teardown_reaps_pasta(ensure_pasta):
    """delete_sandbox ends the pasta process tree and removes all state."""
    backend = GVisorSandboxBackend()
    sb = backend.create_sandbox(_public_config())
    meta = backend._sandbox_metadata[sb]
    assert meta["proc"].poll() is None

    backend.delete_sandbox(sb)
    assert meta["proc"].poll() is not None
    assert not os.path.exists(meta["root_dir"])
    assert sb not in backend._sandbox_metadata


def test_netns_create_failure_leaves_no_pasta(ensure_pasta):
    """A failed create (bad image) leaves no pasta process behind."""
    before = _pasta_pids()
    backend = GVisorSandboxBackend()
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(
            GVisorSandboxConfig(
                image="nonexistent_invalid_image_12345:latest", network="public"
            )
        )
    assert _pasta_pids() == before


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
