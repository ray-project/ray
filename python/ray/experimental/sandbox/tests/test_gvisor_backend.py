import os
import sys

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
    sb = create("busybox:latest", workdir="/workspace")
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
        workdir="/workspace",
        readonly=False,
    )
    cfg2 = GVisorSandboxConfig(
        image="busybox:latest",
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
    cfg_default = GVisorSandboxConfig(image="busybox:latest")
    orig_env = os.environ.pop("RAY_SANDBOX_IGNORE_CGROUPS", None)
    try:
        args_default = backend._runsc_base_args(cfg_default)
        assert "--ignore-cgroups" not in args_default

        cfg_ignored = GVisorSandboxConfig(image="busybox:latest", _ignore_cgroups=True)
        args_ignored = backend._runsc_base_args(cfg_ignored)
        assert "--ignore-cgroups" in args_ignored
    finally:
        if orig_env is not None:
            os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = orig_env


def test_string_exec_shell_detection():
    """String commands use the detected shell (bash if present, else sh)."""
    runtime = SandboxRuntime()
    # busybox has /bin/sh but no /bin/bash.
    instance_id = runtime.create(image="busybox:latest", readonly=False)
    try:
        meta = runtime.backend._sandbox_metadata[instance_id]
        assert meta["shell"] == "/bin/sh"
        result = runtime.exec(instance_id, "echo hello-$0")
        assert result.exit_code == 0
        assert "hello-" in result.stdout
    finally:
        runtime.delete(instance_id)

    # An explicit config shell wins over detection.
    instance_id = runtime.create(
        image="busybox:latest", readonly=False, shell="/bin/sh"
    )
    try:
        meta = runtime.backend._sandbox_metadata[instance_id]
        assert meta["shell"] == "/bin/sh"
    finally:
        runtime.delete(instance_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
