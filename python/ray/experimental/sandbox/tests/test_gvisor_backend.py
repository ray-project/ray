import os

import pytest

from ray.experimental.sandbox import SandboxHandle, create
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxNotFoundError,
)


def test_gvisor_backend_local_lifecycle_and_file_ops():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        work_dir="/workspace",
        cpu=1.0,
        memory="512Mi",
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sb-gvisor-")
    assert sandbox_id in backend._sandbox_meta
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
    assert sandbox_id not in backend._sandbox_meta


def test_gvisor_backend_not_found():
    backend = GVisorSandboxBackend()
    with pytest.raises(SandboxNotFoundError):
        backend.exec_command("nonexistent-id", "echo 'hi'")


def test_create_sandbox_helper():
    sb = create(work_dir="/workspace")
    assert isinstance(sb, SandboxHandle)
    res = sb.exec("echo 'Process isolation'")
    assert res.exit_code == 0
    assert "Process isolation" in res.stdout
    assert res.duration_ms >= 0
    sb.terminate()


def test_gvisor_backend_container_image_support():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="busybox:latest",
        work_dir="/workspace",
    )
    sandbox_id = backend.create_sandbox(config)
    try:
        assert sandbox_id.startswith("ray-sb-gvisor-")
        assert backend.get_status(sandbox_id) == SandboxStatus.RUNNING

        extracted_dir = "/tmp/ray/sandboxes/images/busybox_latest"
        assert os.path.exists(extracted_dir)
        assert os.path.isdir(extracted_dir)
        assert os.path.exists(os.path.join(extracted_dir, ".extracted"))
        assert os.path.exists("/tmp/ray/sandboxes/images/busybox_latest.tar")

        res = backend.exec_command(sandbox_id, "/bin/sh -c 'echo hello from busybox'")
        assert res.exit_code == 0
        assert "hello from busybox" in res.stdout
    finally:
        backend.delete_sandbox(sandbox_id)

    assert os.path.exists("/tmp/ray/sandboxes/images/busybox_latest")


def test_gvisor_backend_image_none():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image=None,
        work_dir="/workspace",
    )
    sandbox_id = backend.create_sandbox(config)
    try:
        res = backend.exec_command(sandbox_id, "echo 'no image test'")
        assert res.exit_code == 0
        assert "no image test" in res.stdout
    finally:
        backend.delete_sandbox(sandbox_id)


def test_gvisor_backend_invalid_image():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="nonexistent_invalid_image_12345:latest",
        work_dir="/workspace",
    )
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(config)
