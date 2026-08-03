import pytest

from ray.experimental.sandbox import SandboxHandle, create
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig
from ray.experimental.sandbox.exceptions import SandboxNotFoundError


def test_gvisor_backend_local_lifecycle_and_file_ops():
    # Use runsc_path_override="/bin/sh" to simulate process execution without requiring runsc binary on host
    backend = GVisorSandboxBackend(runsc_path_override="/bin/sh")
    config = GVisorSandboxConfig(
        work_dir="/workspace",
        runsc_path="/bin/sh",
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
    backend = GVisorSandboxBackend(runsc_path_override="/bin/sh")
    with pytest.raises(SandboxNotFoundError):
        backend.exec_command("nonexistent-id", "echo 'hi'")


def test_create_sandbox_helper():
    sb = create(work_dir="/workspace", runsc_path_override="/bin/sh")
    assert isinstance(sb, SandboxHandle)
    res = sb.exec("echo 'Process isolation'")
    assert res.exit_code == 0
    assert "Process isolation" in res.stdout
    assert res.duration_ms >= 0
    sb.terminate()
