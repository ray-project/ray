from ray.sandbox.backend.base import SandboxStatus
from ray.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.sandbox.config import GVisorSandboxConfig


def test_gvisor_backend_local_lifecycle_and_file_ops():
    # Use runsc_path_override="/bin/sh" to simulate process execution without requiring runsc binary on host
    backend = GVisorSandboxBackend(runsc_path_override="/bin/sh")
    config = GVisorSandboxConfig(
        work_dir="/workspace",
        runsc_path="/bin/sh",
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sb-gvisor-")
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


def test_gvisor_factory_registration():
    from ray.sandbox.backend.factory import SandboxBackendFactory

    backend = SandboxBackendFactory.get_backend("gvisor")
    assert isinstance(backend, GVisorSandboxBackend)
