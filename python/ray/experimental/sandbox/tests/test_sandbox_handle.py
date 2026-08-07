from unittest.mock import MagicMock

import pytest

import ray
from ray.experimental.sandbox import Sandbox, SandboxHandle
from ray.experimental.sandbox.backend.base import ExecResult
from ray.experimental.sandbox.runtime import SandboxRuntime


def test_sandbox_runtime_interface(tmp_path):
    rt = SandboxRuntime()
    instance_id = rt.create(image="busybox:latest", workdir="/workspace")
    assert instance_id.startswith("ray-sandbox-")

    res = rt.exec(instance_id, "echo 'Hello world'")
    assert res.exit_code == 0
    assert "Hello world" in res.stdout
    assert res.duration_ms >= 0

    # Test upload_file
    local_up = tmp_path / "rt_up.txt"
    local_up.write_bytes(b"rt upload bytes")
    rt.upload_file(instance_id, str(local_up), "/workspace/rt_up.txt")

    # Test download_file
    local_down = tmp_path / "rt_down.txt"
    rt.download_file(instance_id, "/workspace/rt_up.txt", str(local_down))
    assert local_down.read_bytes() == b"rt upload bytes"

    rt.delete(instance_id)


def test_sandbox_actor_wrapper():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.remote(image="busybox:latest", workdir="/workspace")
    instance_id = ray.get(actor.get_instance_id.remote())
    assert instance_id.startswith("ray-sandbox-")

    res = ray.get(actor.exec.remote("echo hi"))
    assert res.exit_code == 0
    assert "hi" in res.stdout

    ray.get(actor.write_file.remote("/workspace/test.txt", "actor content"))
    data = ray.get(actor.read_file.remote("/workspace/test.txt"))
    assert data == b"actor content"

    ray.get(actor.delete.remote())
    ray.kill(actor)


def test_sandbox_handle(tmp_path):
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    mock_actor = MagicMock()
    mock_actor.get_instance_id.remote.return_value = ray.put("sb-handle-1")
    mock_actor.exec.remote.return_value = ray.put(
        ExecResult(
            exit_code=0, stdout="handle output\n", stderr="", duration_seconds=0.1
        )
    )
    mock_actor.read_file.remote.return_value = ray.put(b"remote content")
    mock_actor.write_file.remote.return_value = ray.put(None)
    mock_actor.upload_file.remote.return_value = ray.put(None)
    mock_actor.download_file.remote.return_value = ray.put(None)
    mock_actor.delete.remote.return_value = ray.put(None)

    handle = SandboxHandle(actor_handle=mock_actor)

    assert handle.instance_id == "sb-handle-1"
    assert handle.sandbox_id == "sb-handle-1"
    res = handle.exec("echo handle output")
    assert res.exit_code == 0
    assert res.stdout == "handle output\n"
    assert res.duration_ms == 100.0

    # Test upload_file
    local_file = tmp_path / "upload.txt"
    local_file.write_bytes(b"local upload content")
    handle.upload_file(str(local_file), "/workspace/remote_upload.txt")
    mock_actor.upload_file.remote.assert_called_with(
        str(local_file), "/workspace/remote_upload.txt"
    )

    # Test download_file
    download_target = tmp_path / "download.txt"
    handle.download_file("/workspace/remote_file.txt", str(download_target))
    mock_actor.download_file.remote.assert_called_with(
        "/workspace/remote_file.txt", str(download_target)
    )

    handle.terminate()
    mock_actor.delete.remote.assert_called_once()


def test_custom_sandbox_actor_with_sandbox_runtime():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    @ray.remote
    class CustomSandbox:
        def __init__(self):
            self.runtime = SandboxRuntime()
            self.instance_id = self.runtime.create(
                image="busybox:latest", workdir="/workspace"
            )

        def exec(self, command, timeout=None, env=None):
            return self.runtime.exec(
                self.instance_id, command, timeout=timeout, env=env
            )

        def delete(self):
            self.runtime.delete(self.instance_id)

    custom_actor = CustomSandbox.remote()
    res = ray.get(custom_actor.exec.remote("echo 'Hello from Custom Sandbox'"))
    assert res.exit_code == 0
    assert "Hello from Custom Sandbox" in res.stdout

    ray.get(custom_actor.delete.remote())
    ray.kill(custom_actor)


def test_ray_remote_sandbox_runtime():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    remote_rt_cls = ray.remote(SandboxRuntime)
    rt_actor = remote_rt_cls.remote()

    instance_id = ray.get(
        rt_actor.create.remote(image="busybox:latest", workdir="/workspace")
    )
    assert instance_id.startswith("ray-sandbox-")

    res = ray.get(rt_actor.exec.remote(instance_id, "echo 'Hello remote runtime'"))
    assert res.exit_code == 0
    assert "Hello remote runtime" in res.stdout

    ray.get(rt_actor.delete.remote(instance_id))
    ray.kill(rt_actor)


def test_sandbox_actor_resource_translation():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.options(num_cpus=2.0).remote(
        image="busybox:latest", cpu=2.0, workdir="/workspace"
    )

    ret_config = ray.get(actor.get_config.remote())
    assert ret_config.cpu == 2.0

    ray.get(actor.delete.remote())
    ray.kill(actor)


def test_sandbox_runtime_create_variants():
    rt = SandboxRuntime()

    # Pass image as positional arg
    id1 = rt.create("busybox:latest", workdir="/workspace")
    assert id1.startswith("ray-sandbox-")
    rt.delete(id1)

    # Pass image as keyword arg
    id2 = rt.create(image="busybox:latest", workdir="/workspace")
    assert id2.startswith("ray-sandbox-")
    rt.delete(id2)

    # Missing image should raise TypeError
    with pytest.raises(TypeError):
        rt.create(workdir="/workspace", cpu=1.0)
