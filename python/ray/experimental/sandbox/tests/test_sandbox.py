import os
import sys

import pytest

import ray
from ray.experimental.sandbox import Sandbox
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.runtime import SandboxRuntime

_CGROUP_PROVISIONED = bool(os.environ.get("RAY_SANDBOX_CGROUP_PATH"))


def test_sandbox_runtime_interface(tmp_path):
    rt = SandboxRuntime()
    instance_id = rt.create(
        image="busybox:latest", shell="/bin/sh", workdir="/workspace"
    )
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

    actor = Sandbox.remote(
        image="busybox:latest", shell="/bin/sh", workdir="/workspace"
    )
    instance_id = ray.get(actor.get_instance_id.remote())
    assert instance_id.startswith("ray-sandbox-")

    res = ray.get(actor.exec.remote("echo hi"))
    assert res.exit_code == 0
    assert "hi" in res.stdout

    ray.get(actor.write_file.remote("/workspace/test.txt", "actor content"))
    data = ray.get(actor.read_file.remote("/workspace/test.txt"))
    assert data == b"actor content"

    # Test status, pause, and resume
    assert ray.get(actor.get_status.remote()) == SandboxStatus.RUNNING
    ray.get(actor.pause.remote())
    assert ray.get(actor.get_status.remote()) == SandboxStatus.PAUSED
    ray.get(actor.resume.remote())
    assert ray.get(actor.get_status.remote()) == SandboxStatus.RUNNING

    ray.get(actor.delete.remote())
    ray.kill(actor)


def test_netns_sandbox_actor_changes_its_egress_allowlist(ensure_slirp4netns):
    """A sandbox actor, as create() returns, replaces its egress allowlist
    while it runs."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    # ensure_slirp4netns can put slirp4netns on this process's PATH after an
    # earlier test started Ray's workers, so pass the PATH to the actor.
    actor = Sandbox.options(
        runtime_env={"env_vars": {"PATH": os.environ["PATH"]}}
    ).remote(image="python:3.10-slim", network="public", cidr_allowlist=[])
    connect = (
        'python3 -c "import socket; '
        "socket.create_connection(('1.1.1.1', 443), timeout=5).close()\""
    )
    try:
        assert ray.get(actor.exec.remote(connect, timeout=60)).exit_code != 0
        ray.get(actor.set_egress_allowlist.remote(["1.1.1.1:443"]))
        result = ray.get(actor.exec.remote(connect, timeout=60))
        assert result.exit_code == 0, result.stderr
    finally:
        ray.get(actor.delete.remote())
        ray.kill(actor)


def test_custom_sandbox_actor_with_sandbox_runtime():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    @ray.remote
    class CustomSandbox:
        def __init__(self):
            self.runtime = SandboxRuntime()
            self.instance_id = self.runtime.create(
                image="busybox:latest", shell="/bin/sh", workdir="/workspace"
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
        rt_actor.create.remote(
            image="busybox:latest", shell="/bin/sh", workdir="/workspace"
        )
    )
    assert instance_id.startswith("ray-sandbox-")

    res = ray.get(rt_actor.exec.remote(instance_id, "echo 'Hello remote runtime'"))
    assert res.exit_code == 0
    assert "Hello remote runtime" in res.stdout

    ray.get(rt_actor.delete.remote(instance_id))
    ray.kill(rt_actor)


@pytest.mark.skipif(
    not _CGROUP_PROVISIONED, reason="Positive limits need RAY_SANDBOX_CGROUP_PATH"
)
def test_sandbox_actor_resource_translation():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.options(num_cpus=2.0).remote(
        image="busybox:latest", shell="/bin/sh", cpu=2.0, workdir="/workspace"
    )

    ret_config = ray.get(actor.get_config.remote())
    assert ret_config.cpu == 2.0

    ray.get(actor.delete.remote())
    ray.kill(actor)


@pytest.mark.skipif(_CGROUP_PROVISIONED, reason="Checks a node without a cgroup")
def test_sandbox_actor_fails_closed_without_cgroup():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    # Without an explicit cpu, the actor's CPUs become an enforced limit.
    actor = Sandbox.options(num_cpus=2.0).remote(
        image="busybox:latest", shell="/bin/sh", workdir="/workspace"
    )

    with pytest.raises(ray.exceptions.ActorDiedError, match="RAY_SANDBOX_CGROUP_PATH"):
        ray.get(actor.get_config.remote())


def test_sandbox_runtime_create_variants():
    rt = SandboxRuntime()

    # Pass image as positional arg
    id1 = rt.create("busybox:latest", workdir="/workspace")
    assert id1.startswith("ray-sandbox-")
    rt.delete(id1)

    # Pass image as keyword arg
    id2 = rt.create(image="busybox:latest", shell="/bin/sh", workdir="/workspace")
    assert id2.startswith("ray-sandbox-")
    rt.delete(id2)

    # Missing image should raise TypeError
    with pytest.raises(TypeError):
        rt.create(workdir="/workspace", cpu=1.0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
