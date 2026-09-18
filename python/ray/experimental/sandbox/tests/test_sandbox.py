import sys

import pytest

import ray
from ray.experimental.sandbox import Sandbox
from ray.experimental.sandbox.runtime import SandboxRuntime


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


def test_sandbox_gpu_ids_distinct_across_concurrent_actors():
    """One sandbox per GPU: with 4 (fake) GPUs on the cluster and 4 actors
    each requesting num_gpus=1, Ray's own scheduler hands out 4 distinct
    ids, and each actor auto-inherits its own via ray.get_gpu_ids() -- the
    same call Sandbox.__init__ makes. No real GPU/CDI/gVisor is involved,
    only Ray's own GPU scheduling.

    Forces a fresh 4-GPU cluster rather than relying on
    ray.init(ignore_reinit_error=True): that's a no-op on an already-running
    cluster, which would silently keep whatever GPU count an earlier test
    in this module left behind."""
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_gpus=4)

    @ray.remote(num_gpus=1)
    class GpuIdProbe:
        def gpu_ids(self):
            # str()-coerced to match what Sandbox.__init__ actually does --
            # ray.get_gpu_ids() can return ints.
            return [str(i) for i in ray.get_gpu_ids()]

    probes = [GpuIdProbe.remote() for _ in range(4)]
    try:
        resolved = ray.get([p.gpu_ids.remote() for p in probes])
        assert all(len(ids) == 1 for ids in resolved)
        assert sorted(ids[0] for ids in resolved) == ["0", "1", "2", "3"]
    finally:
        for p in probes:
            ray.kill(p)
        ray.shutdown()


def test_sandbox_actor_gpu_ids_default_to_none_without_assigned_gpus():
    """Mirrors cpu/memory auto-inherit: with no GPUs assigned to the actor,
    ray.get_gpu_ids() returns [] and gpu_ids stays None (no error) rather
    than being coerced to an empty/falsy non-None value. A None gpu_ids
    means create_oci_spec never reaches its CDI lookup at all -- see
    test_create_oci_spec_raises_when_no_cdi_spec_found in
    test_image_manager.py for what CDI lookup failure actually looks
    like."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.remote(
        image="busybox:latest", shell="/bin/sh", workdir="/workspace"
    )
    ret_config = ray.get(actor.get_config.remote())
    assert ret_config.gpu_ids is None

    ray.get(actor.delete.remote())
    ray.kill(actor)


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
