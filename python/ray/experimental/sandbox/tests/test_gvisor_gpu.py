import hashlib
import sys

import pytest

import ray
from ray.experimental.sandbox import Sandbox, create
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig

# Real-GPU-hardware sandbox tests, needing a GPU and nvidia-ctk on PATH.
# Tagged "gpu" and built as their own bazel target (see BUILD.bazel). Only
# .buildkite/core.rayci.yml's core-sandbox-gpu-tests job runs them, not the
# CPU-only "core: sandbox tests" job. The ensure_nvidia_ctk fixture skips
# them on a host without a GPU, and fails them on that job, which sets
# TEST_SANDBOX_GPU=1.
pytestmark = pytest.mark.usefixtures("ensure_nvidia_ctk")


def test_sandbox_gpu_nvidia_smi_sees_assigned_gpu():
    """End-to-end validation of the GPU mechanism this module adds: a
    Sandbox actor scheduled with num_gpus=1 auto-inherits that GPU,
    resolves a real CDI spec via nvidia-ctk, injects the real device nodes
    and driver-library mounts, and boots gVisor with --nvproxy."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.options(num_gpus=1).remote(
        image="nvidia/cuda:12.4.0-base-ubuntu22.04",
    )
    try:
        result = ray.get(actor.exec.remote("nvidia-smi"))
        assert result.exit_code == 0, result.stderr
        assert "NVIDIA-SMI" in result.stdout
    finally:
        ray.get(actor.delete.remote())
        ray.kill(actor)


def test_sandbox_create_gpu_nvidia_smi_sees_assigned_gpu():
    """Same as test_sandbox_gpu_nvidia_smi_sees_assigned_gpu, but via
    create(num_gpus=1) rather than Sandbox.options(num_gpus=1).remote(). This
    checks that create()'s num_gpus reaches Ray's actor scheduler on real
    hardware, where test_create_threads_num_gpus_into_actor_options in
    test_gvisor_backend.py mocks Sandbox.options()."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = create(
        image="nvidia/cuda:12.4.0-base-ubuntu22.04",
        num_gpus=1,
    )
    try:
        result = ray.get(actor.exec.remote("nvidia-smi"))
        assert result.exit_code == 0, result.stderr
        assert "NVIDIA-SMI" in result.stdout
    finally:
        ray.get(actor.delete.remote())
        ray.kill(actor)


def test_sandbox_gpu_nvidia_smi_runs_on_non_debian_image():
    """Runs nvidia-smi in a sandbox on a RHEL UBI9-minimal image, whose
    default library search path (/usr/lib64) differs from the other
    tests' Debian-based images."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.options(num_gpus=1).remote(
        image="redhat/ubi9-minimal:latest",
    )
    try:
        result = ray.get(actor.exec.remote("nvidia-smi"))
        assert result.exit_code == 0, result.stderr
        assert "NVIDIA-SMI" in result.stdout
    finally:
        ray.get(actor.delete.remote())
        ray.kill(actor)


def test_sandbox_gpu_cuda_vectoradd_runs_a_real_kernel():
    """nvidia-smi, which the other tests in this file run, only exercises
    NVML's read-only device queries, with no CUDA context. This runs
    NVIDIA's standard vectorAdd sample, the same image GPU Operator uses to
    validate a node, to confirm a sandbox can create a CUDA context, launch
    a kernel, and copy results back."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actor = Sandbox.options(num_gpus=1).remote(
        image="nvcr.io/nvidia/k8s/cuda-sample:vectoradd-cuda11.7.1-ubuntu20.04",
    )
    try:
        result = ray.get(actor.exec.remote("/cuda-samples/vectorAdd"))
        assert result.exit_code == 0, result.stderr
        assert "PASSED" in result.stdout, result.stdout
    finally:
        ray.get(actor.delete.remote())
        ray.kill(actor)


def _file_digest(path: str) -> str:
    """The SHA-256 of the file at ``path``."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@pytest.mark.parametrize("readonly", [True, False])
def test_sandbox_gpu_leaves_cached_image_untouched(
    overlay_mount, readonly, monkeypatch
):
    """A GPU sandbox runs nvidia-smi through each way of mounting its
    overlayfs rootfs, read-only or writable. NVIDIA's createContainer hooks
    write into the rootfs, and runsc creates mount points for the GPU's
    driver files and device nodes. None of it may reach the cached EROFS
    image the overlay sits on."""
    image = "nvidia/cuda:12.4.0-base-ubuntu22.04"
    # The sandbox is created in this process rather than a Sandbox actor, so
    # the mount above applies. gpu_ids must be among the GPUs Ray assigned
    # the caller.
    monkeypatch.setattr(ray, "get_gpu_ids", lambda: [0])
    backend = GVisorSandboxBackend()
    manager = backend._image_manager
    manager.pull_image(image, instance_id="gpu-image-snapshot")
    try:
        rootfs_image = manager.get_rootfs_image(image)
        before = _file_digest(rootfs_image)

        sandbox_id = backend.create_sandbox(
            GVisorSandboxConfig(image=image, readonly=readonly, gpu_ids=["0"])
        )
        try:
            result = backend.exec_command(sandbox_id, "nvidia-smi")
            assert result.exit_code == 0, result.stderr
            probe = backend.exec_command(sandbox_id, "touch /etc/probe")
            assert (probe.exit_code == 0) is not readonly
        finally:
            backend.delete_sandbox(sandbox_id)

        assert _file_digest(rootfs_image) == before
    finally:
        manager.release_image(image, "gpu-image-snapshot")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
