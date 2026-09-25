import os
import sys

import pytest

import ray
from ray.experimental.sandbox import Sandbox, create
from ray.experimental.sandbox._internal import overlayfs
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig

# Real-GPU-hardware sandbox tests, needing a real GPU and real nvidia-ctk
# on PATH. Tagged "gpu" and built as their own bazel target (see
# BUILD.bazel), scheduled only by the "core: sandbox gpu tests" Buildkite
# job (see .buildkite/core.rayci.yml) -- not collected by the CPU-only
# "core: sandbox tests" job. No runtime skip based on GPU availability:
# CI's tag-based scheduling guarantees a GPU is present.
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
    create(num_gpus=1) rather than Sandbox.options(num_gpus=1).remote()
    -- validates create()'s num_gpus actually reaches Ray's actor
    scheduler against real hardware, not just via a mocked
    Sandbox.options() (see test_create_threads_num_gpus_into_actor_options
    in test_gvisor_backend.py)."""
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
    """nvidia-smi (the other tests in this file) only exercises NVML --
    read-only device queries, no CUDA context. This runs NVIDIA's standard
    vectorAdd sample (the same image GPU Operator itself uses to validate
    a node) to confirm a sandbox can actually create a CUDA context, launch
    a kernel, and copy results back -- real CUDA usage, not just device
    visibility."""
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


def _snapshot_tree(root: str) -> dict:
    """Every entry under `root`, keyed by relative path, with enough of its
    lstat to tell whether anything was added, removed or modified."""
    snapshot = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            path = os.path.join(dirpath, name)
            st = os.lstat(path)
            snapshot[os.path.relpath(path, root)] = (
                st.st_mode,
                st.st_size,
                st.st_mtime_ns,
                st.st_ino,
            )
    return snapshot


@pytest.fixture(params=list(overlayfs.MountMode))
def gpu_mount_mode(request, monkeypatch):
    """Runs a test through each way of mounting a GPU sandbox's overlay this
    worker supports. A root worker mounts it directly, and any other worker
    mounts it inside a user namespace."""
    mode = request.param
    privileged = overlayfs.has_mount_privilege()
    if mode is overlayfs.MountMode.PRIVILEGED and not privileged:
        pytest.skip("this worker has no mount privilege")
    if mode is overlayfs.MountMode.USERNS:
        if privileged:
            pytest.skip("a worker with mount privilege never uses a user namespace")
        if not overlayfs.can_mount_in_userns():
            pytest.skip("this host can't mount overlays in a user namespace")
    monkeypatch.setattr(overlayfs, "mount_mode", lambda: mode)
    return mode


@pytest.mark.parametrize("readonly", [True, False])
def test_sandbox_gpu_leaves_unpacked_tree_untouched(
    gpu_mount_mode, readonly, monkeypatch
):
    """A GPU sandbox runs nvidia-smi in each mount mode, read-only or
    writable. NVIDIA's createContainer hooks write into the rootfs, and runsc
    creates mount points for the GPU's driver files and device nodes. None
    of it may reach the host's unpacked tree the overlay sits on, not even
    an empty mount point."""
    image = "nvidia/cuda:12.4.0-base-ubuntu22.04"
    # The sandbox is created in this process rather than a Sandbox actor, so
    # the mount mode above applies. gpu_ids must be among the GPUs Ray
    # assigned the caller.
    monkeypatch.setattr(ray, "get_gpu_ids", lambda: [0])
    backend = GVisorSandboxBackend()
    manager = backend._image_manager
    manager.pull_image(image, instance_id="gpu-tree-snapshot")
    try:
        tree = manager._get_unpacked_rootfs(
            image,
            preserve_owners=gpu_mount_mode is overlayfs.MountMode.PRIVILEGED,
        )
        before = _snapshot_tree(tree)

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

        assert _snapshot_tree(tree) == before
    finally:
        manager.release_image(image, "gpu-tree-snapshot")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
