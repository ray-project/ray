import sys

import pytest

import ray
from ray.experimental.sandbox import Sandbox, create

# Real-GPU-hardware sandbox tests, needing a real GPU and real nvidia-ctk
# on PATH. Tagged "gpu" and built as their own bazel target (see
# BUILD.bazel), scheduled only by the "core: sandbox gpu tests" Buildkite
# job (see .buildkite/core.rayci.yml) -- not collected by the CPU-only
# "core: sandbox tests" job. No runtime skip based on GPU availability:
# CI's tag-based scheduling guarantees a GPU is present.


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
