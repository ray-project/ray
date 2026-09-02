import sys

import pytest

import ray
from ray.experimental.sandbox import Sandbox, create
from ray.experimental.sandbox.runtime import SandboxRuntime

# Real-multi-GPU-hardware sandbox tests, needing 4 real GPUs and real
# nvidia-ctk on PATH. Tagged "multi_gpu" and built as their own bazel
# target (see BUILD.bazel), scheduled only by the "core: sandbox
# multi-gpu tests" Buildkite job (see .buildkite/core.rayci.yml) on a
# gpu-large runner -- not collected by the CPU-only "core: sandbox
# tests" job or the single-GPU "core: sandbox gpu tests" job.


def test_sandbox_gpu_each_sandbox_gets_a_distinct_gpu():
    """One sandbox per GPU, across all 4: each of 4 concurrently created
    Sandbox actors requesting num_gpus=1 sees exactly one GPU inside its
    sandbox, and no two sandboxes see the same GPU -- validating CDI
    device injection is scoped per-sandbox, not just "some GPU works"."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actors = [
        Sandbox.options(num_gpus=1).remote(
            image="nvidia/cuda:12.4.0-base-ubuntu22.04",
        )
        for _ in range(4)
    ]
    try:
        results = ray.get(
            [
                actor.exec.remote("nvidia-smi --query-gpu=uuid --format=csv,noheader")
                for actor in actors
            ]
        )
        seen_uuids = []
        for result in results:
            assert result.exit_code == 0, result.stderr
            uuids = [line for line in result.stdout.strip().splitlines() if line]
            assert len(uuids) == 1, f"expected exactly 1 GPU visible, got {uuids}"
            seen_uuids.append(uuids[0])

        assert (
            len(set(seen_uuids)) == 4
        ), f"sandboxes did not each get a distinct GPU: {seen_uuids}"
    finally:
        for actor in actors:
            ray.get(actor.delete.remote())
            ray.kill(actor)


def test_sandbox_create_gpu_each_sandbox_gets_a_distinct_gpu():
    """Same as test_sandbox_gpu_each_sandbox_gets_a_distinct_gpu, but via
    create(num_gpus=1) rather than Sandbox.options(num_gpus=1).remote()
    -- validates create()'s num_gpus threading against real multi-GPU
    hardware, not just via a mocked Sandbox.options() (see
    test_create_threads_num_gpus_into_actor_options in
    test_gvisor_backend.py)."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    actors = [
        create(
            image="nvidia/cuda:12.4.0-base-ubuntu22.04",
            num_gpus=1,
        )
        for _ in range(4)
    ]
    try:
        results = ray.get(
            [
                actor.exec.remote("nvidia-smi --query-gpu=uuid --format=csv,noheader")
                for actor in actors
            ]
        )
        seen_uuids = []
        for result in results:
            assert result.exit_code == 0, result.stderr
            uuids = [line for line in result.stdout.strip().splitlines() if line]
            assert len(uuids) == 1, f"expected exactly 1 GPU visible, got {uuids}"
            seen_uuids.append(uuids[0])

        assert (
            len(set(seen_uuids)) == 4
        ), f"sandboxes did not each get a distinct GPU: {seen_uuids}"
    finally:
        for actor in actors:
            ray.get(actor.delete.remote())
            ray.kill(actor)


def test_sandbox_gpu_pool_actor_pins_each_sandbox_to_a_distinct_gpu():
    """One actor, multiple GPUs, multiple sandboxes -- each pinned to a
    different GPU it was assigned. Structurally impossible for Sandbox
    (always exactly one actor = one sandbox); this is what
    SandboxRuntime.create(gpu_ids=[id]), called directly inside a custom
    actor, is for (an extension of the SandboxPool pattern in the docs).
    Exercises the explicit-gpu_ids validation path (a subset of what
    ray.get_gpu_ids() assigned to this actor) against real hardware,
    complementing the auto-inherit path the test above checks."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    @ray.remote(num_gpus=4)
    class GpuSandboxPool:
        def __init__(self, image):
            self.runtime = SandboxRuntime()
            self.sandboxes = {
                gpu_id: self.runtime.create(image=image, gpu_ids=[gpu_id])
                for gpu_id in [str(i) for i in ray.get_gpu_ids()]
            }

        def gpu_ids(self):
            return list(self.sandboxes)

        def exec_on(self, gpu_id, command):
            return self.runtime.exec(self.sandboxes[gpu_id], command)

        def close(self):
            for sandbox_id in self.sandboxes.values():
                self.runtime.delete(sandbox_id)

    pool = GpuSandboxPool.remote(image="nvidia/cuda:12.4.0-base-ubuntu22.04")
    try:
        gpu_ids = ray.get(pool.gpu_ids.remote())
        assert len(gpu_ids) == 4, gpu_ids

        results = ray.get(
            [
                pool.exec_on.remote(
                    gpu_id, "nvidia-smi --query-gpu=uuid --format=csv,noheader"
                )
                for gpu_id in gpu_ids
            ]
        )
        seen_uuids = []
        for result in results:
            assert result.exit_code == 0, result.stderr
            uuids = [line for line in result.stdout.strip().splitlines() if line]
            assert len(uuids) == 1, f"expected exactly 1 GPU visible, got {uuids}"
            seen_uuids.append(uuids[0])

        assert (
            len(set(seen_uuids)) == 4
        ), f"pinned sandboxes did not each get a distinct GPU: {seen_uuids}"
    finally:
        ray.get(pool.close.remote())
        ray.kill(pool)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
