"""Scale test: every XCCL collective at larger world sizes (default 8,16,32).

Sizes come from XCCL_SCALE_SIZES; a size is skipped if the cluster has fewer GPUs.
"""
import pytest

import ray
from ray.util.collective.tests.xpu_util import (
    SCALE_SIZES,
    ScaleWorker,
    make_runtime_env,
    require_gpus,
)


@pytest.fixture
def ray_scale_cluster():
    ray.init("auto", runtime_env=make_runtime_env())
    yield
    ray.shutdown()


@pytest.mark.parametrize("world_size", SCALE_SIZES)
def test_scale_collectives(ray_scale_cluster, world_size):
    require_gpus(world_size)
    actors = [ScaleWorker.remote() for _ in range(world_size)]
    try:
        group = f"scale_{world_size}"
        assert all(
            ray.get(
                [
                    a.init_group.remote(world_size, i, group)
                    for i, a in enumerate(actors)
                ],
                timeout=600,
            )
        )
        results = ray.get([a.run_all.remote() for a in actors], timeout=900)
        for rank, res in enumerate(results):
            for op in (
                "allreduce",
                "broadcast",
                "allgather",
                "reducescatter",
                "barrier",
                "ring",
            ):
                assert res[op], f"rank {rank}/{world_size}: {op} gave a wrong result"
        ray.get([a.destroy_group.remote() for a in actors], timeout=120)
    finally:
        for a in actors:
            ray.kill(a)
