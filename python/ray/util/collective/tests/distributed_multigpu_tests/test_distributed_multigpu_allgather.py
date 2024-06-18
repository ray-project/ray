"""Test the allgather API on a distributed Ray cluster."""
import pytest
import ray

import cupy as cp
import torch

from ray.util.collective.tests.util import (
    create_collective_multiacc_workers,
    init_tensors_for_gather_scatter_multiacc,
)


@pytest.mark.parametrize("tensor_backend", ["cupy", "torch"])
@pytest.mark.parametrize(
    "array_size", [2, 2**5, 2**10, 2**15, 2**20, [2, 2], [5, 5, 5]]
)
def test_allgather_different_array_size(
    ray_start_distributed_multiacc_2_nodes_4_accs, array_size, tensor_backend
):
    world_size = 2
    num_acc_per_worker = 2
    actual_world_size = world_size * num_acc_per_worker
    actors, _ = create_collective_multiacc_workers(world_size)
    init_tensors_for_gather_scatter_multiacc(
        actors, array_size=array_size, tensor_backend=tensor_backend
    )
    results = ray.get([a.do_allgather_multiacc.remote() for a in actors])
    for i in range(world_size):
        for j in range(num_acc_per_worker):
            for k in range(actual_world_size):
                if tensor_backend == "cupy":
                    assert (
                        results[i][j][k] == cp.ones(array_size, dtype=cp.float32)
                    ).all()
                else:
                    assert (
                        results[i][j][k]
                        == torch.ones(array_size, dtype=torch.float32).cuda(j)
                    ).all()


def test_allgather_torch_cupy(ray_start_distributed_multiacc_2_nodes_4_accs):
    world_size = 2
    num_acc_per_worker = 2
    actual_world_size = world_size * num_acc_per_worker
    shape = [10, 10]
    actors, _ = create_collective_multiacc_workers(world_size)

    # tensor is pytorch, list is cupy
    for i, a in enumerate(actors):
        ray.get(
            [a.set_buffer.remote(shape, tensor_type0="torch", tensor_type1="torch")]
        )
        ray.get(
            [a.set_list_buffer.remote(shape, tensor_type0="cupy", tensor_type1="cupy")]
        )
    results = ray.get([a.do_allgather_multiacc.remote() for a in actors])
    for i in range(world_size):
        for j in range(num_acc_per_worker):
            for k in range(actual_world_size):
                assert (results[i][j][k] == cp.ones(shape, dtype=cp.float32)).all()

    # tensor is cupy, list is pytorch
    for i, a in enumerate(actors):
        ray.get([a.set_buffer.remote(shape, tensor_type0="cupy", tensor_type1="cupy")])
        ray.get(
            [
                a.set_list_buffer.remote(
                    shape, tensor_type0="torch", tensor_type1="torch"
                )
            ]
        )
    results = ray.get([a.do_allgather_multiacc.remote() for a in actors])
    for i in range(world_size):
        for j in range(num_acc_per_worker):
            for k in range(actual_world_size):
                assert (
                    results[i][j][k] == torch.ones(shape, dtype=torch.float32).cuda(j)
                ).all()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
