"""Test the send/recv API."""
import pytest
import torch
import torch_npu

import ray
from ray.util.collective.tests.npu_util import create_collective_multinpu_workers
from ray.util.collective.types import Backend


@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("dst_npu_index", [0, 1])
@pytest.mark.parametrize("src_npu_index", [0, 1])
@pytest.mark.parametrize(
    "array_size", [2**10, 2**15, 2**20, [2, 2], [5, 9, 10, 85]]
)
@pytest.mark.parametrize("backend", [Backend.HCCL])
def test_sendrecv(
    ray_start_distributed_multinpu_2_nodes_4_npus,
    array_size,
    src_rank,
    dst_rank,
    src_npu_index,
    dst_npu_index,
    backend,
):
    if src_rank == dst_rank:
        return

    world_size = 2
    actors, _ = create_collective_multinpu_workers(num_workers=world_size, backend=backend)

    ray.get(actors[0].set_buffer.remote(array_size, value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote(array_size, value0=4, value1=5))

    refs = []
    for i in range(world_size):
        refs.append(actors[i].get_buffer.remote())

    refs[src_rank][src_npu_index] = actors[src_rank].do_send_multinpu.remote(
        dst_rank=dst_rank,
        dst_npu_index=dst_npu_index,
        src_npu_index=src_npu_index,
    )
    refs[dst_rank][dst_npu_index] = actors[dst_rank].do_recv_multinpu.remote(
        src_rank=src_rank,
        src_npu_index=src_npu_index,
        dst_npu_index=dst_npu_index,
    )

    results = []
    results_flattened = ray.get(refs[0] + refs[1])
    results.append([results_flattened[0], results_flattened[1]])
    results.append([results_flattened[2], results_flattened[3]])

    expected_val = (src_rank + 1) * 2 + src_npu_index

    assert (
        results[src_rank][src_npu_index]
        == torch.ones(array_size, dtype=torch.float32).npu(src_npu_index) * expected_val
    ).all()

    assert (
        results[dst_rank][dst_npu_index]
        == torch.ones(array_size, dtype=torch.float32).npu(dst_npu_index) * expected_val
    ).all()

    ray.get([a.destroy_group.remote() for a in actors])
