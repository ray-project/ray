"""Test the send/recv API."""
import cupy as cp
import pytest
import ray

from ray.util.collective.tests.util import create_collective_multiacc_workers


# @pytest.mark.parametrize("group_name", ["default", "test", "123?34!"])
@pytest.mark.parametrize("dst_rank", [0, 1])
@pytest.mark.parametrize("src_rank", [0, 1])
@pytest.mark.parametrize("dst_acc_index", [0, 1])
@pytest.mark.parametrize("src_acc_index", [0, 1])
@pytest.mark.parametrize(
    "array_size", [2**10, 2**15, 2**20, [2, 2], [5, 9, 10, 85]]
)
def test_sendrecv(
    ray_start_distributed_multiacc_2_nodes_4_accs,
    array_size,
    src_rank,
    dst_rank,
    src_acc_index,
    dst_acc_index,
):
    if src_rank == dst_rank:
        return
    world_size = 2
    actors, _ = create_collective_multiacc_workers(num_workers=world_size)

    ray.get(actors[0].set_buffer.remote(array_size, value0=2, value1=3))
    ray.get(actors[1].set_buffer.remote(array_size, value0=4, value1=5))

    refs = []
    for i in range(world_size):
        refs.append(actors[i].get_buffer.remote())
    refs[src_rank][src_acc_index] = actors[src_rank].do_send_multiacc.remote(
        dst_rank=dst_rank, dst_acc_index=dst_acc_index, src_acc_index=src_acc_index
    )
    refs[dst_rank][dst_acc_index] = actors[dst_rank].do_recv_multiacc.remote(
        src_rank=src_rank, src_acc_index=src_acc_index, dst_acc_index=dst_acc_index
    )
    results = []
    results_flattend = ray.get(refs[0] + refs[1])
    results.append([results_flattend[0], results_flattend[1]])
    results.append([results_flattend[2], results_flattend[3]])
    assert (
        results[src_rank][src_acc_index]
        == cp.ones(array_size, dtype=cp.float32) * ((src_rank + 1) * 2 + src_acc_index)
    ).all()
    assert (
        results[dst_rank][dst_acc_index]
        == cp.ones(array_size, dtype=cp.float32) * ((src_rank + 1) * 2 + src_acc_index)
    ).all()
    ray.get([a.destroy_group.remote() for a in actors])
