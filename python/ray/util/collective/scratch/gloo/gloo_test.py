import sys
import numpy as np
import torch

import ray
import ray.util.collective as col
from ray.util.collective.types import Backend, ReduceOp


result = {
    "allreduce": [[2, 4, 6], [2, 4, 6]],
    "allgather": [[1, 2, 3], [1, 2, 3]],
    "reduce": [[2, 4, 6], [2, 4, 6]],
    "gather": [[1, 2, 3], [1, 2, 3]],
}


def get_test_data(tensor_type="numpy"):
    '''
    tensor_type: the tensor type to test. Optional: "numpy", "torch"
    '''
    data = [[1, 2, 3], [1, 2, 3]]
    if tensor_type == "numpy":
        return np.array(data, dtype=np.float32)
    elif tensor_type == "torch":
        return torch.Tensor(data).float()
    else:
        raise RuntimeError(f"Unrecognized tensor type. Got {tensor_type}")


@ray.remote(num_gpus=1)
def allreduce(rank, world_size, group_name="default", tensor_type="numpy"):
    '''
    rank, world_size, group_name="default"
    '''
    assert col.gloo_available()

    col.init_collective_group(
        world_size, rank, Backend.GLOO, f"{group_name}_{tensor_type}")

    data = get_test_data(tensor_type)

    col.allreduce(data, group_name)

    col.destroy_collective_group(group_name)

    assert (data == result[group_name]).all()

    print(f"{group_name} test completed")


@ray.remote(num_gpus=1)
def allgather(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    col.init_collective_group(
        world_size, rank, Backend.GLOO, f"{group_name}_{tensor_type}")

    data = get_test_data(tensor_type)

    col.allreduce(data, group_name)

    col.destroy_collective_group(group_name)

    assert (data == result[group_name]).all()

    print(f"{group_name} test completed")


@ray.remote(num_gpus=1)
def reduce(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    col.init_collective_group(
        world_size, rank, Backend.GLOO, f"{group_name}_{tensor_type}")

    data = get_test_data(tensor_type)

    col.allreduce(data, group_name)

    col.destroy_collective_group(group_name)

    assert (data == result[group_name]).all()

    print(f"{group_name} test completed")


@ray.remote(num_gpus=1)
def gather(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    col.init_collective_group(
        world_size, rank, Backend.GLOO, f"{group_name}_{tensor_type}")

    data = get_test_data(tensor_type)

    col.allreduce(data, group_name)

    col.destroy_collective_group(group_name)

    assert (data == result[group_name]).all()

    print(f"{group_name} test completed")


if __name__ == "__main__":
    ray.init(num_cpus=6)
    world_size = 2

    for fn_name in result:
        fn = getattr(sys.modules[__name__], fn_name)
        ray.get([fn.remote(i, world_size, fn_name) for i in range(world_size)])
