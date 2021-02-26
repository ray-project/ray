import sys
import numpy as np
import torch

import ray
import ray.util.collective as col
from ray.util.collective.types import Backend


result = {
    "allreduce": [[2, 4, 6], [2, 4, 6]],
    "allgather": [[[1, 2, 3], [1, 2, 3]],
                  [[1, 2, 3], [1, 2, 3]]],
    "reduce": [[2, 4, 6], [2, 4, 6]],
    "reducescatter": [[2, 4, 6], [2, 4, 6]],
    "sendrecv": [[1, 2, 3], [1, 2, 3]],
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


def get_empty_buffer(data):
    '''
    data: the data buffer used to create empty buffer
    '''
    if isinstance(data, np.ndarray):
        return np.zeros_like(data, dtype=data.dtype)
    elif isinstance(data, torch.Tensor):
        return torch.zeros_like(data).float()
    else:
        raise RuntimeError(f"Unrecognized tensor type. Got {type(data)}")


def get_reducescatter_empty_buffer(data, world_size):
    if isinstance(data, np.ndarray):
        return np.zeros((data.size//world_size,), dtype=data.dtype)
    elif isinstance(data, torch.Tensor):
        return torch.zeros(torch.numel(data)//world_size).float()
    else:
        raise RuntimeError("Unrecognized tensor type. Got {type(data)}")


@ray.remote(num_gpus=1)
def allreduce(rank, world_size, group_name="default", tensor_type="numpy"):
    '''
    rank, world_size, group_name="default"
    '''
    assert col.gloo_available()

    group_name = f"{group_name}_{tensor_type}"
    col.init_collective_group(
        world_size, rank, Backend.GLOO, group_name)

    data = get_test_data(tensor_type)

    col.allreduce(data, group_name)

    col.destroy_collective_group(group_name)

    assert (data == result[group_name.split("_")[0]]).all()

    print(f"{group_name} test completed") if rank == 0 else None


@ray.remote(num_gpus=1)
def allgather(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    group_name = f"{group_name}_{tensor_type}"
    col.init_collective_group(
        world_size, rank, Backend.GLOO, group_name)

    data = get_test_data(tensor_type)

    data_list = [np.zeros_like(data, dtype=np.float32)
                 for _ in range(world_size)]

    col.allgather(data_list, data, group_name)

    col.destroy_collective_group(group_name)

    data_list = [i.tolist() for i in data_list]
    assert data_list == result[group_name.split("_")[0]]

    print(f"{group_name} test completed") if rank == 0 else None


@ray.remote(num_gpus=1)
def reduce(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    group_name = f"{group_name}_{tensor_type}"
    col.init_collective_group(
        world_size, rank, Backend.GLOO, group_name)

    data = get_test_data(tensor_type)

    col.reduce(data, dst_rank=0, group_name=group_name)

    col.destroy_collective_group(group_name)

    if rank == 0:
        assert (data == result[group_name.split("_")[0]]).all()
    print(f"{group_name} test completed") if rank == 0 else None


@ray.remote(num_gpus=1)
def reducescatter(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    group_name = f"{group_name}_{tensor_type}"
    col.init_collective_group(
        world_size, rank, Backend.GLOO, group_name)

    data_list = [get_test_data(tensor_type) for _ in range(world_size)]
    data = get_empty_buffer(get_test_data(tensor_type))

    col.reducescatter(data, data_list, group_name)

    col.destroy_collective_group(group_name)

    assert (data == result[group_name.split("_")[0]]).all()

    print(f"{group_name} test completed") if rank == 0 else None


@ray.remote(num_gpus=1)
def sendrecv(rank, world_size, group_name="default", tensor_type="numpy"):
    assert col.gloo_available()

    group_name = f"{group_name}_{tensor_type}"
    col.init_collective_group(
        world_size, rank, Backend.GLOO, group_name)

    data = get_test_data(tensor_type)
    if rank == 0:
        col.send(data, 1, group_name)
    elif rank == 1:
        data = get_empty_buffer(data)
        col.recv(data, 0, group_name)
    col.destroy_collective_group(group_name)

    if rank == 0:
        print(f"{group_name}(send) test completed")
    elif rank == 1:
        assert (data == result[group_name.split("_")[0]]).all()
        print(f"{group_name}(recv) test completed")


@ray.remote(num_cpus=0)
def multiGroup_cocurrent(groupNum, world_size):
    @ray.remote(num_cpus=1)
    def singleGroup(rank, world_size, group_name):
        col.init_collective_group(world_size, rank,
                                  Backend.GLOO, group_name)

        col.destroy_collective_group(group_name)

    assert col.gloo_available()
    group_names = [f"default_cocu{i}" for i in range(groupNum)]
    fns = []
    for group_name in group_names:
        fns += [singleGroup.remote(rank, world_size, group_name)
                for rank in range(world_size)]
    ray.get(fns)

    print("multiGroup_cocurrent test completed")


@ray.remote(num_cpus=1)
def multiGroup_seq(rank, world_size):
    assert col.gloo_available()
    groupNum = 10
    group_names = [f"default{i}" for i in range(groupNum)]
    for group_name in group_names:
        col.init_collective_group(world_size, rank,
                                  Backend.GLOO, group_name)
    for group_name in group_names:
        col.destroy_collective_group(group_name)

    print("multiGroup_seq test completed") if rank == 0 else None


if __name__ == "__main__":
    ray.init(num_cpus=16)
    world_size = 2
    test_list = result.keys()
    for fn_name in test_list:
        fn = getattr(sys.modules[__name__], fn_name)
        ray.get([fn.remote(rank, world_size, fn_name)
                 for rank in range(world_size)])

    ray.get([multiGroup_seq.remote(rank, world_size)
             for rank in range(world_size)])

    ray.get(multiGroup_cocurrent.remote(5, 2))
