import cupy as cp

import ray
import ray.util.collective as col
from ray.util.collective.types import Backend, ReduceOp

import torch


@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        self.buffer = cp.ones((10, ), dtype=cp.float32)
        self.list_buffer = [
            cp.ones((10, ), dtype=cp.float32),
            cp.ones((10, ), dtype=cp.float32)
        ]

    def init_group(self,
                   world_size,
                   rank,
                   backend=Backend.NCCL,
                   group_name="default"):
        col.init_collective_group(world_size, rank, backend, group_name)
        return True

    def set_buffer(self, data):
        self.buffer = data
        return self.buffer

    def get_buffer(self):
        return self.buffer

    def set_list_buffer(self, list_of_arrays):
        self.list_buffer = list_of_arrays
        return self.list_buffer

    def do_allreduce(self, group_name="default", op=ReduceOp.SUM):
        col.allreduce(self.buffer, group_name, op)
        return self.buffer

    def do_reduce(self, group_name="default", dst_rank=0, op=ReduceOp.SUM):
        col.reduce(self.buffer, dst_rank, group_name, op)
        return self.buffer

    def do_broadcast(self, group_name="default", src_rank=0):
        col.broadcast(self.buffer, src_rank, group_name)
        return self.buffer

    def do_allgather(self, group_name="default"):
        col.allgather(self.list_buffer, self.buffer, group_name)
        return self.list_buffer

    def do_reducescatter(self, group_name="default", op=ReduceOp.SUM):
        col.reducescatter(self.buffer, self.list_buffer, group_name, op)
        return self.buffer

    def do_send(self, group_name="default", dst_rank=0):
        col.send(self.buffer, dst_rank, group_name)
        return self.buffer

    def do_recv(self, group_name="default", src_rank=0):
        col.recv(self.buffer, src_rank, group_name)
        return self.buffer

    def destroy_group(self, group_name="default"):
        col.destroy_collective_group(group_name)
        return True

    def report_rank(self, group_name="default"):
        rank = col.get_rank(group_name)
        return rank

    def report_world_size(self, group_name="default"):
        ws = col.get_world_size(group_name)
        return ws

    def report_nccl_availability(self):
        avail = col.nccl_available()
        return avail

    def report_gloo_availability(self):
        avail = col.gloo_available()
        return avail

    def report_is_group_initialized(self, group_name="default"):
        is_init = col.is_group_initialized(group_name)
        return is_init


def create_collective_workers(num_workers=2,
                              group_name="default",
                              backend="nccl"):
    actors = [Worker.remote() for _ in range(num_workers)]
    world_size = num_workers
    init_results = ray.get([
        actor.init_group.remote(world_size, i, backend, group_name)
        for i, actor in enumerate(actors)
    ])
    return actors, init_results


@ray.remote(num_gpus=2)
class MultiGPUWorker:
    def __init__(self):
        with cp.cuda.Device(0):
            self.buffer0 = cp.ones((10, ), dtype=cp.float32)
            self.list_buffer0 = [cp.ones((10,), dtype=cp.float32) for _ in range(4)]
        with cp.cuda.Device(1):
            self.buffer1 = cp.ones((10, ), dtype=cp.float32)
            self.list_buffer1 = [cp.ones((10,), dtype=cp.float32) for _ in range(4)]

    def init_group(self,
                   world_size,
                   rank,
                   backend=Backend.NCCL,
                   group_name="default"):
        col.init_collective_group(world_size, rank, backend, group_name)
        return True

    def set_buffer(self, buffer0, buffer1):
        self.buffer0 = buffer0
        self.buffer1 = buffer1
        return self.buffer0

    @ray.remote(num_returns=2)
    def get_buffer(self):
        return self.buffer0, self.buffer1

    def set_list_buffer(self, list_of_arrays):
        self.list_buffer = list_of_arrays
        return self.list_buffer

    def do_allreduce_multigpu(self, group_name="default", op=ReduceOp.SUM):
        col.allreduce_multigpu([self.buffer0, self.buffer1], group_name, op)
        return self.buffer0

    def do_reduce_multigpu(self, group_name="default", dst_rank=0, dst_gpu_index=0, op=ReduceOp.SUM):
        col.reduce_multigpu([self.buffer0, self.buffer1], dst_rank, dst_gpu_index, group_name, op)
        return self.buffer0

    def do_broadcast_multigpu(self, group_name="default", src_rank=0, src_gpu_index=0):
        col.broadcast_multigpu([self.buffer0, self.buffer1], src_rank, src_gpu_index, group_name)
        return self.buffer0

    def do_allgather_multigpu(self, group_name="default"):
        col.allgather_multigpu([self.list_buffer0, self.list_buffer1],
                               [self.buffer0, self.buffer1],
                               group_name)
        return self.list_buffer0

    def do_reducescatter_multigpu(self, group_name="default", op=ReduceOp.SUM):
        col.reducescatter_multigpu([self.buffer0, self.buffer1],
                                   [self.list_buffer0, self.list_buffer1],
                                   group_name, op)
        return self.buffer0

    def do_send_multigpu(self, group_name="default", dst_rank=0, dst_gpu_index=0, src_gpu_index=0):
        if src_gpu_index == 0:
            col.send_multigpu(self.buffer0, dst_rank, dst_gpu_index, group_name)
            return self.buffer0
        elif src_gpu_index == 1:
            col.send_multigpu(self.buffer1, dst_rank, dst_gpu_index, group_name)
            return self.buffer1
        else:
            raise RuntimeError()

    def do_recv_multigpu(self, group_name="default", src_rank=0, src_gpu_index=0, dst_gpu_index=0):
        if dst_gpu_index == 0:
            col.recv_multigpu(self.buffer0, src_rank, src_gpu_index, group_name)
            return self.buffer0
        elif dst_gpu_index == 1:
            col.recv_multigpu(self.buffer1, src_rank, src_gpu_index, group_name)
            return self.buffer1
        else:
            raise RuntimeError()

    def destroy_group(self, group_name="default"):
        col.destroy_collective_group(group_name)
        return True

    def report_rank(self, group_name="default"):
        rank = col.get_rank(group_name)
        return rank

    def report_world_size(self, group_name="default"):
        ws = col.get_world_size(group_name)
        return ws

    def report_nccl_availability(self):
        avail = col.nccl_available()
        return avail

    def report_gloo_availability(self):
        avail = col.gloo_available()
        return avail

    def report_is_group_initialized(self, group_name="default"):
        is_init = col.is_group_initialized(group_name)
        return is_init


def create_collective_multigpu_workers(num_workers=2,
                                       group_name="default",
                                       backend="nccl"):
    actors = [MultiGPUWorker.remote() for _ in range(num_workers)]
    world_size = num_workers
    init_results = ray.get([
        actor.init_group.remote(world_size, i, backend, group_name)
        for i, actor in enumerate(actors)
    ])
    return actors, init_results


def init_tensors_for_gather_scatter(actors,
                                    array_size=10,
                                    dtype=cp.float32,
                                    tensor_backend="cupy"):
    world_size = len(actors)
    for i, a in enumerate(actors):
        if tensor_backend == "cupy":
            t = cp.ones(array_size, dtype=dtype) * (i + 1)
        elif tensor_backend == "torch":
            t = torch.ones(array_size, dtype=torch.float32).cuda() * (i + 1)
        else:
            raise RuntimeError("Unsupported tensor backend.")
        ray.wait([a.set_buffer.remote(t)])
    if tensor_backend == "cupy":
        list_buffer = [
            cp.ones(array_size, dtype=dtype) for _ in range(world_size)
        ]
    elif tensor_backend == "torch":
        list_buffer = [
            torch.ones(array_size, dtype=torch.float32).cuda()
            for _ in range(world_size)
        ]
    else:
        raise RuntimeError("Unsupported tensor backend.")
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])
