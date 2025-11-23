import logging

import torch
import torch_npu

import ray
import ray.util.collective as col
from ray.util.collective.collective_group.hccl_collective_group import get_num_npus
from ray.util.collective.types import Backend, ReduceOp

import os

logger = logging.getLogger(__name__)


@ray.remote(resources={"NPU":1})
class Worker:
    def __init__(self):
        self.buffer = None
        self.list_buffer = None

    def init_tensors(self):
        self.buffer = torch.ones((10,), dtype=torch.float32).npu()
        self.list_buffer = [torch.ones((10,), dtype=torch.float32).npu() for _ in range(2)]
        return True

    def init_group(self, world_size, rank, backend=Backend.HCCL, group_name="default"):
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
        ws = col.get_collective_group_size(group_name)
        return ws

    def report_hccl_availability(self):
        avail = col.hccl_available()
        return avail

    def report_gloo_availability(self):
        avail = col.gloo_available()
        return avail

    def report_is_group_initialized(self, group_name="default"):
        is_init = col.is_group_initialized(group_name)
        return is_init


def create_collective_workers(num_workers=2, group_name="default", backend="hccl"):
    actors = [None] * num_workers
    for i in range(num_workers):
        actor = Worker.remote()
        ray.get([actor.init_tensors.remote()])
        actors[i] = actor
    world_size = num_workers
    init_results = ray.get(
        [
            actor.init_group.remote(world_size, i, backend, group_name)
            for i, actor in enumerate(actors)
        ]
    )
    return actors, init_results


def init_tensors_for_gather_scatter(
    actors, array_size=10, dtype=torch.float32, tensor_backend="torch"
):
    world_size = len(actors)
    for i, a in enumerate(actors):
        if tensor_backend == "torch":
            t = torch.ones(array_size, dtype=torch.float32).npu() * (i + 1)
        else:
            raise RuntimeError("Unsupported tensor backend.")
        ray.get([a.set_buffer.remote(t)])
    if tensor_backend == "torch":
        list_buffer = [
            torch.ones(array_size, dtype=torch.float32).npu()
            for _ in range(world_size)
        ]
    else:
        raise RuntimeError("Unsupported tensor backend.")
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])


@ray.remote(resources={"NPU":2})
class MultiNPUWorker:
    def __init__(self):
        self.buffer0 = None
        self.buffer1 = None
        self.list_buffer0 = None
        self.list_buffer1 = None

    def __del__(self):
        self.buffer0 = None
        self.buffer1 = None
        self.list_buffer0 = None
        self.list_buffer1 = None

    def init_tensors(self):
        os.environ["ASCEND_RT_VISIBLE_DEVICES"]="0,1"

        self.buffer0 = torch.ones((10,), dtype=torch.float32).npu(0)
        self.list_buffer0 = [torch.ones((10,), dtype=torch.float32).npu() for _ in range(4)]

        self.buffer1 = torch.ones((10,), dtype=torch.float32).npu(1)
        self.list_buffer1 = [torch.ones((10,), dtype=torch.float32).npu() for _ in range(4)]
        return True

    def init_group(self, world_size, rank, backend=Backend.HCCL, group_name="default"):
        col.init_collective_group(world_size, rank, backend, group_name)
        return True

    def set_buffer(
        self,
        size,
        value0=1.0,
        value1=1.0,
        dtype=torch.float32,
        tensor_type0="torch",
        tensor_type1="torch",
    ):
        os.environ["ASCEND_RT_VISIBLE_DEVICES"]="0,1"
        if tensor_type0 == "torch":
            self.buffer0 = torch.ones(size, dtype=torch.float32).npu(0) * value0
        else:
            raise RuntimeError()

        if tensor_type1 == "torch":
            self.buffer1 = torch.ones(size, dtype=torch.float32).npu(1) * value1
        else:
            raise RuntimeError()
        return True

    def set_list_buffer(
        self,
        size,
        value0=1.0,
        value1=1.0,
        dtype=torch.float32,
        tensor_type0="torch",
        tensor_type1="torch",
    ):
        os.environ["ASCEND_RT_VISIBLE_DEVICES"]="0,1"
        if tensor_type0 == "torch":
            self.list_buffer0 = [
                torch.ones(size, dtype=torch.float32).npu(0) * value0 for _ in range(4)
            ]
        else:
            raise RuntimeError()

        if tensor_type1 == "torch":
            self.list_buffer1 = [
                torch.ones(size, dtype=torch.float32).npu(1) * value1 for _ in range(4)
            ]
        else:
            raise RuntimeError()
        return True

    @ray.method(num_returns=2)
    def get_buffer(self):
        return self.buffer0, self.buffer1

    def do_allreduce_multinpu(self, group_name="default", op=ReduceOp.SUM):
        col.allreduce_multigpu([self.buffer0, self.buffer1], group_name, op)
        return self.buffer0

    def do_reduce_multinpu(
        self, group_name="default", dst_rank=0, dst_npu_index=0, op=ReduceOp.SUM
    ):
        col.reduce_multigpu(
            [self.buffer0, self.buffer1], dst_rank, dst_npu_index, group_name, op
        )
        return self.buffer0, self.buffer1

    def do_broadcast_multinpu(self, group_name="default", src_rank=0, src_npu_index=0):
        col.broadcast_multigpu(
            [self.buffer0, self.buffer1], src_rank, src_npu_index, group_name
        )
        return self.buffer0, self.buffer1

    def do_allgather_multinpu(self, group_name="default"):
        col.allgather_multigpu(
            [self.list_buffer0, self.list_buffer1],
            [self.buffer0, self.buffer1],
            group_name,
        )
        return self.list_buffer0, self.list_buffer1

    def do_reducescatter_multinpu(self, group_name="default", op=ReduceOp.SUM):
        col.reducescatter_multigpu(
            [self.buffer0, self.buffer1],
            [self.list_buffer0, self.list_buffer1],
            group_name,
            op,
        )
        return self.buffer0, self.buffer1

    def do_send_multinpu(
        self, group_name="default", dst_rank=0, dst_npu_index=0, src_npu_index=0
    ):
        if src_npu_index == 0:
            col.send_multigpu(self.buffer0, dst_rank, dst_npu_index, group_name)
            return self.buffer0
        elif src_npu_index == 1:
            col.send_multigpu(self.buffer1, dst_rank, dst_npu_index, group_name)
            return self.buffer1
        else:
            raise RuntimeError()

    def do_recv_multinpu(
        self, group_name="default", src_rank=0, src_npu_index=0, dst_npu_index=0
    ):
        if dst_npu_index == 0:
            col.recv_multigpu(self.buffer0, src_rank, src_npu_index, group_name)
            return self.buffer0
        elif dst_npu_index == 1:
            col.recv_multigpu(self.buffer1, src_rank, src_npu_index, group_name)
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
        ws = col.get_collective_group_size(group_name)
        return ws

    def report_hccl_availability(self):
        avail = col.hccl_available()
        return avail

    def report_gloo_availability(self):
        avail = col.gloo_available()
        return avail

    def report_is_group_initialized(self, group_name="default"):
        is_init = col.is_group_initialized(group_name)
        return is_init

    def report_num_npus(self):
        n_npus = get_num_npus()
        return n_npus


def create_collective_multinpu_workers(
    num_workers=2, group_name="default", backend="hccl"
):
    actors = [None] * num_workers
    for i in range(num_workers):
        actor = MultiNPUWorker.remote()
        ray.get([actor.set_buffer.remote([10])])
        ray.get([actor.set_list_buffer.remote([10])])
        actors[i] = actor
    world_size = num_workers
    init_results = ray.get(
        [
            actor.init_group.remote(world_size, i, backend, group_name)
            for i, actor in enumerate(actors)
        ]
    )
    return actors, init_results


def init_tensors_for_gather_scatter_multinpu(
    actors, array_size=10, tensor_backend="torch"
):
    for i, a in enumerate(actors):
        if tensor_backend == "torch":
            ray.get(
                [
                    a.set_buffer.remote(
                        array_size, tensor_type0="torch", tensor_type1="torch"
                    )
                ]
            )
            ray.get(
                [
                    a.set_list_buffer.remote(
                        array_size, tensor_type0="torch", tensor_type1="torch"
                    )
                ]
            )
        else:
            raise RuntimeError("Unsupported tensor backend.")
