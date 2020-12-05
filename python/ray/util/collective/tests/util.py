import cupy as cp

import ray
import ray.util.collective as col
from ray.util.collective.types import Backend, ReduceOp


@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        self.buffer = cp.ones((10, ), dtype=cp.float32)

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

    def do_work(self, group_name="default", op=ReduceOp.SUM):
        col.allreduce(self.buffer, group_name, op)
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

    def report_mpi_availability(self):
        avail = col.mpi_available()
        return avail

    def report_is_group_initialized(self, group_name="default"):
        is_init = col.is_group_initialized(group_name)
        return is_init
