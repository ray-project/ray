"""Helpers for the XCCL collective tests (port of ray.util.collective.tests.util).

Differences from upstream are mechanical: cupy -> torch tensors on ``xpu``,
backend ``nccl`` -> ``XCCL``, no multi-GPU worker.  The driver process may have
no XPU, so tensors are always created on CPU in the driver and moved to the
device inside the actor, and actors hand results back as CPU tensors.
"""
import logging
import os

import pytest
import torch

import ray
import ray.util.collective as col
from ray.util.collective.types import ReduceOp

logger = logging.getLogger(__name__)


BACKEND = "XCCL"
DEVICE = "xpu"


def _env_int_list(name, default):
    raw = os.environ.get(name, default)
    return [int(x) for x in raw.split(",") if x.strip()]


WORLD_SIZES = _env_int_list("XCCL_WORLD_SIZES", "2,3,4")
SCALE_SIZES = _env_int_list("XCCL_SCALE_SIZES", "8,16,32")


def register_backend():
    """XCCL is registered by ray.util.collective itself; kept so tests read the same."""


def make_runtime_env():
    return None


def available_gpus():
    return int(ray.cluster_resources().get("GPU", 0))


def require_gpus(n):
    have = available_gpus()
    if have < n:
        pytest.skip(f"needs {n} GPUs, cluster has {have}")


def cluster_world_size(wanted=4):
    """min(wanted, GPUs in the cluster); skips if fewer than 2 are available."""
    ws = min(wanted, available_gpus())
    if ws < 2:
        pytest.skip(f"needs >= 2 GPUs, cluster has {available_gpus()}")
    return ws


def _sync():
    if hasattr(torch, "xpu") and torch.xpu.is_available():
        torch.xpu.synchronize()


def _cpu(x):
    _sync()
    if isinstance(x, (list, tuple)):
        return [_cpu(t) for t in x]
    return x.detach().cpu()


@ray.remote(num_gpus=1)
class Worker:
    def __init__(self):
        self.buffer = None
        self.list_buffer = None

    def init_tensors(self):
        self.buffer = torch.ones((10,), dtype=torch.float32, device=DEVICE)
        self.list_buffer = [
            torch.ones((10,), dtype=torch.float32, device=DEVICE) for _ in range(2)
        ]
        _sync()
        return True

    def init_group(self, world_size, rank, backend=BACKEND, group_name="default"):
        register_backend()
        col.init_collective_group(world_size, rank, backend, group_name)
        return True

    def set_buffer(self, data, device=DEVICE):
        self.buffer = data.to(device)
        return True

    def get_buffer(self):
        return _cpu(self.buffer)

    def set_list_buffer(self, list_of_arrays, device=DEVICE):
        self.list_buffer = [t.to(device) for t in list_of_arrays]
        return True

    def do_allreduce(self, group_name="default", op=ReduceOp.SUM):
        col.allreduce(self.buffer, group_name, op)
        return _cpu(self.buffer)

    def do_reduce(self, group_name="default", dst_rank=0, op=ReduceOp.SUM):
        col.reduce(self.buffer, dst_rank, group_name, op)
        return _cpu(self.buffer)

    def do_broadcast(self, group_name="default", src_rank=0):
        col.broadcast(self.buffer, src_rank, group_name)
        return _cpu(self.buffer)

    def do_allgather(self, group_name="default"):
        col.allgather(self.list_buffer, self.buffer, group_name)
        return _cpu(self.list_buffer)

    def do_reducescatter(self, group_name="default", op=ReduceOp.SUM):
        col.reducescatter(self.buffer, self.list_buffer, group_name, op)
        return _cpu(self.buffer)

    def do_send(self, group_name="default", dst_rank=0):
        col.send(self.buffer, dst_rank, group_name)
        return _cpu(self.buffer)

    def do_recv(self, group_name="default", src_rank=0):
        col.recv(self.buffer, src_rank, group_name)
        return _cpu(self.buffer)

    def destroy_group(self, group_name="default"):
        col.destroy_collective_group(group_name)
        return True

    def report_rank(self, group_name="default"):
        return col.get_rank(group_name)

    def report_world_size(self, group_name="default"):
        return col.get_collective_group_size(group_name)

    def report_xccl_availability(self):
        register_backend()
        return col.is_backend_available(BACKEND)

    def report_is_group_initialized(self, group_name="default"):
        return col.is_group_initialized(group_name)


@ray.remote(num_gpus=1)
class ScaleWorker:
    """One rank of the scale test; runs every collective and self-checks."""

    def init_group(self, world_size, rank, group_name="scale"):
        register_backend()
        self.ws, self.rank, self.group = world_size, rank, group_name
        col.init_collective_group(world_size, rank, BACKEND, group_name)
        return True

    def _t(self, value, n=16):
        return torch.full((n,), float(value), dtype=torch.float32, device=DEVICE)

    def run_all(self):
        ws, r, g = self.ws, self.rank, self.group
        out = {}

        t = self._t(r + 1)
        col.allreduce(t, g)
        out["allreduce"] = bool((_cpu(t) == ws * (ws + 1) / 2).all())

        root = ws // 2  # non-zero root
        t = self._t(r + 1)
        col.broadcast(t, root, g)
        out["broadcast"] = bool((_cpu(t) == root + 1).all())

        outs = [self._t(-1) for _ in range(ws)]
        col.allgather(outs, self._t(r), g)
        out["allgather"] = all(bool((_cpu(o) == j).all()) for j, o in enumerate(outs))

        # rank s contributes (s + 1 + i) as chunk i; rank r receives sum_s(s+1+r)
        ins = [self._t(r + 1 + i) for i in range(ws)]
        o = self._t(-1)
        col.reducescatter(o, ins, g)
        out["reducescatter"] = bool((_cpu(o) == ws * (ws + 1) / 2 + ws * r).all())

        col.barrier(g)
        out["barrier"] = True

        nxt, prv = (r + 1) % ws, (r - 1) % ws
        sbuf, rbuf = self._t(r + 1), self._t(-1)
        if r % 2 == 0:
            col.send(sbuf, nxt, g)
            col.recv(rbuf, prv, g)
        else:
            col.recv(rbuf, prv, g)
            col.send(sbuf, nxt, g)
        out["ring"] = bool((_cpu(rbuf) == prv + 1).all())
        return out

    def destroy_group(self):
        col.destroy_collective_group(self.group)
        return True


def create_collective_workers(num_workers=2, group_name="default", backend=BACKEND):
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


def init_tensors_for_gather_scatter(actors, array_size=10, dtype=torch.float32):
    world_size = len(actors)
    for i, a in enumerate(actors):
        t = torch.ones(array_size, dtype=dtype) * (i + 1)
        ray.get([a.set_buffer.remote(t)])
    list_buffer = [torch.ones(array_size, dtype=dtype) for _ in range(world_size)]
    ray.get([a.set_list_buffer.remote(list_buffer) for a in actors])
