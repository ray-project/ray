"""XCCL (oneCCL) collective backend for ``ray.util.collective`` on Intel GPUs.

The XPU counterpart of ``NCCLGroup``: one independent ``ProcessGroup`` wrapping
``ProcessGroupXCCL``, built over its own TCPStore whose address is published
through Ray's internal KV. It never touches ``torch.distributed``'s default
group, so it coexists with whatever world a trainer already initialised. A Ray
actor is one process bound to one XPU, so none of NCCL's per-device bookkeeping
is needed (and ``*_multigpu`` calls are not supported).
"""

import logging
import socket
import time
from datetime import timedelta
from typing import List, Optional

import torch
import torch._C._distributed_c10d as c10d
import torch.distributed as dist

import ray
import ray.experimental.internal_kv as internal_kv
from ray._common.network_utils import find_free_port, is_ipv6
from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray.util.collective.types import (
    AllGatherOptions,
    AllReduceOptions,
    BarrierOptions,
    BroadcastOptions,
    RecvOptions,
    ReduceOp,
    ReduceOptions,
    ReduceScatterOptions,
    SendOptions,
)

logger = logging.getLogger(__name__)

_REDUCE_OP = {
    ReduceOp.SUM: dist.ReduceOp.SUM,
    ReduceOp.PRODUCT: dist.ReduceOp.PRODUCT,
    ReduceOp.MIN: dist.ReduceOp.MIN,
    ReduceOp.MAX: dist.ReduceOp.MAX,
}

_DEFAULT_TIMEOUT_S = 300
_CONNECT_TIMEOUT_S = 5


def _store_key(group_name: str) -> str:
    """Internal-KV key for a group's rendezvous address.

    The internal KV is shared by the whole cluster, so the Ray namespace is part
    of the key: same-named groups in different namespaces must not see (or
    delete) each other's address. The namespace is length-prefixed so that
    (namespace, group) pairs map to keys one-to-one even when either contains
    ``:``.
    """
    namespace = ray.get_runtime_context().namespace
    return f"xccl_collective_store:{len(namespace)}:{namespace}:{group_name}"


def _xpu_available() -> bool:
    return hasattr(torch, "xpu") and torch.xpu.is_available()


def _xccl_compiled() -> bool:
    """Whether this torch build ships ``ProcessGroupXCCL``."""
    return dist.is_available() and hasattr(c10d, "ProcessGroupXCCL")


def _unwrap(tensor: List[torch.Tensor]) -> torch.Tensor:
    """ray.util.collective hands tensors over as a one-element list."""
    assert isinstance(tensor, list) and len(tensor) == 1
    t = tensor[0]
    if not isinstance(t, torch.Tensor):
        raise ValueError(f"XCCL group only accepts torch.Tensor, got {type(t)}")
    return t


def _unwrap_list(tensor_list: List[List[torch.Tensor]]) -> List[torch.Tensor]:
    assert isinstance(tensor_list, list) and len(tensor_list) == 1
    return list(tensor_list[0])


class XCCLGroup(BaseGroup):
    # Overridable so the Ray glue can be tested on a machine without an XPU.
    _device_type = "xpu"

    def __init__(self, world_size: int, rank: int, group_name: str):
        super().__init__(world_size, rank, group_name)
        if self._device_type == "xpu" and not _xpu_available():
            raise RuntimeError(
                f"XCCL group '{group_name}' needs an XPU on this worker, but "
                "torch.xpu.is_available() is False."
            )
        self._timeout = timedelta(seconds=_DEFAULT_TIMEOUT_S)
        self._store = self._rendezvous(group_name, rank, world_size)
        self._pg = self._build_process_group(self._store, rank, world_size)

    # ------------------------------------------------------------ setup

    def _rendezvous(self, group_name: str, rank: int, world_size: int):
        """Rank 0 owns a TCPStore and publishes its address via Ray's KV."""
        key = _store_key(group_name)
        if rank == 0:
            addr = ray.util.get_node_ip_address()
            family = socket.AF_INET6 if is_ipv6(addr) else socket.AF_INET
            port = find_free_port(family)
            store = dist.TCPStore(
                addr,
                port,
                world_size,
                is_master=True,
                timeout=self._timeout,
                wait_for_workers=False,
            )
            internal_kv._internal_kv_put(key, f"{addr}:{port}", overwrite=True)
            return store

        # The KV key is cluster-global: a group that was never destroyed can leave
        # a dead address behind. Retry with a short connect timeout until rank 0
        # overwrites it with the live one.
        deadline = time.time() + self._timeout.total_seconds()
        last_err = None
        while time.time() < deadline:
            meta = internal_kv._internal_kv_get(key)
            if meta is not None:
                addr, port = meta.decode().rsplit(":", 1)
                try:
                    store = dist.TCPStore(
                        addr,
                        int(port),
                        world_size,
                        is_master=False,
                        timeout=timedelta(seconds=_CONNECT_TIMEOUT_S),
                    )
                except (RuntimeError, OSError) as e:
                    last_err = e
                else:
                    # The short timeout only bounds the connect attempt. Ranks
                    # can reach their first collective far apart (e.g. a
                    # trainer waiting for weights), and the lazy communicator
                    # setup waits on this store, so restore the full timeout.
                    store.set_timeout(self._timeout)
                    return store
            time.sleep(0.1)
        raise TimeoutError(
            f"XCCL rendezvous timed out for group '{group_name}': {last_err}"
        )

    def _build_process_group(self, store, rank: int, world_size: int):
        prefix_store = dist.PrefixStore(f"ray_xccl_{self.group_name}", store)
        pg = dist.ProcessGroup(prefix_store, rank, world_size)
        from torch._C._distributed_c10d import ProcessGroupXCCL

        backend = ProcessGroupXCCL(
            prefix_store, rank, world_size, ProcessGroupXCCL.Options()
        )
        pg._set_default_backend(dist.ProcessGroup.BackendType.XCCL)
        pg._register_backend(
            torch.device(self._device_type), dist.ProcessGroup.BackendType.XCCL, backend
        )
        return pg

    def destroy_group(self):
        if self._rank == 0:
            try:
                internal_kv._internal_kv_del(_store_key(self._group_name))
            except Exception as e:  # key may already be gone
                logger.debug("XCCL rendezvous key cleanup failed: %s", e)
        pg, self._pg, self._store = self._pg, None, None
        shutdown = getattr(pg, "shutdown", None)
        if shutdown is not None:
            try:
                shutdown()
            except Exception as e:
                logger.debug("XCCL process group shutdown failed: %s", e)

    @classmethod
    def backend(cls):
        return "XCCL"

    @classmethod
    def check_backend_availability(cls) -> bool:
        # Only whether this torch build can run XCCL. The driver that creates a
        # declarative group may have no XPU of its own; each worker checks for
        # its XPU when it joins the group.
        return _xccl_compiled()

    # ------------------------------------------------------- collectives

    def allreduce(self, tensor, allreduce_options: Optional[AllReduceOptions] = None):
        opts = c10d.AllreduceOptions()
        opts.reduceOp = _REDUCE_OP[(allreduce_options or AllReduceOptions).reduceOp]
        self._pg.allreduce([_unwrap(tensor)], opts).wait()

    def barrier(self, barrier_options: Optional[BarrierOptions] = None):
        try:
            self._pg.barrier().wait()
        except (AttributeError, NotImplementedError):
            # Older torch/oneCCL: no native barrier on ProcessGroupXCCL.
            # Real failures (RuntimeError) propagate instead of being masked.
            t = torch.ones(1, dtype=torch.int32, device=self._device_type)
            self._pg.allreduce([t], c10d.AllreduceOptions()).wait()

    def reduce(self, tensor, reduce_options: Optional[ReduceOptions] = None):
        o = reduce_options or ReduceOptions
        opts = c10d.ReduceOptions()
        opts.reduceOp = _REDUCE_OP[o.reduceOp]
        opts.rootRank = o.root_rank
        t = _unwrap(tensor)
        # Keep the caller's tensor intact on non-root ranks (matches NCCLGroup).
        if self._rank != o.root_rank:
            t = t.detach().clone()
        self._pg.reduce([t], opts).wait()

    def broadcast(self, tensor, broadcast_options: Optional[BroadcastOptions] = None):
        opts = c10d.BroadcastOptions()
        opts.rootRank = (broadcast_options or BroadcastOptions).root_rank
        self._pg.broadcast([_unwrap(tensor)], opts).wait()

    def allgather(
        self, tensor_list, tensor, allgather_options: Optional[AllGatherOptions] = None
    ):
        t = _unwrap(tensor)
        outs = _unwrap_list(tensor_list)
        self._check_gather_scatter(outs, t, "allgather")
        # c10d allgather takes a list of output lists, one per input tensor.
        self._pg.allgather(
            [_unwrap_list(tensor_list)], [_unwrap(tensor)], c10d.AllgatherOptions()
        ).wait()

    def reducescatter(
        self,
        output_tensor,
        tensor_list,
        reducescatter_options: Optional[ReduceScatterOptions] = None,
    ):
        out = _unwrap(output_tensor)
        ins = _unwrap_list(tensor_list)
        self._check_gather_scatter(ins, out, "reducescatter")
        opts = c10d.ReduceScatterOptions()
        opts.reduceOp = _REDUCE_OP[
            (reducescatter_options or ReduceScatterOptions).reduceOp
        ]
        self._pg.reduce_scatter([out], [ins], opts).wait()

    def _check_gather_scatter(
        self, tensors: List[torch.Tensor], single: torch.Tensor, op: str
    ):
        """Same contract as NCCLGroup: world_size-long list, matching shape/dtype/device."""
        if len(tensors) != self._world_size:
            raise RuntimeError(
                f"{op}: tensor list has {len(tensors)} entries, expected world_size={self._world_size}"
            )
        for x in tensors:
            if (
                x.shape != single.shape
                or x.dtype != single.dtype
                or x.device != single.device
            ):
                raise RuntimeError(
                    f"{op}: tensor mismatch, list item {tuple(x.shape)}/{x.dtype}/{x.device} "
                    f"vs {tuple(single.shape)}/{single.dtype}/{single.device}"
                )

    def send(self, tensor, send_options: SendOptions):
        self._pg.send([_unwrap(tensor)], send_options.dst_rank, 0).wait()

    def recv(self, tensor, recv_options: RecvOptions):
        self._pg.recv([_unwrap(tensor)], recv_options.src_rank, 0).wait()
