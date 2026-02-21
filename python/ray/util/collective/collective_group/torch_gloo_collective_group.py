import os
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import torch
import torch.distributed as dist

import ray.experimental.internal_kv as internal_kv
from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray.util.collective.types import (
    AllGatherOptions,
    AllReduceOptions,
    Backend,
    BarrierOptions,
    BroadcastOptions,
    RecvOptions,
    ReduceOp,
    ReduceOptions,
    ReduceScatterOptions,
    SendOptions,
)

if TYPE_CHECKING:
    import torch


TORCH_REDUCE_OP_MAP = {
    ReduceOp.SUM: dist.ReduceOp.SUM,
    ReduceOp.PRODUCT: dist.ReduceOp.PRODUCT,
    ReduceOp.MIN: dist.ReduceOp.MIN,
    ReduceOp.MAX: dist.ReduceOp.MAX,
}


def get_master_address_metadata_key(group_name: str):
    return f"collective_group_master_address_{group_name}"


class TorchGLOOGroup(BaseGroup):
    def __init__(
        self,
        world_size: int,
        rank: int,
        group_name: str,
        gloo_timeout: Optional[int] = None,
    ):
        # Initialize the default process group only once per process.
        if not dist.is_initialized():
            metadata_key = get_master_address_metadata_key(group_name)
            try:
                metadata = internal_kv._internal_kv_get(metadata_key)
            except ValueError:
                raise RuntimeError(
                    f"TorchGLOOGroup expected metadata in internal_kv with name `{metadata_key}`. "
                    "TorchGLOOGroup should not be instantiated directly. "
                    "Use ray.experimental.collective.create_collective_group to create the group."
                )
            if metadata is None:
                raise RuntimeError(
                    f"Missing rendezvous metadata for group `{group_name}` under key `{metadata_key}`."
                )
            metadata = metadata.decode()
            master_addr, master_port = metadata.split(":")
            os.environ["MASTER_ADDR"] = master_addr
            os.environ["MASTER_PORT"] = master_port

            dist.init_process_group(
                backend="gloo", init_method="env://", world_size=world_size, rank=rank
            )

        super().__init__(world_size, rank, group_name)

        # Create a subgroup for this logical group. For the default group, use WORLD.
        self._is_default_group = group_name == "default"
        if self._is_default_group:
            self._pg = dist.group.WORLD
        else:
            # All ranks participate in this subgroup with global ranks [0..world_size-1].
            ranks = list(range(world_size))
            self._pg = dist.new_group(ranks=ranks, backend="gloo")

        # Compatibility shim for legacy tests expecting a pygloo context with getTimeout().
        # Store the rendezvous timeout in milliseconds, defaulting to 30000 if unspecified.
        class _GlooCompatContext:
            def __init__(self, timeout_ms: int):
                self._timeout_ms = timeout_ms

            def getTimeout(self) -> int:
                return self._timeout_ms

        self._gloo_context = _GlooCompatContext(
            gloo_timeout if gloo_timeout is not None else 30000
        )

    def destroy_group(self):
        """GC the communicators."""
        # Destroy only the subgroup for non-default groups. Allow default to be torn down explicitly.
        if self._is_default_group:
            # Destroy default process group to allow re-init in tests that recreate the same group.
            dist.destroy_process_group()
        else:
            # Destroy just this subgroup.
            if self._pg is not None:
                dist.destroy_process_group(self._pg)

    @classmethod
    def backend(cls):
        """The backend of this collective group."""
        return Backend.GLOO

    def _check_tensor_input(self, tensor: List["torch.Tensor"]) -> "torch.Tensor":
        """ray.util.collective wraps tensor arguments in a list.
        Accept a single torch.Tensor or numpy.ndarray and unwrap/convert it.
        """
        assert isinstance(tensor, list) and len(tensor) == 1
        t = tensor[0]
        if isinstance(t, torch.Tensor):
            return t
        if isinstance(t, np.ndarray):
            return torch.from_numpy(t)
        raise ValueError(
            f"torch_gloo group only accepts torch.Tensor or numpy.ndarray, received {type(t)}"
        )

    def _check_tensor_list_input(
        self, tensor_list: List[List["torch.Tensor"]]
    ) -> List["torch.Tensor"]:
        """ray.util.collective wraps tensor arguments in a list.
        Accept a single list containing torch.Tensors or numpy.ndarrays and
        unwrap/convert items as needed.
        """
        assert isinstance(tensor_list, list) and len(tensor_list) == 1
        tensor_list = tensor_list[0]
        converted_tensor_list = []
        for tensor in tensor_list:
            if isinstance(tensor, np.ndarray):
                tensor = torch.from_numpy(tensor)
                converted_tensor_list.append(tensor)
            elif isinstance(tensor, torch.Tensor):
                converted_tensor_list.append(tensor)
            else:
                raise ValueError(
                    f"torch_gloo group only accepts torch.Tensor or numpy.ndarray types, received tensor list with value {tensor}"
                )
        return converted_tensor_list

    def allreduce(
        self,
        tensor: List["torch.Tensor"],
        allreduce_options: Optional[AllReduceOptions] = None,
    ) -> None:
        if allreduce_options is None:
            allreduce_options = AllReduceOptions()
        tensor = self._check_tensor_input(tensor)
        torch_reduce_op = TORCH_REDUCE_OP_MAP[allreduce_options.reduceOp]
        dist.all_reduce(tensor, op=torch_reduce_op, group=self._pg)

    def barrier(self, barrier_options=BarrierOptions()) -> None:
        dist.barrier(group=self._pg)

    def reduce(
        self,
        tensor: List["torch.Tensor"],
        reduce_options: Optional[ReduceOptions] = None,
    ) -> None:
        if reduce_options is None:
            reduce_options = ReduceOptions()
        t = self._check_tensor_input(tensor)
        torch_reduce_op = TORCH_REDUCE_OP_MAP[reduce_options.reduceOp]
        # Avoid mutating non-root ranks' user tensors to match util.collective semantics.
        if self._rank == reduce_options.root_rank:
            dist.reduce(
                t, dst=reduce_options.root_rank, op=torch_reduce_op, group=self._pg
            )
        else:
            tmp = t.detach().clone()
            dist.reduce(
                tmp, dst=reduce_options.root_rank, op=torch_reduce_op, group=self._pg
            )

    def allgather(
        self,
        tensor_list: List[List["torch.Tensor"]],
        tensor: List["torch.Tensor"],
        allgather_options: Optional[AllGatherOptions] = None,
    ) -> None:
        if allgather_options is None:
            allgather_options = AllGatherOptions()
        tensor_list = self._check_tensor_list_input(tensor_list)
        tensor = self._check_tensor_input(tensor)
        dist.all_gather(tensor_list, tensor, group=self._pg)

    def broadcast(
        self, tensor: List["torch.Tensor"], broadcast_options=BroadcastOptions()
    ) -> None:
        tensor = self._check_tensor_input(tensor)
        dist.broadcast(tensor, src=broadcast_options.root_rank, group=self._pg)

    def reducescatter(
        self,
        output_tensor: List["torch.Tensor"],
        tensor_list: List[List["torch.Tensor"]],
        reducescatter_options: Optional[ReduceScatterOptions] = None,
    ) -> None:
        if reducescatter_options is None:
            reducescatter_options = ReduceScatterOptions()
        tensor_list = self._check_tensor_list_input(tensor_list)
        output_tensor = self._check_tensor_input(output_tensor)
        if output_tensor.shape != tensor_list[self._rank].shape:
            raise ValueError(
                "Output tensor has wrong shape {output_tensor.shape}, expected {tensor_list[self._rank].shape}"
            )
        torch_reduce_op = TORCH_REDUCE_OP_MAP[reducescatter_options.reduceOp]

        # torch.distributed gloo doesn't support reducescatter. Implement a
        # simple version using allreduce.
        for tensor in tensor_list:
            dist.all_reduce(tensor, op=torch_reduce_op, group=self._pg)

        if output_tensor.data_ptr() != tensor_list[self._rank].data_ptr():
            output_tensor.copy_(tensor_list[self._rank])

    def send(self, tensor: List["torch.Tensor"], send_options: SendOptions) -> None:
        tensor = self._check_tensor_input(tensor)
        dist.send(tensor, dst=send_options.dst_rank)

    def recv(self, tensor: List["torch.Tensor"], recv_options: RecvOptions) -> None:
        tensor = self._check_tensor_input(tensor)
        dist.recv(tensor, src=recv_options.src_rank)
