from typing import TYPE_CHECKING, List, Optional
import os
import torch
import torch.distributed as dist

import ray.experimental.internal_kv as internal_kv
from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray._common.network_utils import parse_address
from ray.util.collective.types import (
    AllReduceOptions,
    BarrierOptions,
    Backend,
    ReduceOp,
    ReduceOptions,
    BroadcastOptions,
    AllGatherOptions,
    ReduceScatterOptions,
    SendOptions,
    RecvOptions,
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
    ):
        metadata_key = get_master_address_metadata_key(group_name)
        try:
            metadata = internal_kv._internal_kv_get(metadata_key)
        except ValueError:
            raise RuntimeError(
                f"TorchGLOOGroup expected metadata in internal_kv with name `{metadata_key}`. "
                "TorchGLOOGroup should not be instantiated directly. "
                "Use ray.experimental.collective.create_collective_group to create the group."
            )

        metadata = metadata.decode()
        master_addr, master_port = parse_address(metadata)
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = master_port

        dist.init_process_group(
            backend="gloo", init_method="env://", world_size=world_size, rank=rank
        )
        super().__init__(world_size, rank, group_name)

    def destroy_group(self):
        """GC the communicators."""
        dist.destroy_process_group()

    @classmethod
    def backend(cls):
        """The backend of this collective group."""
        return Backend.TORCH_GLOO

    def _check_tensor_input(self, tensor: List["torch.Tensor"]) -> "torch.Tensor":
        """ray.util.collective wraps tensor arguments in a list. Check for a
        single tensor and unwrap it.
        """
        assert isinstance(tensor, list) and len(tensor) == 1
        tensor = tensor[0]
        if not isinstance(tensor, torch.Tensor):
            raise ValueError(
                f"torch_gloo group only accepts torch.Tensor types, received {tensor}"
            )
        return tensor

    def _check_tensor_list_input(
        self, tensor_list: List[List["torch.Tensor"]]
    ) -> List["torch.Tensor"]:
        """ray.util.collective wraps tensor arguments in a list. Check for a
        single list of tensors and unwrap it.
        """
        assert isinstance(tensor_list, list) and len(tensor_list) == 1
        tensor_list = tensor_list[0]
        for tensor in tensor_list:
            if not isinstance(tensor, torch.Tensor):
                raise ValueError(
                    f"torch_gloo group only accepts torch.Tensor types, received tensor list with value {tensor}"
                )
        return tensor_list

    def allreduce(
        self,
        tensor: List["torch.Tensor"],
        allreduce_options: Optional[AllReduceOptions] = None,
    ) -> None:
        if allreduce_options is None:
            allreduce_options = AllReduceOptions()
        tensor = self._check_tensor_input(tensor)
        torch_reduce_op = TORCH_REDUCE_OP_MAP[allreduce_options.reduceOp]
        dist.all_reduce(tensor, op=torch_reduce_op)

    def barrier(self, barrier_options=BarrierOptions()) -> None:
        dist.barrier()

    def reduce(
        self,
        tensor: List["torch.Tensor"],
        reduce_options: Optional[ReduceOptions] = None,
    ) -> None:
        if reduce_options is None:
            reduce_options = ReduceOptions()
        tensor = self._check_tensor_input(tensor)
        torch_reduce_op = TORCH_REDUCE_OP_MAP[reduce_options.reduceOp]
        dist.reduce(tensor, dst=reduce_options.root_rank, op=torch_reduce_op)

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
        dist.all_gather(tensor_list, tensor)

    def broadcast(
        self, tensor: List["torch.Tensor"], broadcast_options=BroadcastOptions()
    ) -> None:
        tensor = self._check_tensor_input(tensor)
        dist.broadcast(tensor, src=broadcast_options.root_rank)

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
            dist.all_reduce(tensor, op=torch_reduce_op)

        if output_tensor.data_ptr() != tensor_list[self._rank].data_ptr():
            output_tensor.copy_(tensor_list[self._rank])

    def send(self, tensor: List["torch.Tensor"], send_options: SendOptions) -> None:
        tensor = self._check_tensor_input(tensor)
        dist.send(tensor, dst=send_options.dst_rank)

    def recv(self, tensor: List["torch.Tensor"], recv_options: RecvOptions) -> None:
        tensor = self._check_tensor_input(tensor)
        dist.recv(tensor, src=recv_options.src_rank)
