import os
import torch
import torch.distributed as dist
import ray

from ray.util.collective.collective_group.base_collective_group import BaseGroup
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
    torch_available,
)


TORCH_REDUCE_OP_MAP = {
    ReduceOp.SUM: dist.ReduceOp.SUM,
    ReduceOp.PRODUCT: dist.ReduceOp.PRODUCT,
    ReduceOp.MIN: dist.ReduceOp.MIN,
    ReduceOp.MAX: dist.ReduceOp.MAX,
}


class TorchGLOOGroup(BaseGroup):
    def __init__(
        self,
        world_size: int,
        rank: int,
        group_name: str,
    ):
        info_actor_name = "info_" + group_name
        try:
            info_actor = ray.get_actor(info_actor_name)
        except ValueError:
            raise RuntimeError(
                f"TorchGLOOGroup expected actor with name `{info_actor_name}` to be created. TorchGLOOGroup should not be instantiated directly. Use ray.experimental.collective.create_collective_group to create the group."
            )

        _, _, _, _, _, metadata = ray.get(info_actor.get_info.remote())
        master_addr, master_port = metadata
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(master_port)

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

    def _check_tensor_input(self, tensor):
        assert isinstance(tensor, list) and len(tensor) == 1
        tensor = tensor[0]
        if not isinstance(tensor, torch.Tensor):
            raise ValueError(
                f"torch_gloo group only accepts torch.Tensor types, received {tensor}"
            )
        return tensor

    def _check_tensor_list_input(self, tensor_list):
        assert isinstance(tensor_list, list) and len(tensor_list) == 1
        tensor_list = tensor_list[0]
        for tensor in tensor_list:
            if not isinstance(tensor, torch.Tensor):
                raise ValueError(
                    f"torch_gloo group only accepts torch.Tensor types, received tensor list with value {tensor}"
                )
        return tensor_list

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        tensor = self._check_tensor_input(tensor)
        torch_reduce_op = TORCH_REDUCE_OP_MAP[allreduce_options.reduceOp]
        dist.all_reduce(tensor, op=torch_reduce_op)

    def barrier(self, barrier_options=BarrierOptions()):
        dist.barrier()

    def reduce(self, tensor, reduce_options=ReduceOptions()):
        tensor = self._check_tensor_input(tensor)
        torch_reduce_op = TORCH_REDUCE_OP_MAP[reduce_options.reduceOp]
        dist.reduce(tensor, dst=reduce_options.root_rank, op=torch_reduce_op)

    def allgather(self, tensor_list, tensor, allgather_options=AllGatherOptions()):
        tensor_list = self._check_tensor_list_input(tensor_list)
        tensor = self._check_tensor_input(tensor)
        dist.all_gather(tensor_list, tensor)

    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        tensor = self._check_tensor_input(tensor)
        dist.broadcast(tensor, src=broadcast_options.root_rank)

    def reducescatter(
        self, output_tensor, tensor_list, reducescatter_options=ReduceScatterOptions()
    ):
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

    def send(self, tensor, send_options: SendOptions):
        tensor = self._check_tensor_input(tensor)
        dist.send(tensor, dst=send_options.dst_rank)

    def recv(self, tensor, recv_options: RecvOptions):
        tensor = self._check_tensor_input(tensor)
        dist.recv(tensor, src=recv_options.src_rank)
