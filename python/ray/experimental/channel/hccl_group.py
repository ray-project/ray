import logging
import os
from typing import Optional

import torch
import torch.distributed as dist
import torch_npu  # The torch_npu for communicate

import ray
from ray.exceptions import RayChannelError
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    TorchTensorAllocator,
)
from ray.experimental.util.types import ReduceOp

# Set ASCEND_RT_VISIBLE_DEVICES environment variable to ensure all NPUs are visible
# This enables NPU to NPU communication across devices.
# Explaination: Since currently the worker can only see the GPU/NPU asign to
# that worker, the NPU needs to see all NPUs to enable the communication channel.
os.environ["ASCEND_RT_VISIBLE_DEVICES"] = "0,1,2,3,4,5,6,7"

logger = logging.getLogger(__name__)


class _HcclGroup(GPUCommunicator):
    """
    Represents an actor's HCCL communicator using NPUs.

    This is the default HCCL communicator to be used in aDAG if a
    custom communicator is not provided.

    This class is not thread-safe.
    """

    def __init__(
        self,
        world_size: int,
        comm_id: int,
        rank: int,
        actor_handles: list,
        cuda_stream: Optional[int],
    ):
        # TODO(zhilong): Change cuda_stream to more general name like "stream".
        """
        Initialize an HCCL communicator that can be used to communicate p2p with
        other NPU actors.

        This method blocks until the same call has been made on all other
        actors in the group, with the same arguments for world_size and comm_id.

        Args:
            world_size: The number of participating actors/devices.
            comm_id: A unique communicator ID.
            rank: The rank of this actor. If None, then the caller is not a
                participant of the HCCL group.
            actor_handles: A list of actor handles, in rank order.
            cuda_stream: Not used here but to keep same agrs with nccl_group.
        """
        self._world_size = world_size
        self._comm_id = comm_id
        self._rank = rank
        self._actor_handles = actor_handles
        self._closed = False
        # Initialize distributed HCCL communication if rank is provided
        if rank is not None:
            self._init_dist_hccl(rank, world_size)

    def _init_dist_hccl(self, rank, world_size):
        """
        Initialize the HCCL communication group on NPUs.

        Args:
            rank: The rank of the current process.
            world_size: The total number of processes participating
                in the communication.
        """
        # Set environment variables if not already set
        os.environ["MASTER_ADDR"] = os.environ.get("MASTER_ADDR", "127.0.0.1")
        os.environ["MASTER_PORT"] = os.environ.get("MASTER_PORT", "29500")
        os.environ["HCCL_WHITELIST_DISABLE"] = os.environ.get(
            "HCCL_WHITELIST_DISABLE", "1"
        )

        torch_npu.npu.set_device(rank)  # Set the NPU device according to the rank
        self.ctx = dist.init_process_group(
            backend="hccl", world_size=world_size, rank=rank
        )

    def initialize(self, rank: int) -> None:
        pass  # No additional initialization needed for HCCL group

    def get_actor_handles(self) -> list:
        """
        Return the list of actor handles.

        Returns:
            list: Actor handles in rank order.
        """
        return self._actor_handles

    def get_rank(self, actor: "ray.actor.ActorHandle") -> int:
        """
        Return the given actor's rank in the HCCL communicator.

        Args:
            actor: The actor handle to look up.

        Returns:
            int: The rank of the actor.
        """
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError:
            raise ValueError("Actor is not in the HCCL group.")
        return rank

    def get_self_rank(self) -> int:
        """
        Return this actor's rank.

        Returns:
            int: The rank of this actor in the HCCL group.
        """
        return self._rank

    def get_world_size(self) -> int:
        """
        Return the number of ranks in the HCCL communicator.

        Returns:
            int: The world size of the HCCL group.
        """
        return self._world_size

    def send(self, tensor: "torch.Tensor", peer_rank: int) -> None:
        """
        Send a tensor to a peer using HCCL.

        Args:
            tensor: The tensor to be sent.
            peer_rank: The rank of the peer to send the tensor to.
        """
        print("send")
        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")
        logger.info(f"start to send to:{peer_rank},self._rank : {self._rank} ")
        if self._closed:
            raise RuntimeError("HCCL group has been destroyed.")
        dist.send(tensor, dst=peer_rank)
        logger.info(f"finishe send to dist {peer_rank}")

    def recv(
        self,
        shape: tuple,
        dtype: "torch.dtype",
        peer_rank: int,
        allocator=Optional[TorchTensorAllocator],
    ) -> "torch.Tensor":
        """
        Receive a tensor from a peer using HCCL.

        Args:
            shape: The shape of the tensor to receive.
            dtype: The data type of the tensor.
            peer_rank: The rank of the peer to receive the tensor from.
            allocator: Optional allocator to allocate memory for the tensor.

        Returns:
            torch.Tensor: The received tensor.
        """
        if self._closed:
            raise RuntimeError("HCCL group has been destroyed.")
        torch_npu.npu.set_device(f"npu:{self._rank}")
        tensor = torch.zeros(*shape, dtype=dtype).to(f"npu:{self._rank}")
        dist.recv(tensor, src=peer_rank)
        # torch.npu.synchronize(self._rank)
        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")
        return tensor

    def recv_stream(self):
        pass

    def send_stream(self):
        pass

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp,
    ) -> None:
        pass

    def destroy(self) -> None:
        """
        Destroy the HCCL group and clean up resources.
        """
        if self._closed:
            return
        self._closed = True
        dist.destroy_process_group()
        if self._rank is not None:
            logger.info(
                "Destructing HCCL group on actor: "
                f"{ray.get_runtime_context().current_actor}"
            )
