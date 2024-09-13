import logging
from typing import TYPE_CHECKING, List, Optional, Tuple
import os

import ray
from ray.exceptions import RayChannelError
from ray.experimental.channel.gpu_communicator import GPUCommunicator,


if TYPE_CHECKING:
    import torch


logger = logging.getLogger(__name__)


class _HcclGroup(GPUCommunicator):
    """
    Represents an actor's HCCL communicator using NPUs.
    
    This is the default HCCL communicator to be used in aDAG if a custom communicator is not provided.

    This class is not thread-safe.
    """

    def __init__(
        self,
        world_size: int,
        comm_id: int,
        rank: Optional[int],
        actor_handles: List["ray.actor.ActorHandle"],
        device_id: Optional[int],
    ):
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
            device_id: The NPU device id to use for HCCL operations.
        """
        self._world_size = world_size
        self._rank: Optional[int] = rank
        self._actor_handles = actor_handles
        self._device_id = device_id

        if rank is not None:
            assert ray.get_gpu_ids(), "HCCL actor has no NPUs assigned"
            assert device_id is not None, "HCCL actor must specify device_id"

            expected_rank = self.get_rank(ray.get_runtime_context().current_actor)
            assert (
                rank == expected_rank
            ), f"HCCL actor's rank {rank} does not match expected rank {expected_rank}"

            import torch
            import torch_npu
            import torch.distributed as dist

            # Initialize HCCL process group
            os.environ['MASTER_ADDR'] = '127.0.0.1'
            os.environ['MASTER_PORT'] = '29500'
            os.environ['HCCL_WHITELIST_DISABLE'] = '1'
            torch_npu.npu.set_device(device_id)
            dist.init_process_group(backend='hccl', world_size=world_size, rank=rank)

            self._comm = dist
        else:
            self._comm = None

        self._closed = False

    def initialize(self, rank: int) -> None:
        # No additional initialization is needed.
        pass

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        """
        Return the given actor's rank in the HCCL communicator.

        Args:
            actor: The actor handle to look up.
        """
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError:
            raise ValueError("Actor is not in the HCCL group.")
        return rank

    def get_self_rank(self) -> Optional[int]:
        """
        Return this actor's rank.
        """
        return self._rank

    def get_world_size(self) -> int:
        """
        Return the number of ranks in the HCCL communicator.
        """
        return self._world_size

    def send(self, value: "torch.Tensor", peer_rank: int) -> None:
        """
        Send a torch.Tensor to a peer.

        Args:
            value: The torch.Tensor to send. It should already be on this
                actor's NPU device.
            peer_rank: The rank of the actor to send to.
        """
        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")
        
        self._comm.send(tensor=value, dst=peer_rank)

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[Callable[[Tuple[int], "torch.dtype"], "torch.Tensor"]] = None,
    ) -> "torch.Tensor":
        """
        Receive a torch.Tensor from a peer.

        Args:
            shape: The shape of the tensor to receive.
            dtype: The dtype of the tensor to receive.
            peer_rank: The rank of the actor to receive from.
            allocator: A function to allocate the tensor to receive into.
        """
        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")
        assert allocator is not None, "HCCL group requires a tensor allocator"

        # Allocate the receive buffer
        buf = allocator(shape, dtype)
        self._comm.recv(tensor=buf, src=peer_rank)
        return buf

    def destroy(self) -> None:
        """
        Destroy the HCCL group.
        """
        if self._closed:
            return

        self._closed = True

        if self._comm is not None:
            logger.info(
                "Destructing HCCL group on actor: "
                f"{ray.get_runtime_context().current_actor}"
            )
            # Clean up the HCCL process group
            self._comm.destroy_process_group()

