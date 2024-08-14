from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Optional, Tuple

import ray
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch


# Signature for a torch.Tensor allocator is:
# (shape: Tuple[int], dtype: torch.dtype) -> torch.Tensor.
TorchTensorAllocator = Callable[[Tuple[int], "torch.dtype"], "torch.Tensor"]


@DeveloperAPI
class GPUCommunicator(ABC):
    """
    Communicator for a group of aDAG actors on Nvidia GPU.

    The aDAG execution leverages this internally to support communication
    between actors in the group.
    """

    def register(self, group_id: str):
        """
        Register the group in the Ray channel context.

        This should be called once remotely on each actor
        in the group before any other methods can be called,
        with the same `group_id`.
        """
        from ray.experimental.channel.common import ChannelContext

        ctx = ChannelContext.get_current()
        ctx.nccl_groups[group_id] = self

    @abstractmethod
    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        """
        Return the given actor's rank in the group.

        Args:
            actor: The actor handle to look up.
        """
        raise NotImplementedError

    @abstractmethod
    def get_self_rank(self) -> Optional[int]:
        """
        Return this actor's rank.
        """
        raise NotImplementedError

    @abstractmethod
    def send(self, value: "torch.Tensor", peer_rank: int) -> None:
        """
        Send a torch.Tensor to a peer.

        This returns when the send kernel has been queued, but the kernel may
        not have completed. Therefore, the caller should ensure that there are
        no concurrent writes to the sent `value` until the send has finished.
        That is, either all writes should be submitted on the current stream
        (self._cuda_stream) or, if on a different stream, that stream should
        synchronize with the current stream.

        Args:
            value: The torch.Tensor to send. It should already be on this
                actor's default device.
            peer_rank: The rank of the actor to send to.
        """
        raise NotImplementedError

    @abstractmethod
    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ) -> "torch.Tensor":
        """
        Receive a torch.Tensor from a peer and synchronize.

        After this call returns, the receive buffer is safe to read from from
        any stream. An RayChannelError will be raised if an error occurred (e.g.,
        remote actor died), and the buffer is not safe to read.

        Args:
            shape: The shape of the tensor to receive.
            dtype: The dtype of the tensor to receive.
            peer_rank: The rank of the actor to receive from.
            allocator: A function to allocate the tensor to receive into.
        """
        raise NotImplementedError
