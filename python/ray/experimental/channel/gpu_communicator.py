from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, List, Optional, Tuple

import ray
from ray.experimental.util.types import ReduceOp
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import cupy as cp
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

    @abstractmethod
    def initialize(self, rank: int) -> None:
        """
        Initialize the communicator from the actor.

        This is called once by aDAG on each actor to initialize the communicator,
        before any other methods.

        Args:
            rank: The rank of this actor in the group.
        """
        raise NotImplementedError

    @abstractmethod
    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        """
        Get handles of all actors for this communicator group.
        """
        raise NotImplementedError

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

    def get_world_size(self) -> int:
        """
        Return the number of ranks in the group.
        """
        raise NotImplementedError

    @abstractmethod
    def send(self, value: "torch.Tensor", peer_rank: int) -> None:
        """
        Send a torch.Tensor to a peer.

        This returns when the send kernel has been queued, but the kernel may
        not have completed. Therefore, the caller should ensure that there are
        no concurrent writes to the sent `value` until the send has finished.

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

    @property
    @abstractmethod
    def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
        """
        Return the cuda stream used for receiving tensors.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
        """
        Return the cuda stream used for sending tensors.
        """
        raise NotImplementedError

    @abstractmethod
    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp,
    ) -> None:
        """
        Collectively allreduce the tensor across the group.

        Args:
            send_buf: The input torch.tensor to allreduce. It should already be
                on this actor's default device.
            recv_buf: The output torch.tensor to store the allreduce result.
            op: The reduce operation.
        """
        raise NotImplementedError

    @abstractmethod
    def destroy() -> None:
        """
        Destroy the GPU communicator.

        Any destruction and cleanup for the GPU communicator should be
        done here. Implement as a noop is nothing is needed.
        """
        raise NotImplementedError
