from typing import TYPE_CHECKING, List, Optional, Tuple
import ray
from ray.experimental.channel.communicator import Communicator, TorchTensorAllocator
from ray.experimental.util.types import ReduceOp

if TYPE_CHECKING:
    import torch


class CommunicatorHandle(Communicator):
    """
    A lightweight communicator handle used to store world size and actors
    without providing actual communication capabilities.

    This class is mainly used on the driver side where no real communicator
    initialization or communication operations are needed. Most communication
    methods are not implemented and will raise NotImplementedError if called.
    """

    def __init__(
        self,
        world_size: int,
        actor_handles: List["ray.actor.ActorHandle"],
    ):
        """
        Initializes the CommunicatorHandle with the given world size and actor handles.

        Args:
            world_size: The number of actors participating in the communicator.
            actor_handles: A list of actor handles to be stored.
        """
        self._world_size = world_size
        self._actor_handles = actor_handles

    def initialize(self, rank: int) -> None:
        # No additional initialization is needed.
        pass

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        """
        Retuan all actor handles in this communicator.
        """
        return self._actor_handles

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        return None

    def get_self_rank(self) -> Optional[int]:
        return None

    def send(self, tensor: "torch.Tensor", peer_rank: int):
        raise NotImplementedError

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None,
    ):
        raise NotImplementedError

    def allgather(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
    ) -> None:
        raise NotImplementedError

    def reducescatter(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp,
    ) -> None:
        raise NotImplementedError

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp = ReduceOp.SUM,
    ):
        raise NotImplementedError

    def destroy(self) -> None:
        pass

    def get_world_size(self) -> int:
        """
        Return the number of ranks in the communicator.
        """
        return self._world_size

    def get_transport_name(self) -> str:
        raise NotImplementedError

    def recv_stream(self):
        raise NotImplementedError

    def send_stream(self):
        raise NotImplementedError

    @classmethod
    def generate_communicator_id(cls) -> str:
        raise NotImplementedError
