from typing import TYPE_CHECKING, List, Optional, Tuple
import ray
from ray.experimental.channel.communicator import Communicator, TorchTensorAllocator
from ray.experimental.util.types import ReduceOp

if TYPE_CHECKING:
    import torch


class CommunicatorInfoHolder(Communicator):
    """
    The driver maintains a communicator instance but does not participate
    in the group. However, it still needs to store certain information,
    such as world size and actor details.

    Since the driver may not have a GPU (or other accelerators), it could
    mistakenly select the wrong `TorchDeviceManager`
    (e.g., `CPUTorchDeviceManager`), causing `get_communicator`
    to return `None`.

    Because the driver does not actively join the group and only uses the
    communicator for metadata storage, Introduced a hardware-independent
    class specifically for the driver.
    """

    def __init__(
        self,
        world_size: int,
        comm_id: int,
        actor_handles: List["ray.actor.ActorHandle"],
    ):
        """
        Only need to store the actor handles, since we don't need
        to initialize the communicator for Driver.
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
