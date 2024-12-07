import logging
from typing import TYPE_CHECKING, Optional, Tuple, List

import ray
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    TorchTensorAllocator,
)

if TYPE_CHECKING:
    import torch

from ray.util.collective.types import Backend
import ray.util.collective as col
from ray.experimental.channel import ChannelContext
import numpy as np

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

class _GlooGroup(GPUCommunicator):
    def __init__(
        self,
        world_size: int,
        rank: int,
        group_name: str,
    ):
        self._group_name = group_name
        self._world_size = world_size
        self._rank = rank
        if rank is not None:
            col.init_collective_group(world_size, rank, Backend.GLOO, self._group_name)
 
    def initialize(self, rank: int) -> None:
        pass

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        return self._rank

    def get_self_rank(self) -> Optional[int]:
        pass

    def get_world_size(self) -> int:
        return self._world_size

    def send(self, buf: "torch.Tensor", peer_rank: int) -> None:
        col.send(buf, peer_rank, self._group_name)

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator: Optional[TorchTensorAllocator] = None
    ):
        buf = np.zeros(shape, dtype=dtype)
        col.recv(buf, peer_rank, self._group_name)
        return buf
    
    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        raise NotImplementedError
    
    @property
    def recv_stream(self) -> Optional["cp.cuda.ExternalStream"]:
        raise NotImplementedError

    @property
    def send_stream(self) -> Optional["cp.cuda.ExternalStream"]:
        raise NotImplementedError

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op,
    ) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        col.destroy_collective_group(self._group_name)


def do_init_gloo_group(self, world_size: int, rank: int, group_name: str) -> _GlooGroup:
    ctx = ChannelContext.get_current()
    print("do_init_gloo_group", group_name, "world_size", world_size, "rank", rank)
    ctx.gloo_groups[group_name] = _GlooGroup(world_size, rank, group_name)

def init_gloo_group(actors: List[ray.actor.ActorHandle]) -> _GlooGroup:
    world_size = len(actors)
    group_name = "default_gloo_group"
    init_tasks = [
        actor.__ray_call__.remote(
            do_init_gloo_group,
            world_size,
            rank,
            group_name,
        )
        for rank, actor in enumerate(actors)
    ]
    ray.get(init_tasks, timeout=30)
    ctx = ChannelContext.get_current()
    ctx.gloo_groups[group_name] = _GlooGroup(world_size, None, group_name)
