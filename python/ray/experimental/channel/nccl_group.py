from typing import Dict, Optional

import ray
from ray.util.collective.collective_group import nccl_util

try:
    import torch
except ImportError:
    torch = None


class _NcclGroup:
    def __init__(
        self,
        world_size: int,
        comm_id: int,
        rank: Optional[int],
        actor_ids_to_ranks: Dict[ray.ActorID, int],
        cuda_stream: Optional[int],
    ):
        self._rank: Optional[int] = rank
        if rank is not None:
            assert ray.get_gpu_ids(), "NCCL actor has no GPUs assigned"

            from cupy.cuda import nccl

            self._comm = nccl.NcclCommunicator(world_size, comm_id, rank)
        else:
            # Driver does not have a rank.
            self._comm = None
        self._actor_ids_to_ranks = actor_ids_to_ranks

        self._cuda_stream = cuda_stream

        self._closed = False

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        if actor._ray_actor_id not in self._actor_ids_to_ranks:
            raise ValueError("Actor is not in the NCCL group.")
        return self._actor_ids_to_ranks[actor._ray_actor_id]

    def get_self_rank(self) -> int:
        return self._rank

    def send(self, value: torch.Tensor, peer_rank: int):
        if self._closed:
            raise IOError("NCCL group has been destroyed.")
        # TODO(swang): Handle send/recv async NCCL errors such as network
        # failures.
        self._comm.send(
            nccl_util.get_tensor_ptr(value),
            value.numel(),
            nccl_util.get_nccl_tensor_dtype(value),
            peer_rank,
            self._cuda_stream,
        )

    def recv(self, buf: torch.Tensor, peer_rank: int):
        if self._closed:
            raise IOError("NCCL group has been destroyed.")
        self._comm.recv(
            nccl_util.get_tensor_ptr(buf),
            buf.numel(),
            nccl_util.get_nccl_tensor_dtype(buf),
            peer_rank,
            self._cuda_stream,
        )

        # Buffer values are undefined if NCCL ops are aborted. Therefore, we
        # need to synchronize here and check that the channel is still open to
        # ensure that the receive buffer is valid.
        # TODO(swang): Avoid CUDA synchronization.
        torch.cuda.synchronize()
        if self._closed:
            raise IOError("NCCL group has been destroyed.")

    def destroy(self):
        if self._closed:
            return

        self._closed = True
        # Abort *after* setting the _closed flag.
        self._comm.abort()
        self._comm.destroy()
