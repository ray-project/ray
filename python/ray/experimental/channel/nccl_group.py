import logging
from types import ModuleType
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.exceptions import RayChannelError

if TYPE_CHECKING:
    import cupy as cp
    import torch


# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


class _NcclGroup:
    """
    Represents an actor's NCCL communicator.

    This class is not thread-safe.
    """

    def __init__(
        self,
        world_size: int,
        comm_id: int,
        rank: Optional[int],
        actor_handles: List["ray.actor.ActorHandle"],
        cuda_stream: Optional[int],
    ):
        """
        Initialize a NCCL communicator that can be used to communicate p2p with
        other GPU actors.

        This method blocks until the same call has been made on all other
        actors in the group, with the same arguments for world_size and
        comm_id.

        NOTE: A concurrent NCCL group can coexist with this one but using the
        two groups concurrently on different CUDA streams may cause deadlock.
        See
        https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/usage/communicators.html
        #using-multiple-nccl-communicators-concurrently.

        If the user can guarantee that all involved actors execute the same ops
        in the same order, then the other NCCL group should use the given
        `cuda_stream`, and there will not be a concurrency issue. Otherwise,
        the other stream needs to synchronize with the given `cuda_stream`
        before and after it launches NCCL ops, e.g., at the beginning and end
        of a DAG task.

        Args:
            world_size: The number of participating actors/devices.
            comm_id: A unique communicator ID returned by
                cupy.cuda.nccl.get_unique_id().
            rank: The rank of this actor. If None, then the caller is not a
                participant of the NCCL group.
            actor_handles: A list of actor handles, in rank order.
            cuda_stream: A raw CUDA stream to dispatch NCCL ops to. If rank is
                specified, then this must be specified too.
        """
        self._rank: Optional[int] = rank
        self.nccl_util: Optional[ModuleType] = None
        self._actor_handles = actor_handles

        if rank is not None:
            assert ray.get_gpu_ids(), "NCCL actor has no GPUs assigned"
            assert cuda_stream is not None, "NCCL actor must specify cuda_stream"

            expected_rank = self.get_rank(ray.get_runtime_context().current_actor)
            assert (
                rank == expected_rank
            ), f"NCCL actor's rank {rank} does not match expected rank {expected_rank}"

            from ray.util.collective.collective_group import nccl_util

            self.nccl_util = nccl_util

            self._comm = self.nccl_util.NcclCommunicator(world_size, comm_id, rank)
        else:
            # Driver does not have a rank.
            self._comm = None

        self._cuda_stream: Optional["cp.cuda.ExternalStream"] = None
        if cuda_stream is not None:
            assert rank is not None, "NCCL actor has no rank assigned"

            import cupy as cp

            from ray.air._internal import torch_utils

            # TODO(swang): Allow default device to be overridden.
            device = torch_utils.get_devices()[0]
            self._cuda_stream = cp.cuda.ExternalStream(
                cuda_stream, device_id=device.index
            )

        self._closed = False

    def _get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        return self._actor_handles

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        """
        Return the given actor's rank in the NCCL communicator.

        Args:
            actor: The actor handle to look up.
        """
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError:
            raise ValueError("Actor is not in the NCCL group.")
        return rank

    def get_self_rank(self) -> Optional[int]:
        """
        Return this actor's rank.
        """
        return self._rank

    def send(self, value: "torch.Tensor", peer_rank: int):
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
        if self._closed:
            raise RayChannelError("NCCL group has been destroyed.")
        # TODO(swang): Handle send/recv async NCCL errors such as network
        # failures.
        self._comm.send(
            self.nccl_util.get_tensor_ptr(value),
            value.numel(),
            self.nccl_util.get_nccl_tensor_dtype(value),
            peer_rank,
            self._cuda_stream.ptr,
        )

    def recv(self, buf: "torch.Tensor", peer_rank: int):
        """
        Receive a torch.Tensor from a peer and synchronize the current stream.

        After this call returns, the receive buffer is safe to read from from
        any stream. An RayChannelError will be raised if an error occurred (e.g.,
        remote actor died), and the buffer is not safe to read.

        Args:
            buf: The torch.Tensor to receive into. This buffer is safe to read
            peer_rank: The rank of the actor to receive from.
        """
        if self._closed:
            raise RayChannelError("NCCL group has been destroyed.")
        self._comm.recv(
            self.nccl_util.get_tensor_ptr(buf),
            buf.numel(),
            self.nccl_util.get_nccl_tensor_dtype(buf),
            peer_rank,
            self._cuda_stream.ptr,
        )

        # Buffer values are undefined if NCCL ops are aborted. Therefore, we
        # need to synchronize here and check that the channel is still open to
        # ensure that the receive buffer is valid.
        # TODO(swang): Avoid CUDA synchronization.
        self._cuda_stream.synchronize()
        if self._closed:
            raise RayChannelError("NCCL group has been destroyed.")

    def destroy(self):
        """
        Destroy the NCCL group.
        """
        if self._closed:
            return

        self._closed = True

        if self._comm is not None:
            logger.info(
                "Destructing NCCL group on actor: "
                f"{ray.get_runtime_context().current_actor}"
            )
            # Abort *after* setting the _closed flag. This ensures that NCCL
            # ops that were blocked on a remote peer will see that the _closed
            # flag is True when they exit from the abort.
            self._comm.abort()
            self._comm.destroy()
