import logging
from types import ModuleType
from typing import TYPE_CHECKING, List, Optional, Tuple, Any

import ray
from ray.exceptions import RayChannelError
from ray.experimental.channel.communicator import Communicator, TorchTensorAllocator
from ray.experimental.util.types import ReduceOp

if TYPE_CHECKING:
    import torch
    import torch_npu  # noqa: F401

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def set_default_device():
    """
    Set the default NPU device for PyTorch.

    This function initializes the NPU device by selecting the first available device
    from `torch_utils.get_devices()`. It then sets the selected device as the default
    NPU device using `torch.npu.set_device(device)`.

    Returns:
        torch.device: The selected NPU device.
    """
    import torch
    import torch_npu  # noqa: F401
    from ray.air._internal import torch_utils

    device = torch_utils.get_devices()[0]
    torch.npu.set_device(device)
    return device


class StreamContext:
    """
    Context-manager that selects a given stream.

    Stream context, in which torch.npu.current_stream will be set to the value
    in the context. This class comes from pytorch because torch for NPU does
    not implement this context

    Args:
        stream: selected stream. This context manager is a no-op if
        it's ``None``.
    note:: streams are per-device.
    """

    cur_stream: Optional["torch.npu.Stream"]

    def __init__(self, stream: Optional["torch.npu.Stream"]):
        import torch
        import torch_npu  # noqa: F401

        self.stream = stream
        self.idx = torch.npu.current_device()
        if self.idx is None:
            self.idx = -1

        self.src_prev_stream: Optional["torch.npu.Stream"] = None
        self.dst_prev_stream: Optional["torch.npu.Stream"] = None

    def __enter__(self):
        import torch
        import torch_npu  # noqa: F401

        # Local cur_stream variable for type refinement
        cur_stream = self.stream
        # Return if stream is None or npu device not available
        if cur_stream is None or self.idx == -1:
            return
        self.src_prev_stream = torch.npu.current_stream(None)

        # If the stream is not on the current device, then
        # set the current stream on the device
        if self.src_prev_stream.device != cur_stream.device:
            with torch.npu.device(cur_stream.device):
                self.dst_prev_stream = torch.npu.current_stream(cur_stream.device)
        torch.npu.set_stream(cur_stream)

    def __exit__(self, type: Any, value: Any, traceback: Any):
        import torch
        import torch_npu  # noqa: F401

        # Local cur_stream variable for type refinement
        cur_stream = self.stream
        # If stream is None or no npu device available, return
        if cur_stream is None or self.idx == -1:
            return

        # Reset the stream on the original device
        # and destination device
        if self.src_prev_stream.device != cur_stream.device:  # type: ignore[union-attr]
            torch.npu.set_stream(self.dst_prev_stream)  # type: ignore[arg-type]
        torch.npu.set_stream(self.src_prev_stream)  # type: ignore[arg-type]


class _HcclGroup(Communicator):
    """
    Represents an actor's HCCL communicator. This is the default HCCL communicator
    to be used in Compiled Graph if a custom communicator is not provided.

    This class is not thread-safe.
    """

    def __init__(
        self,
        world_size: int,
        comm_id: int,
        rank: Optional[int],
        actor_handles: List["ray.actor.ActorHandle"],
        npu_stream: Optional["torch.npu.Stream"],
        use_communication_streams: bool = False,
    ):
        """
        Initialize a HCCL communicator that can be used to communicate p2p with
        other NPU actors.

        This method blocks until the same call has been made on all other
        actors in the group, with the same arguments for world_size and
        comm_id.

        If the user can guarantee that all involved actors execute the same ops
        in the same order, then the other HCCL group should use the given
        `npu_stream`, and there will not be a concurrency issue. Otherwise,
        the other stream needs to synchronize with the given `npu_stream`
        before and after it launches HCCL ops, e.g., at the beginning and end
        of a DAG task.

        Args:
            world_size: The number of participating actors/devices.
            comm_id: A unique communicator ID returned by
                hccl.get_unique_id().
            rank: The rank of this actor. If None, then the caller is not a
                participant of the HCCL group.
            actor_handles: A list of actor handles, in rank order.
            npu_stream: A torch.npu.Stream to dispatch HCCL ops to. If rank is
                specified, then this must be specified too.
            use_communication_streams: Whether to use dedicated send and recv
                streams for communication. If True, communication and computation
                can be overlapped to improve performance.
        """
        self._world_size = world_size
        self._rank: Optional[int] = rank
        self.hccl: Optional[ModuleType] = None
        self._actor_handles = actor_handles
        self._use_communication_streams = use_communication_streams

        device = set_default_device()

        if rank is not None:
            assert "NPU" in ray.cluster_resources(), "HCCL actor has no NPUs assigned"
            assert npu_stream is not None, "HCCL actor must specify aclrtStream"

            expected_rank = self.get_rank(ray.get_runtime_context().current_actor)
            assert (
                rank == expected_rank
            ), f"HCCL actor's rank {rank} does not match expected rank {expected_rank}"

            from ray.experimental.channel import hccl

            self.hccl = hccl

            self._comm = self.hccl.HCCLCommunicator(world_size, comm_id, rank)
        else:
            # Driver does not have a rank.
            self._comm = None

        self._npu_stream: Optional["torch.npu.Stream"] = None
        self._send_stream: Optional["torch.npu.Stream"] = None
        self._recv_stream: Optional["torch.npu.Stream"] = None
        if npu_stream is not None:
            assert rank is not None, "HCCL actor has no rank assigned"

            self._npu_stream = npu_stream

            if use_communication_streams:
                import torch
                import torch_npu  # noqa: F401

                self._send_stream = torch.npu.Stream(device=device)
                self._recv_stream = torch.npu.Stream(device=device)
            else:
                self._send_stream = self._npu_stream
                self._recv_stream = self._npu_stream

        self._closed = False

    def initialize(self, rank: int) -> None:
        # No additional initialization is needed.
        pass

    def get_actor_handles(self) -> List["ray.actor.ActorHandle"]:
        """
        Return all actor handles.
        """
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

    def send(self, buf: "torch.Tensor", peer_rank: int) -> None:
        """
        Send a torch.Tensor to a peer.

        This returns when the send kernel has been queued, but the kernel may
        not have completed. Therefore, the caller should ensure that there are
        no concurrent writes to the sent `buf` until the send has finished.
        That is, either all writes should be submitted on the current stream
        (self._npu_stream) or, if on a different stream, that stream should
        synchronize with the current stream.

        Args:
            buf: The torch.Tensor to send. It should already be on this
                actor's default device.
            peer_rank: The rank of the actor to send to.
        """
        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")

        if self._use_communication_streams:
            # We observed that if all recv/compute/send operations run on NPU,
            # since there is no synchronization, the CPU execution loop may be
            # far ahead of the NPU operations and lead to runtime failures.
            # To avoid that, we synchronize on the send stream.
            # TODO(rui): find a better approach
            self._send_stream.synchronize()

        # TODO(swang): Handle send/recv async HCCL errors such as network
        # failures.
        self._comm.send(
            buf.data_ptr(),
            buf.numel(),
            self.hccl.get_hccl_tensor_dtype(buf),
            peer_rank,
            self._send_stream.npu_stream,
        )

    def recv(
        self,
        shape: Tuple[int],
        dtype: "torch.dtype",
        peer_rank: int,
        allocator=Optional[TorchTensorAllocator],
    ) -> "torch.Tensor":
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
            raise RayChannelError("HCCL group has been destroyed.")
        assert allocator is not None, "HCCL group requires a tensor allocator"
        buf = allocator(shape, dtype)

        if self._use_communication_streams:
            # We observed that if all recv/compute/send operations run on NPU,
            # since there is no synchronization, the CPU execution loop may be
            # far ahead of the NPU operations and lead to runtime failures.
            # To avoid that, we synchronize on the recv stream.
            # TODO(rui): find a better approach
            self._recv_stream.synchronize()

            self._comm.recv(
                buf.data_ptr(),
                buf.numel(),
                self.hccl.get_hccl_tensor_dtype(buf),
                peer_rank,
                self._recv_stream.npu_stream,
            )
        else:
            self._comm.recv(
                buf.data_ptr(),
                buf.numel(),
                self.hccl.get_hccl_tensor_dtype(buf),
                peer_rank,
                self._recv_stream.npu_stream,
            )

            # Buffer values are undefined if HCCL ops are aborted. Therefore, we
            # need to synchronize here and check that the channel is still open to
            # ensure that the receive buffer is valid.
            # TODO(swang): Avoid NPU synchronization.
            self._npu_stream.synchronize()

        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")
        return buf

    def allreduce(
        self,
        send_buf: "torch.Tensor",
        recv_buf: "torch.Tensor",
        op: ReduceOp = ReduceOp.SUM,
    ):
        if self._closed:
            raise RayChannelError("HCCL group has been destroyed.")

        assert send_buf.dtype == recv_buf.dtype, (
            "Ray Compiled Graph derived the dtype of recv_buf from send_buf, "
            "so send_buf and recv_buf must have the same dtype. "
            "If you see this error, please file an issue at Ray repository."
        )
        self._comm.allReduce(
            send_buf.data_ptr(),
            recv_buf.data_ptr(),
            send_buf.numel(),
            self.hccl.get_hccl_tensor_dtype(send_buf),
            op.value,
            self._npu_stream.npu_stream,
        )

        # Buffer values are undefined if HCCL ops are aborted. Therefore, we
        # need to synchronize here and check that the channel is still open to
        # ensure that the receive buffer is valid.
        # TODO(swang): Avoid NPU synchronization.
        # TODO(wxdeng): Use check_async_error.
        self._npu_stream.synchronize()
        if self._closed:
            raise RayChannelError(
                "HCCL group has been destroyed during allreduce operation. "
                "There may be a dtype mismatch between input tensors from "
                "different ranks."
            )

    @property
    def recv_stream(self):
        return StreamContext(self._recv_stream)

    @property
    def send_stream(self):
        return StreamContext(self._send_stream)

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
            # Abort *after* setting the _closed flag. This ensures that HCCL
            # ops that were blocked on a remote peer will see that the _closed
            # flag is True when they exit from the abort.
            self._comm.destroy()

    def get_transport_name(self) -> str:
        return "hccl"


def get_hccl_unique_id() -> str:
    """
    Generate a unique HCCL ID.

    This function retrieves a unique identifier for HCCL.

    Returns:
        str: A unique HCCL ID as a string.
    """
    from ray.experimental.channel import hccl

    set_default_device()
    return hccl.get_unique_id()
