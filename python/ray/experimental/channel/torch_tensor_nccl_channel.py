import io
import logging
import uuid
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple, Union

import ray
import ray.util.serialization
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch

    from ray.experimental.channel.shared_memory_channel import Channel


# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

# 100KB to store metadata and/or exceptions.
# NOTE(swang): This will consume memory but it should not affect performance
# because we only copy the actual data stored, not the maximum size of the
# shared meomry buffer.
TENSOR_METADATA_SIZE_BYTES = 100_000


@dataclass
class _TorchTensorMetadata:
    """
    Metadata for torch.Tensors that can be sent between processes to determine
    how large of a buffer to allocate on the receiver(s).
    """

    shape: Union[int, Tuple[int]]
    dtype: "torch.dtype"


# Signature for a torch.Tensor allocator is:
# (shape: Tuple[int], dtype: torch.dtype) -> torch.Tensor.
TorchTensorAllocator = Callable[[_TorchTensorMetadata], "torch.Tensor"]


@DeveloperAPI
class TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        readers: List[ray.actor.ActorHandle],
        tensor_data_channel: "_TorchTensorNcclChannel",
        non_tensor_data_channel: "Channel",
    ):
        """
        Can be used to send GPU tensors nested inside other data. The data is
        sent via shared memory while the GPU tensors are sent through a P2P
        transport (NCCL).

        NOTE: This class is currently not thread-safe because it reads and
        writes the worker-local
        ray.experimental.channel.serialization_context._SerializationContext
        when serializing data.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
            tensor_data_channel: A GPU-GPU channel for sending tensor data.
            non_tensor_data_channel: A shared-memory channel for sending
                non-tensor data.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        self._writer = writer
        self._readers = readers

        self._tensor_data_channel: _TorchTensorNcclChannel = tensor_data_channel
        self._non_tensor_data_channel: "Channel" = non_tensor_data_channel

        # Used for serialization.
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

        ctx = ChannelContext.get_current()
        self.serialization_ctx = ctx.serialization_context
        assert self.serialization_ctx is not None

    def __reduce__(self):
        return (
            TorchTensorNcclChannel,
            (
                self._writer,
                self._readers,
                self._tensor_data_channel,
                self._non_tensor_data_channel,
            ),
        )

    def ensure_registered_as_writer(self):
        self._tensor_data_channel.ensure_registered_as_writer()
        self._non_tensor_data_channel.ensure_registered_as_writer()

    def ensure_registered_as_reader(self):
        self._tensor_data_channel.ensure_registered_as_reader()
        self._non_tensor_data_channel.ensure_registered_as_reader()

    def write(self, value: Any):
        self.serialization_ctx.reset_tensors([])
        # All tensors found in `value` will be transferred via NCCL.
        self.serialization_ctx.set_use_external_transport(True)

        try:
            # Serialize the data. All tensors that match our current device
            # will be extracted into the serialization context and replaced
            # with a placeholder.
            serialized_cpu_data = self._worker.get_serialization_context().serialize(
                value
            )
        except TypeError as e:
            sio = io.StringIO()
            ray.util.inspect_serializability(value, print_file=sio)
            msg = (
                "Could not serialize the put value "
                f"{repr(value)}:\n"
                f"{sio.getvalue()}"
            )
            raise TypeError(msg) from e
        finally:
            # Pop the tensors that were found during serialization of `value`.
            tensors_to_send = self.serialization_ctx.reset_tensors([])
            # Reset the serialization method to now serialize torch.Tensors
            # normally.
            self.serialization_ctx.set_use_external_transport(False)

        # Send the extracted tensors through a GPU-specific channel.
        self._tensor_data_channel.write(tensors_to_send)
        # Send the rest of the data, with placeholders for the extracted
        # tensors, through a CPU-specific channel.
        self._non_tensor_data_channel.write(serialized_cpu_data)

    def begin_read(self) -> Any:
        tensors = self._tensor_data_channel.begin_read()

        # TODO: If static non-tensor data, deserialize the same data as
        # before.
        self.serialization_ctx.reset_tensors(tensors)
        data = self._non_tensor_data_channel.begin_read()
        self.serialization_ctx.reset_tensors([])

        return data

    def end_read(self) -> None:
        self._tensor_data_channel.end_read()
        self._non_tensor_data_channel.end_read()

    def close(self) -> None:
        self._tensor_data_channel.close()
        self._non_tensor_data_channel.close()


def _torch_zeros_allocator(meta: _TorchTensorMetadata):
    import torch

    ctx = ChannelContext.get_current()
    return torch.zeros(meta.shape, dtype=meta.dtype, device=ctx.torch_device)


class _TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        readers: List[ray.actor.ActorHandle],
        # TODO(swang): Make a NCCLGroupID class.
        nccl_group_id: str,
        static_shape: bool = False,
        _meta_channel: Optional["Channel"] = None,
        _torch_tensor_allocator: Optional[TorchTensorAllocator] = None,
    ):
        """
        A helper channel for TorchTensorNcclChannel that is used to transfer
        torch.Tensors via NCCL. This class can only transfer torch.Tensors and
        cannot transfer other CPU data, such as tensors nested inside of a
        dictionary.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            readers: The actors that may read from the channel. None signifies
                the driver.
            typ: Type information about the values passed through the channel.
            _meta_channel: A channel used to send metadata for the tensors,
                i.e. shape and dtype. If not provided, and if the typ does not
                specify a static shape and dtype, then a metadata channel based
                on shared memory will be created.
            _torch_tensor_allocator: An optional allocator function for
                allocating torch.Tensor buffers on receivers. By default,
                torch.zeros will be used.
        """
        import torch

        self.torch: ModuleType = torch

        self._writer = writer
        self._writer_rank: Optional[int] = None
        self._readers = readers
        self._reader_ranks: Optional[List[int]] = None
        self._writer_registered: bool = False
        self._reader_registered: bool = False
        self._torch_tensor_allocator = _torch_tensor_allocator
        if self._torch_tensor_allocator is None:
            self._torch_tensor_allocator = _torch_zeros_allocator

        ctx = ChannelContext.get_current()
        assert isinstance(
            nccl_group_id, str
        ), "NCCL group ID ({nccl_group_id}) must be a str."
        self._nccl_group_id: str = nccl_group_id
        self._nccl_group: "_NcclGroup" = ctx.nccl_groups[nccl_group_id]
        assert (
            self._nccl_group is not None
        ), "ChannelContext.nccl_group is not initialized."

        self._static_shape = static_shape

        self._writer_rank = self._nccl_group.get_rank(self._writer)
        self._reader_ranks = [
            self._nccl_group.get_rank(reader) for reader in self._readers
        ]

        if (
            self._writer_rank is not None
            and self._writer_rank == self._nccl_group.get_self_rank()
        ):
            self._writer_registered = True

        if (
            self._reader_ranks
            and self._nccl_group.get_self_rank() in self._reader_ranks
        ):
            self._reader_registered = True

        # If the channel type specifies that the tensor shape is static, then the
        # receiver can allocate buffers without needing to coordinate with the
        # sender. We set the metadata on the first send-recv op. Thereafter,
        # the sender must ensure that sent tensors match this metadata, and the
        # receiver will allocate tensors with this shape.
        self._static_tensor_metadata: Optional[List[_TorchTensorMetadata]] = None
        self._meta_channel: Optional[Channel] = _meta_channel
        if self._meta_channel is None and self._writer_registered:
            # We are the writer. Therefore, we also need to allocate a metadata
            # channel that will be used to send the shape and dtype of the
            # tensor to the receiver(s).
            metadata_type = SharedMemoryType(
                buffer_size_bytes=TENSOR_METADATA_SIZE_BYTES
            )
            self._meta_channel = metadata_type.create_channel(
                self._writer,
                self._readers,
            )

    def ensure_registered_as_writer(self):
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"
        assert self._writer_registered
        ctx = ChannelContext.get_current()
        assert ctx.torch_device.type == "cuda"

    def ensure_registered_as_reader(self) -> bool:
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"
        assert self._reader_registered
        ctx = ChannelContext.get_current()
        assert ctx.torch_device.type == "cuda"

    def __reduce__(self):
        return (
            self.__class__,
            (
                self._writer,
                self._readers,
                self._nccl_group_id,
                self._static_shape,
                self._meta_channel,
                self._torch_tensor_allocator,
            ),
        )

    def _get_send_tensors_metadata(
        self, tensors: List["torch.Tensor"]
    ) -> Optional[List[_TorchTensorMetadata]]:
        ctx = ChannelContext.get_current()

        # Get the shape and dtype of each tensor to send.
        metadata_list = []
        for tensor in tensors:
            # Basic type checking.
            if not isinstance(tensor, self.torch.Tensor):
                raise ValueError("Task must return torch.Tensors")

            if tensor.device != ctx.torch_device:
                raise ValueError(
                    f"torch.Tensor must be on the default device: {ctx.torch_device}"
                )

            metadata = _TorchTensorMetadata(tensor.shape, tensor.dtype)
            metadata_list.append(metadata)

        if self._static_tensor_metadata is not None:
            if metadata_list != self._static_tensor_metadata:
                metadata_str = [
                    f"(shape={m.shape}, dtype={m.dtype})" for m in metadata_list
                ]
                expected_str = [
                    f"(shape={m.shape}, dtype={m.dtype})"
                    for m in self._static_tensor_metadata
                ]
                raise ValueError(
                    "Expected torch.Tensors with shapes and dtypes: "
                    "[" + ", ".join(expected_str) + "],\n"
                    "found: [" + ", ".join(metadata_str) + "]"
                )
            # The receiver has already determined the shape and dtype of the
            # tensors from a previous send, so no need to send the metadata
            # again.
            return None

        if self._static_shape:
            # The shape and dtype is static. This is the first send op and
            # afterwards, a ValueError will be thrown if the sent tensors do
            # not match this metadata.
            self._static_tensor_metadata = metadata_list
        return metadata_list

    def write(
        self,
        tensors: Union[List["torch.Tensor"], Exception],
    ):
        if isinstance(tensors, ray.exceptions.RayTaskError):
            # TODO(swang): Write exceptions to the meta channel if it is
            # available.
            raise tensors

        # Send the tensors metadata so that the receiver knows what buffers to
        # allocate.
        metadata = self._get_send_tensors_metadata(tensors)
        if metadata is not None:
            self._meta_channel.write(metadata)

        # NOTE(swang): We must send the metadata *before* launching the NCCL
        # send. We are using blocking NCCL ops, so the following calls will
        # block until the kernel has been enqueued. Also, peers must launch the
        # kernel together before either can proceed. Therefore, we send the
        # metadata first so that the receiver can read the metadata and then
        # launch the same NCCL op.
        for tensor in tensors:
            # TODO: If there are multiple readers, can replace with a
            # broadcast.
            for rank in self._reader_ranks:
                self._nccl_group.send(tensor, rank)

    def _get_recv_tensors_metadata(self) -> List[_TorchTensorMetadata]:
        if self._static_tensor_metadata is not None:
            return self._static_tensor_metadata

        meta = self._meta_channel.begin_read()
        # It's safe to release the channel because the metadata should get
        # copied during deserialization.
        self._meta_channel.end_read()

        if self._static_shape:
            self._static_tensor_metadata = meta

        return meta

    def begin_read(self) -> Union["torch.Tensor", List["torch.Tensor"]]:
        meta_list: List[_TorchTensorMetadata] = self._get_recv_tensors_metadata()

        bufs: List["torch.Tensor"] = []
        for meta in meta_list:
            buf = self._torch_tensor_allocator(meta)
            self._nccl_group.recv(buf, self._writer_rank)
            bufs.append(buf)
        # TODO: Sync CUDA stream after receiving all tensors, instead of after
        # each tensor.
        return bufs

    def end_read(self) -> None:
        return

    def close(self) -> None:
        self._meta_channel.close()

        self._nccl_group.destroy()
        ctx = ChannelContext.get_current()
        if self._nccl_group_id in ctx.nccl_groups:
            del ctx.nccl_groups[self._nccl_group_id]


def _do_init_nccl_group(self, group_id, world_size, comm_id, rank, actor_handles):
    import torch

    assert (
        ray.get_gpu_ids()
    ), "Actors participating in NCCL group must have at least one GPU assigned"

    ctx = ChannelContext.get_current()
    ctx.nccl_groups[group_id] = _NcclGroup(
        world_size,
        comm_id,
        rank,
        actor_handles,
        torch.cuda.current_stream().cuda_stream,
    )


def _do_destroy_nccl_group(self, group_id):
    ctx = ChannelContext.get_current()
    if group_id not in ctx.nccl_groups:
        return

    ctx.nccl_groups[group_id].destroy()
    del ctx.nccl_groups[group_id]


def _do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _do_get_unique_nccl_id(self) -> bool:
    from cupy.cuda import nccl

    return nccl.get_unique_id()


def _init_nccl_group(
    actors: List[ray.actor.ActorHandle],
) -> str:
    ctx = ChannelContext.get_current()

    has_gpus = ray.get(
        [actor.__ray_call__.remote(_do_check_has_gpu) for actor in actors]
    )
    for has_gpu, actor in zip(has_gpus, actors):
        if not has_gpu:
            raise ValueError(
                f"Actor {actor} returns a tensor with type hint "
                'TorchTensor(transport="nccl") but actor does not have a '
                "GPU assigned by Ray."
            )

    actor_ids = {actor._ray_actor_id for actor in actors}
    assert len(actor_ids) == len(actors), "Actors must be unique"

    # Allocate a communicator ID on one of the actors that will participate in
    # the group. This is in case the driver is not on the same node as one of
    # the NCCL actors.
    nccl_comm_id = ray.get(actors[0].__ray_call__.remote(_do_get_unique_nccl_id))
    # Used to uniquely identify this NCCL group.
    group_id = str(uuid.uuid4())

    logger.info(f"Creating NCCL group {group_id} on actors: {actors}")

    world_size = len(actors)
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_nccl_group,
            group_id,
            world_size,
            nccl_comm_id,
            rank,
            actors,
        )
        for rank, actor in enumerate(actors)
    ]
    try:
        ray.get(init_tasks, timeout=30)
    except ray.exceptions.GetTimeoutError:
        logger.warning(
            "NCCL group creation not done after 30s. NCCL group creation may be hung."
        )
        ray.get(init_tasks)

    logger.info("NCCL group created.")

    ctx.nccl_groups[group_id] = _NcclGroup(
        world_size,
        nccl_comm_id,
        rank=None,
        actor_handles=actors,
        cuda_stream=None,
    )
    return group_id


def _destroy_nccl_group(group_id: str) -> None:
    ctx = ChannelContext.get_current()
    if group_id not in ctx.nccl_groups:
        return

    group = ctx.nccl_groups[group_id]
    actors = group._get_actor_handles()
    destroy_tasks = [
        actor.__ray_call__.remote(
            _do_destroy_nccl_group,
            group_id,
        )
        for actor in actors
    ]

    _, unready = ray.wait(destroy_tasks, timeout=30, num_returns=len(destroy_tasks))
    if unready:
        logger.warning(
            "NCCL group destruction not done after 30s. NCCL group destruction "
            "may be hung."
        )

    del ctx.nccl_groups[group_id]
