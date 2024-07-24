import io
import logging
import uuid
from types import ModuleType
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple, Union

import ray
import ray.util.serialization
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.experimental.channel.torch_tensor_type import TENSOR_METADATA_SIZE_BYTES
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch

    from ray.experimental.channel.shared_memory_channel import Channel
    from ray.experimental.channel.torch_tensor_type import TorchTensorType


# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


# Signature for a torch.Tensor allocator is:
# (shape: Tuple[int], dtype: torch.dtype) -> torch.Tensor.
TorchTensorAllocator = Callable[[Tuple[int], "torch.dtype"], "torch.Tensor"]


class NestedTorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        gpu_data_typ: "TorchTensorType",
        cpu_data_typ: Optional["SharedMemoryType"] = None,
        _gpu_data_channel: Optional["TorchTensorNcclChannel"] = None,
        _cpu_data_channel: Optional["Channel"] = None,
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
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
            gpu_data_typ: Type information about the GPU tensors
            cpu_data_typ: Type information about the CPU data
        """
        self._writer = writer
        self._reader_and_node_list = reader_and_node_list

        if _gpu_data_channel is not None or _cpu_data_channel is not None:
            # This path is used when the NestedTorchTensorNcclChannel is being
            # deserialized.
            assert (
                writer is None
                and reader_and_node_list is None
                and gpu_data_typ is None
                and cpu_data_typ is None
            )
            assert _gpu_data_channel is not None and _cpu_data_channel is not None
            self._gpu_data_channel = _gpu_data_channel
            self._cpu_data_channel = _cpu_data_channel
        else:
            # This path is used when the NestedTorchTensorNcclChannel is first
            # being created, by the writer of the channel.
            self._gpu_data_channel: TorchTensorNcclChannel = (
                gpu_data_typ.create_channel(writer, reader_and_node_list)
            )
            self._cpu_data_channel: Optional["Channel"] = None
            if cpu_data_typ is not None:
                self._cpu_data_channel = cpu_data_typ.create_channel(
                    writer, reader_and_node_list
                )

        # Used for serialization.
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

        ctx = ChannelContext.get_current()
        self.serialization_ctx = ctx.serialization_context
        assert self.serialization_ctx is not None

    @classmethod
    def from_channels(
        cls,
        gpu_data_channel: "TorchTensorNcclChannel",
        cpu_data_channel: Optional["Channel"],
    ):
        return cls(
            writer=None,
            reader_and_node_list=None,
            gpu_data_typ=None,
            cpu_data_typ=None,
            _gpu_data_channel=gpu_data_channel,
            _cpu_data_channel=cpu_data_channel,
        )

    def __reduce__(self):
        return (
            NestedTorchTensorNcclChannel.from_channels,
            (self._gpu_data_channel, self._cpu_data_channel),
        )

    def ensure_registered_as_writer(self):
        self._gpu_data_channel.ensure_registered_as_writer()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.ensure_registered_as_writer()

    def ensure_registered_as_reader(self):
        self._gpu_data_channel.ensure_registered_as_reader()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.ensure_registered_as_reader()

    def write(self, value: Any, timeout: Optional[float] = None) -> None:
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
        self._gpu_data_channel.write(tensors_to_send)
        # Send the rest of the data, with placeholders for the extracted
        # tensors, through a CPU-specific channel.
        self._cpu_data_channel.write(serialized_cpu_data)

    def read(self, timeout: Optional[float] = None) -> Any:
        tensors = self._gpu_data_channel.read()

        if self._gpu_data_channel.has_static_type():
            # If the channel was declared with a static TorchTensorType, then
            # the task is allowed to return at most one tensor, and its shape
            # and dtype must match the declared type. Wrap the tensor in a
            # list since the following calls expect a list.
            tensors = [tensors]

        self.serialization_ctx.reset_tensors(tensors)
        data = self._cpu_data_channel.read()
        self.serialization_ctx.reset_tensors([])

        return data

    def close(self) -> None:
        self._gpu_data_channel.close()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.close()


def _torch_zeros_allocator(shape: Tuple[int], dtype: "torch.dtype"):
    import torch

    ctx = ChannelContext.get_current()
    return torch.zeros(shape, dtype=dtype, device=ctx.torch_device)


@DeveloperAPI
class TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        typ: "TorchTensorType",
        _meta_channel: Optional["Channel"] = None,
        _torch_tensor_allocator: Optional[TorchTensorAllocator] = None,
    ):
        """
        Create a channel for torch.Tensors transferred via NCCL.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
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

        from ray.experimental.channel.torch_tensor_type import TorchTensorType

        self.torch: ModuleType = torch

        self._writer = writer
        self._writer_rank: Optional[int] = None
        self._reader_and_node_list = reader_and_node_list
        self._reader_ranks: Optional[List[int]] = None
        self._writer_registered: bool = False
        self._reader_registered: bool = False
        self._torch_tensor_allocator = _torch_tensor_allocator
        if self._torch_tensor_allocator is None:
            self._torch_tensor_allocator = _torch_zeros_allocator

        assert isinstance(typ, TorchTensorType)
        assert typ.transport == typ.NCCL
        self._typ: "TorchTensorType" = typ

        ctx = ChannelContext.get_current()
        assert self._typ.nccl_group_id is not None, "No NCCL group specified."
        self._nccl_group_id: str = self._typ.nccl_group_id
        self._nccl_group: "_NcclGroup" = ctx.nccl_groups[self._typ.nccl_group_id]
        assert (
            self._nccl_group is not None
        ), "ChannelContext.nccl_group is not initialized."

        self._writer_rank = self._nccl_group.get_rank(self._writer)
        self._reader_ranks = [
            self._nccl_group.get_rank(reader)
            for reader, _ in self._reader_and_node_list
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

        self._meta_channel: Optional[Channel] = _meta_channel
        if (
            self._meta_channel is None
            and self._writer_registered
            and not self.has_static_type()
        ):
            # We are the writer and the shape and/or dtype of the tensor was
            # not statically declared. Therefore, we also need to allocate a
            # metadata channel that will be used to send the shape and dtype of
            # the tensor to the receiver(s).
            metadata_type = SharedMemoryType(
                buffer_size_bytes=TENSOR_METADATA_SIZE_BYTES
            )
            self._meta_channel = metadata_type.create_channel(
                self._writer,
                self._reader_and_node_list,
            )

        if self._meta_channel is None:
            # Check that if there is no metadata channel, then we will only
            # pass tensors of static shape and dtype.
            assert self.has_static_type()

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
                self._reader_and_node_list,
                self._typ,
                self._meta_channel,
                self._torch_tensor_allocator,
            ),
        )

    def _get_tensor_meta(self, tensor: "torch.Tensor") -> Optional["TorchTensorType"]:
        from ray.experimental.channel.torch_tensor_type import TorchTensorType

        if not isinstance(tensor, self.torch.Tensor):
            raise ValueError("Task must return torch.Tensors")

        ctx = ChannelContext.get_current()
        if tensor.device != ctx.torch_device:
            raise ValueError(
                f"torch.Tensor must be on the default device: {ctx.torch_device}"
            )

        meta: Optional["TorchTensorType"] = None
        if not self.has_static_type():
            # User did not declare a static type, so we must send the metadata
            # for this tensor.
            meta = TorchTensorType(_shape=tensor.shape, _dtype=tensor.dtype)
        elif tensor.shape != self._typ._shape:
            raise ValueError(
                f"torch.Tensor has shape {tensor.shape}, expected {self._typ._shape}"
            )
        elif tensor.dtype != self._typ._dtype:
            raise ValueError(
                f"torch.Tensor has dtype {tensor.dtype}, expected {self._typ._dtype}"
            )

        return meta

    def write(
        self,
        tensors: Union["torch.Tensor", List["torch.Tensor"], Exception],
        timeout: Optional[float] = None,
    ):
        if isinstance(tensors, ray.exceptions.RayTaskError):
            # TODO(swang): Write exceptions to the meta channel if it is
            # available.
            raise tensors

        if isinstance(tensors, list):
            meta_list = []
            for tensor in tensors:
                meta_list.append(self._get_tensor_meta(tensor))
            if self.has_static_type():
                # Make sure that there is exactly one tensor to send, and its
                # metadata should have matched the static type.
                if meta_list != [None]:
                    raise ValueError(
                        "DAGNode annotated with "
                        "TorchTensorType(_shape=shape, _dtype=dtype))` can return at "
                        "most one tensor with the declared `_shape` and `_dtype`. "
                        "Use TorchTensorType() if value contains more than one "
                        "tensor or tensor of dynamic size."
                    )
            else:
                self._meta_channel.write(meta_list)
        else:
            meta = self._get_tensor_meta(tensors)
            if meta is not None:
                self._meta_channel.write(meta)
            tensors = [tensors]

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

    def _read_single_tensor(self, typ: "TorchTensorType") -> "torch.Tensor":
        buf = self._torch_tensor_allocator(typ._shape, typ._dtype)
        self._nccl_group.recv(buf, self._writer_rank)
        return buf

    def read(
        self, timeout: Optional[float] = None
    ) -> Union["torch.Tensor", List["torch.Tensor"]]:
        if self._meta_channel is not None:
            meta = self._meta_channel.read()
        else:
            meta = self._typ

        if not isinstance(meta, list):
            return self._read_single_tensor(meta)

        bufs: List["torch.Tensor"] = []
        for typ in meta:
            bufs.append(self._read_single_tensor(typ))
        # TODO: Sync CUDA stream after receiving all tensors, instead of after
        # each tensor.
        return bufs

    def close(self) -> None:
        if self._meta_channel is not None:
            self._meta_channel.close()

        self._nccl_group.destroy()
        ctx = ChannelContext.get_current()
        if self._nccl_group_id in ctx.nccl_groups:
            del ctx.nccl_groups[self._nccl_group_id]

    def has_static_type(self) -> bool:
        from ray.experimental.channel.torch_tensor_type import TorchTensorType

        return (
            self._typ._shape != TorchTensorType.AUTO
            and self._typ._dtype != TorchTensorType.AUTO
        )


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
