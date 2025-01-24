import io
import logging
import uuid
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

import ray
import ray.util.serialization
from ray.experimental.channel import ChannelContext, utils
from ray.experimental.channel.common import ChannelInterface
from ray.experimental.channel.communicator import Communicator
from ray.experimental.channel.cpu_communicator import CPUCommunicator
from ray.experimental.channel.intra_process_channel import IntraProcessChannel
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch

    from ray.experimental.channel.shared_memory_channel import Channel


# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


@dataclass
class _TorchTensorMetadata:
    """
    Metadata for torch.Tensors that can be sent between processes to determine
    how large of a buffer to allocate on the receiver(s).
    """

    shape: Union[int, Tuple[int]]
    dtype: "torch.dtype"


@DeveloperAPI
class TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        typ: "TorchTensorType",
        driver_actor_id: str,
        tensor_metadata_channel: Optional["Channel"] = None,
        _cpu_data_channel: Optional["Channel"] = None,
        _gpu_data_channel: Optional["_TorchTensorNcclChannel"] = None,
        _local_channel: Optional["IntraProcessChannel"] = None,
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
            writer: The actor that may write to the channel. None signifies the
                driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
            typ: Type information about the values passed through the channel.
            driver_actor_id: The actor ID of the DAGDriverProxyActor.
            tensor_metadata_channel: A shared-memory channel for sending tensor
                metadata.
            _cpu_data_channel: A shared-memory channel for sending
                non-tensor data. Its writer and readers should match the given
                writer and readers. If None is provided, then we assume that
                there is no CPU-specific data, i.e. the task directly returned
                a CUDA torch.Tensor.
            _gpu_data_channel: A channel for sending torch.Tensors via NCCL.
            _local_channel: A channel for sending data between the writer and
                local readers.

        NOTE: `tensor_metadata_channel` will be set only for testing purposes.
        `_cpu_data_channel` is set for testing purposes and for deserialization.
        `_gpu_data_channel` and `_local_channel` are set only during deserialization.
        """
        self._writer = writer
        self._reader_and_node_list = reader_and_node_list
        self._typ = typ

        (
            remote_reader_and_node_list,
            local_reader_and_node_list,
        ) = utils.split_readers_by_locality(self._writer, self._reader_and_node_list)

        num_local_readers = len(local_reader_and_node_list)
        self._local_channel = _local_channel
        if self._local_channel is None and num_local_readers > 0:
            # There are some local readers which are the same worker process as
            # the writer. Create a local channel for the writer and the local readers.
            #
            # Use num_readers = 1 when creating the local channel,
            # because we have channel cache to support reading
            # from the same channel multiple times.
            self._local_channel = IntraProcessChannel(num_readers=1)

        assert len(remote_reader_and_node_list) > 0, (
            "All readers are from the same actor. "
            "The TorchTensorType type hint is not needed. "
            "No NCCL channel will be created."
        )
        self._gpu_data_channel = _gpu_data_channel
        if self._gpu_data_channel is None:
            self._gpu_data_channel: _TorchTensorNcclChannel = _TorchTensorNcclChannel(
                writer,
                remote_reader_and_node_list,
                typ,
                _meta_channel=tensor_metadata_channel,
            )

        self._cpu_data_channel: Optional["Channel"] = _cpu_data_channel
        if self._cpu_data_channel is not None:
            assert (
                not self._typ.direct_return
            ), "CPU channel should be None if direct return is enabled"

        if self._cpu_data_channel is None and not self._typ.direct_return:
            # Create a CPU channel to send non-tensor data.
            self._cpu_data_channel = SharedMemoryType().create_channel(
                writer, remote_reader_and_node_list, driver_actor_id
            )

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
                self._reader_and_node_list,
                self._typ,
                # driver_actor_id and tensor_metadata_channel are used to initialize
                # the _cpu_data_channel and _gpu_data_channel, so we don't need to
                # pass them in here.
                None,
                None,
                self._cpu_data_channel,
                self._gpu_data_channel,
                self._local_channel,
            ),
        )

    def ensure_registered_as_writer(self):
        if self._local_channel is not None:
            self._local_channel.ensure_registered_as_writer()
        self._gpu_data_channel.ensure_registered_as_writer()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.ensure_registered_as_writer()

    def ensure_registered_as_reader(self):
        reader = utils.get_self_actor()
        if reader == self._writer:
            self._local_channel.ensure_registered_as_reader()
            return
        self._gpu_data_channel.ensure_registered_as_reader()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.ensure_registered_as_reader()

    def _send_cpu_and_gpu_data(self, value: Any, timeout: Optional[float]):
        self.serialization_ctx.reset_out_of_band_tensors([])
        # All tensors found in `value` will be transferred via NCCL.
        self.serialization_ctx.set_use_external_transport(True)

        try:
            # Serialize the data. All tensors that match our current device
            # will be extracted into the serialization context and replaced
            # with a placeholder.
            cpu_data = self._worker.get_serialization_context().serialize(value)
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
            gpu_tensors, _ = self.serialization_ctx.reset_out_of_band_tensors([])
            # Reset the serialization method to now serialize torch.Tensors
            # normally.
            self.serialization_ctx.set_use_external_transport(False)

        # First send the extracted tensors through a GPU-specific channel.
        self._gpu_data_channel.write(gpu_tensors)
        # Next send the non-tensor data through a CPU-specific channel. The
        # data contains placeholders for the extracted tensors.
        self._cpu_data_channel.write(cpu_data)

    def write(self, value: Any, timeout: Optional[float] = None) -> None:
        """
        Send a value that may contain torch.Tensors that should be sent via
        external transport.

        Case 1: Use `_local_channel` to send the data to local readers.

        Case 2: Otherwise, use the following method to send the data to remote readers.

        1) Serializes `value`. During serialization, all torch.Tensors that are
        on the default device are extracted and replaced with a unique
        placeholder. Thus, the serialized value will contain all non-tensor
        data, and any tensors that were not on the default device (e.g., CPU
        tensor returned by a GPU actor).
        2) Sends extracted torch.Tensors via the tensor data channel (e.g.,
        NCCL).
        3) Sends the non-tensor data via the non-tensor data channel.

        If static_non_tensor_data=True was specified, then we only perform step
        (3) on the first `write` call. The reader is expected to reuse the sent
        data for subsequent messages.
        """
        self.ensure_registered_as_writer()

        if self._local_channel is not None:
            self._local_channel.write(value)

        if isinstance(value, ray.exceptions.RayTaskError):
            if self._typ.static_shape or self._typ.direct_return:
                # Raise a fatal error to teardown the DAG.
                # This error will also be caught from `CompiledDAGRef.get()`
                # and raised to the user
                # TODO(swang): Write exceptions to the tensor metadata or
                # non-tensor data channel if it is available to make these
                # exceptions recoverable.
                raise value

        if self._cpu_data_channel is None:
            # Handle the case where _direct_return=True. In this case, we check
            # that the task returned a CUDA torch.Tensor and just send it
            # directly without trying to serialize it first.
            import torch

            # These ValueErrors will also be caught from `CompiledDAGRef.get()`
            # and raised to the user
            if not isinstance(value, torch.Tensor):
                # TODO(swang): These errors are currently fatal for the DAG.
                # This could be improved by sending the exception through the
                # gpu_data_channel's CPU-based metadata channel, if one exists.
                raise ValueError(
                    "Task annotated with _direct_return=True must "
                    "return a CUDA torch.Tensor, instead found value "
                    f"`{value}`. DAG will shut down."
                )
            elif not value.is_cuda:
                raise ValueError(
                    "Task annotated with _direct_return=True must "
                    "return a CUDA torch.Tensor, instead found CPU tensor. "
                    "DAG will shut down."
                )
            self._gpu_data_channel.write([value], timeout=timeout)
        else:
            self._send_cpu_and_gpu_data(value, timeout)

    def _recv_cpu_and_gpu_data(
        self, tensors: List["torch.Tensor"], timeout: Optional[float] = None
    ) -> Any:
        """
        Helper method to receive data that contains a mix of CPU and GPU data.

        Args:
            tensors: The GPU data. This is a list of the torch.Tensors that
                were found in the sent data.
            timeout: Timeout for channel receive.
        """
        self.serialization_ctx.reset_out_of_band_tensors(tensors)

        # Next, read and deserialize the non-tensor data. The registered custom
        # deserializer will replace the found tensor placeholders with
        # `tensors`.
        data = self._cpu_data_channel.read(
            timeout=timeout,
        )
        # Check that all placeholders had a corresponding tensor.
        (
            _,
            deserialized_tensor_placeholders,
        ) = self.serialization_ctx.reset_out_of_band_tensors([])
        assert deserialized_tensor_placeholders == set(range(len(tensors)))

        return data

    def read(self, timeout: Optional[float] = None) -> Any:
        """
        Read a value that may contain torch.Tensors sent via external
        transport.

        Case 1: If the reader is a local reader and is the same actor as the writer,
        then use the `_local_channel` to read the data.

        Case 2: Otherwise, use the following method to read data from remote readers.

        1) Receives torch.Tensors via the tensor data channel (e.g., NCCL).
        2) Reads the serialized non-tensor data.
        3) Deserializes the non-tensor data. During deserialization, replaces
        all found placeholders with the received torch.Tensors.

        If _direct_return=True was specified, then we skip step (2) and (3) and
        directly return the data received in (1).
        """
        self.ensure_registered_as_reader()

        # If the reader is the same actor as the writer, then we can use the
        # local channel to read the data.
        reader = utils.get_self_actor()
        if reader == self._writer:
            assert self._local_channel is not None
            return self._local_channel.read()

        # First, read the tensor data.
        tensors = self._gpu_data_channel.read(timeout)

        if self._cpu_data_channel is None:
            # Handle _direct_return=True. In this case, we expect to receive
            # only one tensor, and we return it directly.
            assert len(tensors) == 1
            data = tensors[0]
        else:
            data = self._recv_cpu_and_gpu_data(tensors, timeout)

        return data

    def close(self) -> None:
        self._gpu_data_channel.close()
        if self._cpu_data_channel is not None:
            self._cpu_data_channel.close()
        if self._local_channel is not None:
            self._local_channel.close()


def _torch_zeros_allocator(
    shape: Union[int, Tuple[int]],
    dtype: "torch.dtype",
):
    """
    Allocate a zeros tensor buffer matching the given metadata.
    """
    import torch

    ctx = ChannelContext.get_current()
    return torch.zeros(shape, dtype=dtype, device=ctx.torch_device)


class _TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        typ: "TorchTensorType",
        _meta_channel: Optional["Channel"] = None,
    ):
        """
        A helper channel for TorchTensorNcclChannel that is used to transfer
        lists of torch.Tensors via NCCL. This class can only transfer
        torch.Tensors and cannot transfer other CPU data, such as Exception
        objects or tensors nested inside of a dictionary.

        Args:
            writer: The actor that may write to the channel. None signifies the driver.
            reader_and_node_list: A list of tuples, where each tuple contains a reader
                actor handle and the node ID where the actor is located.
            typ: Type information about the values passed through the channel.
            _meta_channel: A channel used to send metadata for the tensors,
                i.e. shape and dtype. If not provided, and if the typ does not
                specify a static shape and dtype, then a metadata channel based
                on shared memory will be created.
        """
        import torch

        self.torch: ModuleType = torch

        self._writer = writer
        self._writer_rank: Optional[int] = None
        self._reader_and_node_list = reader_and_node_list
        self._reader_ranks: Optional[List[int]] = None
        self._writer_registered: bool = False
        self._reader_registered: bool = False

        ctx = ChannelContext.get_current()
        assert isinstance(
            typ.communicator_id, str
        ), f"NCCL group ID ({typ.communicator_id}) must be a str."
        self._typ = typ

        assert self._typ.communicator_id is not None, "No NCCL group specified."
        self._nccl_group_id: str = self._typ.communicator_id
        self._nccl_group: "Communicator" = ctx.communicators[self._typ.communicator_id]
        assert (
            self._nccl_group is not None
        ), "ChannelContext.nccl_group is not initialized."

        self._static_shape = typ.static_shape

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
            metadata_type = SharedMemoryType()
            self._meta_channel = metadata_type.create_channel(
                self._writer,
                self._reader_and_node_list,
                None,
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
                self._reader_and_node_list,
                self._typ,
                self._meta_channel,
            ),
        )

    def _get_send_tensors_metadata(
        self, tensors: List["torch.Tensor"]
    ) -> Optional[List[_TorchTensorMetadata]]:
        """
        Helper method to get the metadata that should be sent to the reader so
        that they can allocate the proper-sized buffer(s). Throws error if
        static_shape=True was set and the given tensors do not match the
        inferred shapes.

        Returns: The metadata to send to the reader. None means that we should
            not send any metadata message to the reader.
        """
        ctx = ChannelContext.get_current()

        # TODO(swang): Currently any exceptions thrown during this method are
        # fatal for the DAG because there is no way for the receiver to receive
        # the exception. This can be improved by sending the exception through
        # the CPU-based non-tensor-data channel, if one exists. The tensor
        # channel can send empty data alongside the exception to avoid hanging.

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
                    "[" + ", ".join(expected_str) + "], "
                    "found: [" + ", ".join(metadata_str) + "]. "
                    "DAG will shut down."
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
        tensors: List["torch.Tensor"],
        timeout: Optional[float] = None,
    ):
        """
        Write a list of tensors via NCCL:

        1) Send the tensor metadata, i.e. the shape and dtypes of all tensors
        via the shared-memory metadata channel.
        2) Send the tensor data via NCCL.

        If static_shape=True was set, then we only perform step (1) on the
        first message. The reader is expected to reuse the sent metadata for
        subsequent messages.
        """
        self.ensure_registered_as_writer()

        import torch

        for tensor in tensors:
            assert isinstance(
                tensor, torch.Tensor
            ), f"{tensor} must be instance of torch.Tensor"

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

    def _get_recv_tensors_metadata(
        self, timeout: Optional[float] = None
    ) -> List[_TorchTensorMetadata]:
        """
        Get the shape(s) and dtype(s) of the tensors to receive from the
        metadata channel. If static_shape=True was set, then we reuse the first
        metadata received.
        """
        if self._static_tensor_metadata is not None:
            return self._static_tensor_metadata

        meta = self._meta_channel.read(timeout)

        if self._static_shape:
            self._static_tensor_metadata = meta

        return meta

    def read(
        self,
        timeout: Optional[float] = None,
    ) -> Union["torch.Tensor", List["torch.Tensor"]]:
        """
        Receive a list of tensors.

        (1) Receive the tensor metadata via the shared-memory metadata channel.
        (2) Allocate buffers on our default device according to the received
        tensor metadata.
        (3) Receive the tensor data via NCCL.

        If static_data=True was set, then we only perform step (1) on the first
        message. Subsequent messages reuse the same metadata.

        NOTE: Currently `timeout` only applies to receiving the CPU-based
        tensor metadata. The GPU recv may exceed the timeout without throwing
        an error.
        """
        self.ensure_registered_as_reader()

        meta_list: List[_TorchTensorMetadata] = self._get_recv_tensors_metadata(timeout)

        bufs: List["torch.Tensor"] = []
        for meta in meta_list:
            buf = self._nccl_group.recv(
                meta.shape, meta.dtype, self._writer_rank, _torch_zeros_allocator
            )
            bufs.append(buf)
        # TODO: Sync CUDA stream after receiving all tensors, instead of after
        # each tensor.
        return bufs

    def close(self) -> None:
        self._meta_channel.close()

        self._nccl_group.destroy()
        ctx = ChannelContext.get_current()
        if self._nccl_group_id in ctx.communicators:
            del ctx.communicators[self._nccl_group_id]


def _do_init_communicator(
    self,
    group_id,
    world_size,
    comm_id,
    rank,
    actor_handles,
    use_communication_streams,
    custom_communicator: Optional[Communicator] = None,
):
    import torch

    if not custom_communicator:
        assert (
            ray.get_gpu_ids()
        ), "Actors participating in NCCL group must have at least one GPU assigned"

    ctx = ChannelContext.get_current()
    if custom_communicator is not None:
        custom_communicator.initialize(rank)
        ctx.communicators[group_id] = custom_communicator
    else:
        # default to NcclGroup
        ctx.communicators[group_id] = _NcclGroup(
            world_size,
            comm_id,
            rank,
            actor_handles,
            torch.cuda.current_stream().cuda_stream,
            use_communication_streams,
        )


def _do_destroy_communicator(self, group_id):
    ctx = ChannelContext.get_current()
    if group_id not in ctx.communicators:
        return
    ctx.communicators[group_id].destroy()

    # Keep the NCCL group in the map after destruction in case there is still a
    # task loop running.


def _do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _do_get_unique_nccl_id(self) -> bool:
    from cupy.cuda import nccl

    return nccl.get_unique_id()


def _get_ranks(
    actors: List[ray.actor.ActorHandle], custom_nccl_group: Optional[Communicator]
) -> List[int]:
    """
    Get ranks for the NCCL group to use. If custom_nccl_group is specified,
    return the ranks of the actors in the custom NCCL group, in the same
    order of the actors; otherwise, return list(range(len(actors))).

    Args:
        actors: A list of actors that participate in the NCCL group.
        custom_nccl_group: The custom NCCL group to use.
    """
    if custom_nccl_group is None:
        return list(range(len(actors)))

    assert len(actors) == custom_nccl_group.get_world_size(), (
        "The world size of the custom NCCL group does not match the number "
        "of actors."
    )
    ranks = []
    for actor in actors:
        rank = custom_nccl_group.get_rank(actor)
        assert rank not in ranks, "Duplicate rank in custom NCCL group"
        ranks.append(rank)
    assert custom_nccl_group.get_world_size() == len(actors), (
        "The world size of the custom NCCL group "
        f"({custom_nccl_group.get_world_size()}) "
        "does not match the number of actors "
        f"({len(actors)})."
    )
    return ranks


def _init_communicator(
    actors: List[ray.actor.ActorHandle],
    custom_communicator: Optional[Communicator] = None,
    use_communication_streams: bool = False,
) -> str:
    """
    Initialize a NCCL group with the given actors. If a custom NCCL group is
    provided, then it will be used, otherwise a new NCCL group will be created.

    Args:
        actors: A list of actors that participate in the NCCL group.
        custom_communicator: A custom NCCL group to initialize.
        use_communication_streams: Whether to use dedicated send and recv
                streams for communication. If True, communication and computation
                can be overlapped to improve performance.
    """
    ctx = ChannelContext.get_current()

    is_cpu_communicator = custom_communicator and isinstance(
        custom_communicator, CPUCommunicator
    )

    has_gpus = ray.get(
        [actor.__ray_call__.remote(_do_check_has_gpu) for actor in actors]
    )
    for has_gpu, actor in zip(has_gpus, actors):
        if not has_gpu and not is_cpu_communicator:
            raise ValueError(
                f"Actor {actor} returns a tensor with type hint "
                'TorchTensor(transport="nccl") or '
                "TorchTensor(transport=nccl_group_handle)"
                "but actor does not have a GPU assigned by Ray."
            )

    actor_ids = {actor._ray_actor_id for actor in actors}
    assert len(actor_ids) == len(actors), "Actors must be unique"

    # Allocate a communicator ID on one of the actors that will participate in
    # the group. This is in case the driver is not on the same node as one of
    # the NCCL actors.
    nccl_comm_id = (
        ray.get(actors[0].__ray_call__.remote(_do_get_unique_nccl_id))
        if not is_cpu_communicator
        else str(uuid.uuid4())
    )
    # Used to uniquely identify this NCCL group.
    group_id = str(uuid.uuid4())

    if custom_communicator is not None:
        logger.info(f"Initializing custom NCCL group {group_id} on actors: {actors}")
    else:
        logger.info(f"Creating NCCL group {group_id} on actors: {actors}")

    world_size = len(actors)
    ranks = _get_ranks(actors, custom_communicator)
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_communicator,
            group_id,
            world_size,
            nccl_comm_id,
            rank,
            actors,
            use_communication_streams,
            custom_communicator,
        )
        for rank, actor in zip(ranks, actors)
    ]
    try:
        ray.get(init_tasks, timeout=30)
    except ray.exceptions.GetTimeoutError:
        logger.warning(
            "NCCL group creation not done after 30s. NCCL group creation may be hung."
        )
        ray.get(init_tasks)

    logger.info("NCCL group initialized.")

    if custom_communicator is not None:
        ctx.communicators[group_id] = custom_communicator
    else:
        ctx.communicators[group_id] = _NcclGroup(
            world_size,
            nccl_comm_id,
            rank=None,
            actor_handles=actors,
            cuda_stream=None,
        )
    return group_id


def _destroy_communicator(group_id: str) -> None:
    """
    Destroy the NCCL group with the given ID.
    """
    ctx = ChannelContext.get_current()
    if group_id not in ctx.communicators:
        return

    group = ctx.communicators[group_id]
    actors = group.get_actor_handles()
    destroy_tasks = [
        actor.__ray_call__.remote(
            _do_destroy_communicator,
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

    del ctx.communicators[group_id]
