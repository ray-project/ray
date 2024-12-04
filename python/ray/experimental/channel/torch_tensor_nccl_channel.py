import io
import logging
import uuid
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Tuple, Union

import ray
import ray.util.serialization
from ray.experimental.channel import ChannelContext
from ray.experimental.channel.common import ChannelInterface, _SerializationContext
from ray.experimental.channel.gpu_communicator import GPUCommunicator
from ray.experimental.channel.nccl_group import _NcclGroup
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import torch

    from ray._raylet import SerializedObject
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


# _TensorKey represents a path of lookup keys in an arbitrary Python object.
# Each element of the path is a dictionary or list key.
# If an object is not a list or a dictionary, we try to lookup the key using
# object.__dict__. These keys are added with a True flag, to differentiate from
# direct lookup into a dictionary object.
# Examples:
# - obj=torch.Tensor; key=()
# - obj=[1, [torch.Tensor(...)], 3]; key=(1, 0)
# - obj={"x": [1, [torch.Tensor(...)], 3]}; key=(("x", False), 1, 0)
# - obj.x=[1, [torch.Tensor(...)], 3]; key=(("x", True), 1, 0)
_TensorKey = Tuple[Union[int, Tuple[str, bool]], ...]


class _TensorKeyMapping(NamedTuple):
    # We replace each tensor with an integer placeholder. This dictionary maps
    # tensors to placeholders. Tensors that appear more than once in the object
    # are replaced with the same integer.
    to_placeholder: Optional[Dict[_TensorKey, int]]
    # The order of tensors to send. The sender should extract the tensors at
    # the corresponding keys and send these in the same order through the GPU
    # channel.
    keys_by_transfer_order: Optional[List[_TensorKey]]


def _lookup_tensor_key(value: Any, tensor_key: _TensorKey):
    """
    Given an arbitrary Python value and a tensor_key, find the nested value at
    that tensor key.
    """
    if tensor_key == ():
        return value

    if isinstance(value, dict):
        return _lookup_tensor_key(value[tensor_key[0]], tensor_key[1:])
    elif isinstance(value, list):
        return _lookup_tensor_key(value[tensor_key[0]], tensor_key[1:])
    elif isinstance(value, tuple):
        return _lookup_tensor_key(value[tensor_key[0]], tensor_key[1:])
    elif hasattr(value, "__dict__"):
        return _lookup_tensor_key(getattr(value, tensor_key[0]), tensor_key[1:])
    else:
        raise TypeError(f"_TensorKey {tensor_key} not valid for object: {value}")


def _find_device_tensors(
    value: Any,
    device: Optional["torch.device"],
    tensors: Optional[Dict[_TensorKey, "torch.Tensor"]] = None,
    tensor_key: Optional[Tuple[Any]] = None,
    seen=None,
) -> Dict[_TensorKey, "torch.Tensor"]:
    """
    A helper function to find any torch.Tensors nested in an arbitrary Python
    value.

    Each torch.Tensor found is added to tensors, using its
    _TensorKey.
    """
    import torch

    if tensors is None:
        tensors = {}
    if tensor_key is None:
        tensor_key = ()
    if seen is None:
        seen = set()

    obj_id = id(value)
    if obj_id in seen:
        return tensors
    seen.add(obj_id)

    if isinstance(value, torch.Tensor):
        tensors[tensor_key] = value
        return tensors

    if isinstance(value, dict):
        for k, v in value.items():
            _find_device_tensors(v, device, tensors, tensor_key + ((k, False),), seen)
    elif isinstance(value, list):
        for i, e in enumerate(value):
            _find_device_tensors(value[i], device, tensors, tensor_key + (i,), seen)
    elif isinstance(value, tuple):
        for i, e in enumerate(value):
            _find_device_tensors(e, device, tensors, tensor_key + (i,), seen)

    elif hasattr(value, "__dict__"):
        for k, v in value.__dict__.items():
            _find_device_tensors(v, device, tensors, tensor_key + ((k, True),), seen)
    else:
        raise TypeError(
            f"Value {value} is not a dict, list, tuple, and does not have a __dict__"
        )

    return tensors


def _format_tensor_key_string(tensor_key):
    """
    Helper function to pretty-print a tensor key.

    Example: obj.x=[1, [torch.Tensor(...)], 3]; key=(("x", True), 1, 0)
    "obj" + _format_tensor_key_string(key) -> "obj.x[1][0]"
    """
    key_str = ""
    for key in tensor_key:
        if isinstance(key, int):
            key_str.append(f"[{key}]")
        else:
            key, is_attr = key
            if is_attr:
                key_str.append(f'["{key}"]')
            else:
                key_str.append(f".{key}")
    return key_str


def _find_device_tensors_with_cached_schema(
    value: Any,
    device: Optional["torch.device"],
    cached_tensor_keys: Optional[_TensorKeyMapping],
) -> Tuple[List["torch.Tensor"], _TensorKeyMapping]:
    """
    Helper function to find all nested torch.Tensors, possibly using a cached
    set of tensor keys. This function is used to avoid sending the non-tensor
    data in cases where the non-tensor data is static. We could also use pickle
    to find the tensors, but we do not have a way to enforce that pickle will
    visit each tensor in the same order.

    Returns: (list of extracted tensors, the tensor key to placeholder mapping
    that was used to extract the tensors)
    """
    # Get the keys for all nested tensors in value.
    tensors = _find_device_tensors(value, device)

    # Replace each tensor with a unique integer placeholder.
    tensor_to_placeholder: Dict["torch.Tensor", int] = {}
    tensor_key_to_placeholder: Dict[_TensorKey, int] = {}
    for key, tensor in tensors.items():
        if tensor not in tensor_to_placeholder:
            tensor_to_placeholder[tensor] = len(tensor_to_placeholder)
        tensor_key_to_placeholder[key] = tensor_to_placeholder[tensor]

    if cached_tensor_keys is not None:
        # There is a cached set of tensor keys from a previous value. Check
        # that there is a 1:1 mapping between the tensors found in this value
        # vs the tensors found in the previous value.
        extra_tensor_keys, extra_cached_tensor_keys = _compute_isomorphic_dict_diff(
            tensor_key_to_placeholder, cached_tensor_keys.to_placeholder
        )
        if extra_tensor_keys or extra_cached_tensor_keys:
            # This value has a different set of tensors from the previous value.
            direct_return_key = ()
            if direct_return_key in extra_cached_tensor_keys:
                # Case where the previous value was a direct return of a
                # torch.Tensor. We special-case this to improve the error
                # message.
                error_msg = (
                    "Expected CUDA torch.Tensor as direct return value "
                    f"for task annotated with _static_tensor_schema=True, got {value}"
                )
            elif extra_cached_tensor_keys:
                # The previous value had tensor keys not found in the current
                # value.
                values = ", ".join(
                    [
                        "value" + _format_tensor_key_string(tensor_key)
                        for tensor_key in extra_cached_tensor_keys
                    ]
                )
                error_msg = (
                    "Task annotated with _static_tensor_schema=True did not "
                    f"return tensors at [{values}]."
                )
            elif extra_tensor_keys:
                # The current value has tensor keys not found in the previous
                # value.
                values = ", ".join(
                    [
                        "value" + _format_tensor_key_string(tensor_key)
                        for tensor_key in extra_tensor_keys
                    ]
                )
                error_msg = (
                    "Task annotated with _static_tensor_schema=True returned "
                    f"extra tensors at [{values}]."
                )
            raise ValueError(error_msg)

        # TODO(swang): Check that the extracted schema matches the cached
        # schema at non-Tensor values?

    else:
        # There is no previous value, so extract the tensors in any order.
        tensor_key_list: List[_TensorKey] = [None] * len(tensor_to_placeholder)
        for key, tensor in tensors.items():
            tensor_key_list[tensor_to_placeholder[tensor]] = key
        cached_tensor_keys = _TensorKeyMapping(
            to_placeholder=tensor_key_to_placeholder,
            keys_by_transfer_order=tensor_key_list,
        )

    # Get the tensors to send, in the correct order for transfer.
    tensors = [tensors[key] for key in cached_tensor_keys.keys_by_transfer_order]

    return tensors, cached_tensor_keys


def _compute_isomorphic_dict_diff(
    dict1: Dict[_TensorKey, int], dict2: Dict[_TensorKey, int]
) -> Tuple[List[_TensorKey], List[_TensorKey]]:
    """
    Helper function to check whether two dictionaries are isomorphic. They are
    isomorphic if:
    1. They have the same keys.
    2. For each value, there is a 1:1 mapping between values of the two
    dictionaries.

    Returns: A tuple of lists, where the first list is keys that are in dict1
        that don't have a matching key-value pair in dict2, and the second list
        is keys that are in dict1 that don't have a matching key-value pair in
        dict2.

    Examples:
    {"x": 0} <> {"x": 0} -> [], []
    {"x": 1} <> {"x": 0} -> [], []
    {"x": 1} <> {"y": 0} -> ["x"], ["y"]
    {"x": 1, "y": 2} <> {"x": 2, "y": 1} -> [], []
    {"x": 1, "y": 2} <> {"x": 1, "y": 1} -> [], ["y"]
    """
    diff1 = []
    diff2 = []
    if len(dict1) != len(dict2):
        diff1 += [key for key in dict1 if key not in dict2]
        diff2 += [key for key in dict2 if key not in dict1]
        return diff1, diff2

    # Mapping from values in dict1 to values in dict2.
    map1_to_2 = {}
    # Mapping from values in dict2 to values in dict1.
    map2_to_1 = {}

    # Find all keys in dict2 that are not in dict1.
    for key, val2 in dict2.items():
        if key not in dict1:
            diff2.append(key)

    # Find all keys in dict1 that are not in dict2.
    for key, val1 in dict1.items():
        if key not in dict2:
            diff1.append(key)
            continue

        # The key exists in both dictionaries. Check that the dictionaries are
        # isomorphic for these keys.
        val2 = dict2[key]

        if val1 not in map1_to_2:
            map1_to_2[val1] = val2
        if val2 not in map2_to_1:
            map2_to_1[val2] = val1

        # The key is in both dictionaries. Check that there is a consistent
        # mapping between val1 and val2.
        if map1_to_2[val1] != val2:
            diff1.append(key)
        if map2_to_1[val2] != val1:
            diff2.append(key)

    return diff1, diff2


def _extract_device_tensors_with_pickle(
    serialization_ctx: _SerializationContext,
    global_worker: ray._private.worker.Worker,
    value: Any,
) -> Tuple["SerializedObject", List["torch.Tensor"]]:
    """
    Helper function to find all nested torch.Tensors using pickle serialization.
    """
    with serialization_ctx.external_transport():
        try:
            # Serialize the data. All tensors that match our current device
            # will be extracted into the serialization context and replaced
            # with a placeholder.
            cpu_data = global_worker.get_serialization_context().serialize(value)
        except TypeError as e:
            sio = io.StringIO()
            ray.util.inspect_serializability(value, print_file=sio)
            msg = (
                "Could not serialize the put value "
                f"{repr(value)}:\n"
                f"{sio.getvalue()}"
            )
            raise TypeError(msg) from e

        # Pop the tensors that were found during serialization of `value`.
        gpu_tensors, _ = serialization_ctx.reset_out_of_band_tensors()

    # Return the non-tensor data (with placeholders for the extracted
    # tensors) and the extracted tensors.
    return cpu_data, gpu_tensors


@DeveloperAPI
class TorchTensorNcclChannel(ChannelInterface):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        typ: "TorchTensorType",
        gpu_data_channel: "_TorchTensorNcclChannel",
        cpu_data_channel: "Channel",
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
            gpu_data_channel: A GPU-GPU channel for sending tensor data. Its
                writer and readers should match the given writer and readers.
            cpu_data_channel: A shared-memory channel for sending
                non-tensor data. Its writer and readers should match the given
                writer and readers. If None is provided, then we assume that
                there is no CPU-specific data, i.e. the task directly returned
                a CUDA torch.Tensor.
        """
        self._writer = writer
        self._reader_and_node_list = reader_and_node_list
        self._typ = typ
        # Used on the writer. If _static_tensor_schema=True, then we cache the
        # tensor keys found in the first value.
        self._cached_tensor_keys: Optional[_TensorKeyMapping] = None
        # Used on the reader. If _static_tensor_schema=True, then we cache the
        # serialized object in the Ray object store.
        self._cached_tensor_schema_ref: Optional[ray.ObjectRef] = None

        self._gpu_data_channel: _TorchTensorNcclChannel = gpu_data_channel
        assert cpu_data_channel is not None
        self._cpu_data_channel: Optional["Channel"] = cpu_data_channel

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
                None,
                None,
                self._typ,
                self._gpu_data_channel,
                self._cpu_data_channel,
            ),
        )

    def ensure_registered_as_writer(self):
        self._gpu_data_channel.ensure_registered_as_writer()
        self._cpu_data_channel.ensure_registered_as_writer()

    def ensure_registered_as_reader(self):
        self._gpu_data_channel.ensure_registered_as_reader()
        self._cpu_data_channel.ensure_registered_as_reader()

    def write(self, value: Any, timeout: Optional[float] = None) -> None:
        """
        Send a value that may contain torch.Tensors that should be sent via
        external transport.

        This method:
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
        if isinstance(value, ray.exceptions.RayTaskError):
            if self._typ.static_shape or self._typ.static_tensor_schema:
                # Raise a fatal error to teardown the DAG.
                # TODO(swang): Write exceptions to the tensor metadata or
                # non-tensor data channel if it is available.
                # TODO(swang): It would also be okay to throw this error if
                # this is still the first DAG execution, because we haven't yet
                # inferred sizes and tensor schemas.
                raise value

        tensor_keys = None
        use_pickle = not self._typ.static_tensor_schema
        if use_pickle:
            value, extracted_tensors = _extract_device_tensors_with_pickle(
                self.serialization_ctx, self._worker, value
            )
        else:
            ctx = ChannelContext.get_current()
            (extracted_tensors, tensor_keys,) = _find_device_tensors_with_cached_schema(
                value, ctx.torch_device, self._cached_tensor_keys
            )

        for tensor in extracted_tensors:
            assert tensor.is_cuda

        # First send the extracted tensors through a GPU-specific channel.
        # This could happen before or after the write of the non-tensor schema
        # data, but we do this first to avoid blocking the GPU on CPU
        # operation.
        self._gpu_data_channel.write(extracted_tensors, timeout=timeout)

        if self._cached_tensor_keys is None:
            if use_pickle:
                # If using pickle, then the tensors have already been replaced
                # with placeholders.
                extracted_tensors = None

            with self.serialization_ctx.external_transport(extracted_tensors):
                # Send the non-tensor data through a CPU-specific channel. The
                # data contains placeholders for the extracted tensors.
                self._cpu_data_channel.write(value, timeout=timeout)

            if self._typ.static_tensor_schema:
                assert tensor_keys is not None
                self._cached_tensor_keys = tensor_keys

    def read(self, timeout: Optional[float] = None) -> Any:
        """
        Read a value that may contain torch.Tensors sent via external
        transport.

        This method:
        1) Receives torch.Tensors via the tensor data channel (e.g., NCCL).
        2) Reads the serialized non-tensor data.
        3) Deserializes the non-tensor data. During deserialization, replaces
        all found placeholders with the received torch.Tensors.

        If _static_tensor_schema=True was specified, then we skip step (2) and (3) and
        directly return the data received in (1).
        """
        # First, read the tensor data.
        device_tensors = self._gpu_data_channel.read(timeout)

        with self.serialization_ctx.external_transport(device_tensors):
            # Next, read and deserialize the non-tensor data. The registered custom
            # deserializer will replace the found tensor placeholders with
            # `tensors`.
            if self._cached_tensor_schema_ref is None:
                data = self._cpu_data_channel.read(timeout)

                if self._typ.static_tensor_schema:
                    self._cached_tensor_schema_ref = ray.put(data)
            else:
                data = ray.get(self._cached_tensor_schema_ref)

        return data

    def close(self) -> None:
        self._gpu_data_channel.close()
        self._cpu_data_channel.close()


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
            typ.nccl_group_id, str
        ), "NCCL group ID ({nccl_group_id}) must be a str."
        self._typ = typ

        assert self._typ.nccl_group_id is not None, "No NCCL group specified."
        self._nccl_group_id: str = self._typ.nccl_group_id
        self._nccl_group: "GPUCommunicator" = ctx.nccl_groups[self._typ.nccl_group_id]
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
                False,
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
        if self._nccl_group_id in ctx.nccl_groups:
            del ctx.nccl_groups[self._nccl_group_id]


def _do_init_nccl_group(
    self,
    group_id,
    world_size,
    comm_id,
    rank,
    actor_handles,
    use_communication_streams,
    custom_nccl_group: Optional[GPUCommunicator] = None,
):
    import torch

    assert (
        ray.get_gpu_ids()
    ), "Actors participating in NCCL group must have at least one GPU assigned"

    ctx = ChannelContext.get_current()
    if custom_nccl_group is not None:
        custom_nccl_group.initialize(rank)
        ctx.nccl_groups[group_id] = custom_nccl_group
    else:
        ctx.nccl_groups[group_id] = _NcclGroup(
            world_size,
            comm_id,
            rank,
            actor_handles,
            torch.cuda.current_stream().cuda_stream,
            use_communication_streams,
        )


def _do_destroy_nccl_group(self, group_id):
    ctx = ChannelContext.get_current()
    if group_id not in ctx.nccl_groups:
        return

    ctx.nccl_groups[group_id].destroy()

    # Keep the NCCL group in the map after destruction in case there is still a
    # task loop running.


def _do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _do_get_unique_nccl_id(self) -> bool:
    from cupy.cuda import nccl

    return nccl.get_unique_id()


def _get_ranks(
    actors: List[ray.actor.ActorHandle], custom_nccl_group: Optional[GPUCommunicator]
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


def _init_nccl_group(
    actors: List[ray.actor.ActorHandle],
    custom_nccl_group: Optional[GPUCommunicator] = None,
    use_communication_streams: bool = False,
) -> str:
    """
    Initialize a NCCL group with the given actors. If a custom NCCL group is
    provided, then it will be used, otherwise a new NCCL group will be created.

    Args:
        actors: A list of actors that participate in the NCCL group.
        custom_nccl_group: A custom NCCL group to initialize.
        use_communication_streams: Whether to use dedicated send and recv
                streams for communication. If True, communication and computation
                can be overlapped to improve perfomrance.
    """
    ctx = ChannelContext.get_current()

    has_gpus = ray.get(
        [actor.__ray_call__.remote(_do_check_has_gpu) for actor in actors]
    )
    for has_gpu, actor in zip(has_gpus, actors):
        if not has_gpu:
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
    nccl_comm_id = ray.get(actors[0].__ray_call__.remote(_do_get_unique_nccl_id))
    # Used to uniquely identify this NCCL group.
    group_id = str(uuid.uuid4())

    if custom_nccl_group is not None:
        logger.info(f"Initializing custom NCCL group {group_id} on actors: {actors}")
    else:
        logger.info(f"Creating NCCL group {group_id} on actors: {actors}")

    world_size = len(actors)
    ranks = _get_ranks(actors, custom_nccl_group)
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_nccl_group,
            group_id,
            world_size,
            nccl_comm_id,
            rank,
            actors,
            use_communication_streams,
            custom_nccl_group,
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

    if custom_nccl_group is not None:
        ctx.nccl_groups[group_id] = custom_nccl_group
    else:
        ctx.nccl_groups[group_id] = _NcclGroup(
            world_size,
            nccl_comm_id,
            rank=None,
            actor_handles=actors,
            cuda_stream=None,
        )
    return group_id


def _destroy_nccl_group(group_id: str) -> None:
    """
    Destroy the NCCL group with the given ID.
    """
    ctx = ChannelContext.get_current()
    if group_id not in ctx.nccl_groups:
        return

    group = ctx.nccl_groups[group_id]
    actors = group.get_actor_handles()
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
