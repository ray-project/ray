import logging
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelContext, ChannelInterface, ChannelOutputType
from ray.experimental.channel.gpu_communicator import (
    GPUCommunicator,
    TorchTensorAllocator,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)

# 100KB to store metadata and/or exceptions.
# NOTE(swang): This will consume memory but it should not affect performance
# because we only copy the actual data stored, not the maximum size of the
# shared meomry buffer.
TENSOR_METADATA_SIZE_BYTES = 100_000


@PublicAPI(stability="alpha")
class TorchTensorType(ChannelOutputType):
    AUTO = "auto"
    NCCL = "nccl"

    def __init__(
        self,
        _shape: Union[int, Tuple[int], str] = AUTO,
        _dtype: "torch.dtype" = AUTO,
        transport: Optional[Union[str, GPUCommunicator]] = AUTO,
        _direct_return: Optional[bool] = False,
    ):
        """
        A type hint that can be used to annotate DAG nodes that return a
        torch.Tensor.

        NOTE: Use of this type in the DAG will register a custom serializer for
        torch.Tensor that moves the tensor to the correct device on the
        receiver. If you are using ray.cloudpickle to serialize objects and you
        do not want this behavior, you should deregister the custom serializer
        using ray.util.serialization.deregister_serializer(torch.Tensor).

        Args:
            _shape: The expected shape of the torch.Tensor. "auto" (default)
                means that the shape will be dynamically inferred. For tensors
                passed via host memory (default), the shape is a hint for the
                maximum size of the tensor. If a DAG node's returned serialized
                tensor exceeds this size, the task will error. For tensors
                passed via NCCL, the returned tensor must *match* the given
                shape; if it does not match, the task will error. Specifying
                the shape and dtype ahead of time will eliminate the
                performance overhead from an additional metadata transfer.
            _dtype: The expected dtype of the torch.Tensor. Similar to the
                shape, this may be statically or dynamically declared.
            transport: "auto" (default) means that tensors will be passed via
                host memory, using numpy as the serialization format. Pass
                TorchTensorType.NCCL or "nccl" to use NCCL instead, avoiding
                the host memory copy.
            _direct_return: Whether the tensor is sent directly or inside of
                other data. If a non-default `transport` is used, this allows
                the sender and receiver to eliminate performance overhead from
                an additional data transfer.
        """
        super().__init__()

        if isinstance(_shape, str):
            _shape = _shape.lower()
        if isinstance(_dtype, str):
            _dtype = _dtype.lower()

        self._shape = _shape
        self._dtype = _dtype
        self._direct_return = _direct_return

        self._custom_nccl_group: Optional[GPUCommunicator] = None
        if isinstance(transport, GPUCommunicator):
            self._custom_nccl_group = transport
            transport = self.NCCL

        if transport not in [self.AUTO, self.NCCL]:
            raise ValueError(
                "`transport` must be TorchTensorType.AUTO or TorchTensorType.NCCL"
            )
        self.transport = transport

        self._nccl_group_id: Optional[str] = None

        if self._direct_return and self.transport == self.AUTO:
            logger.info(
                "TorchTensorType(_direct_return=True) has no effect when "
                "`transport` is TorchTensorType.AUTO (default)."
            )

    @property
    def is_direct_return(self) -> bool:
        return self._direct_return

    def register_custom_serializer(self) -> None:
        super().register_custom_serializer()

        import torch

        def serialize(t):
            ctx = ChannelContext.get_current()
            return ctx.serialization_context.serialize_tensor(t)

        def deserialize(b):
            ctx = ChannelContext.get_current()
            return ctx.serialization_context.deserialize_tensor(b)

        ray.util.serialization.register_serializer(
            torch.Tensor,
            serializer=serialize,
            deserializer=deserialize,
        )

    def set_contains_type(self, typ: "ChannelOutputType") -> None:
        raise ValueError("TorchTensorType cannot contain other types")

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        read_by_adag_driver,
        _torch_tensor_allocator: Optional["TorchTensorAllocator"] = None,
    ) -> ChannelInterface:
        if self.requires_nccl():
            from ray.experimental.channel.torch_tensor_nccl_channel import (
                TorchTensorNcclChannel,
            )

            return TorchTensorNcclChannel(
                writer,
                reader_and_node_list,
                self,
                _torch_tensor_allocator=_torch_tensor_allocator,
            )

        # Transfer via host memory using a shared-memory channel.
        import torch

        from ray.experimental.channel.shared_memory_channel import Channel

        TORCH_DTYPE_ITEMSIZE_MAP = {
            # INT types
            torch.int: 4,
            torch.uint8: 1,
            torch.int8: 1,
            torch.int32: 4,
            torch.int64: 8,
            torch.long: 8,
            # FLOAT types
            torch.half: 2,
            torch.float: 4,
            torch.float16: 2,
            torch.bfloat16: 2,
            torch.float32: 4,
            torch.float64: 8,
            torch.double: 8,
        }

        shape = self._shape
        if isinstance(shape, int):
            shape = (shape,)

        num_elements = 1
        for dim in shape:
            num_elements *= dim
        element_size_bytes = TORCH_DTYPE_ITEMSIZE_MAP[self._dtype]
        buffer_size_bytes = int(num_elements * element_size_bytes)
        buffer_size_bytes += TENSOR_METADATA_SIZE_BYTES

        return Channel(writer, reader_and_node_list, buffer_size_bytes)

    def requires_nccl(self) -> bool:
        return self.transport == self.NCCL

    def get_custom_nccl_group(self) -> Optional[GPUCommunicator]:
        """
        Return the custom NCCL group if one is specified.
        """
        return self._custom_nccl_group

    def set_nccl_group_id(self, group_id: str) -> None:
        self._nccl_group_id = group_id

    @property
    def nccl_group_id(self) -> Optional[str]:
        return self._nccl_group_id

    def __deepcopy__(self, memo):
        """
        Deep copy all the fields except for the custom NCCL group. The custom
        NCCL group should not be deep copied because it can be shared across
        `TorchTensorType` instances.
        """
        copy = TorchTensorType(
            _shape=self._shape,
            _dtype=self._dtype,
            transport=self.transport,
            _direct_return=self._direct_return,
        )
        copy._custom_nccl_group = self._custom_nccl_group
        copy._nccl_group_id = self._nccl_group_id
        return copy
