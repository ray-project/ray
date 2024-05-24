from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelContext, ChannelOutputType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch

# 100KB to store metadata and/or exceptions.
# NOTE(swang): This will consume memory but it should not affect performance
# because we only copy the actual data stored, not the maximum size of the
# shared meomry buffer.
TENSOR_METADATA_SIZE_BYTES = 100_000


def _get_default_torch_device() -> "torch.device":
    from ray.air._internal import torch_utils

    if not ray.get_gpu_ids():
        import torch

        # torch_utils defaults to returning GPU 0 if no GPU IDs were assigned
        # by Ray. We instead want the default to be CPU.
        return torch.device("cpu")

    return torch_utils.get_devices()[0]


@PublicAPI(stability="alpha")
class TorchTensorType(ChannelOutputType):
    AUTO = "auto"
    NCCL = "nccl"

    def __init__(
        self,
        shape: Union[int, Tuple[int], str] = AUTO,
        dtype: "torch.dtype" = AUTO,
        transport: Optional[str] = None,
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
            shape: The expected shape of the torch.Tensor. "auto" (default)
                means that the shape will be dynamically inferred. For tensors
                passed via host memory (default), the shape is a hint for the
                maximum size of the tensor. If a DAG node's returned serialized
                tensor exceeds this size, the task will error. For tensors
                passed via NCCL, the returned tensor must *match* the given
                shape; if it does not match, the task will error.
            dtype: The expected dtype of the torch.Tensor. Similar to the
                shape, this may be statically or dynamically declared.
            transport: "auto" (default) means that tensors will be passed via
                host memory, using numpy as the serialization format. Pass
                TorchTensorType.NCCL or "nccl" to use NCCL instead, avoiding
                the host memory copy.
        """
        super().__init__()

        if isinstance(shape, str):
            shape = shape.lower()
        if isinstance(dtype, str):
            dtype = dtype.lower()

        self.shape = shape
        self.dtype = dtype
        self.transport = transport
        self._nccl_group_id: Optional[str] = None

    def register_custom_serializer(self) -> None:
        import torch

        default_device = _get_default_torch_device()
        ctx = ChannelContext.get_current()
        ctx.serialization_context.set_torch_device(default_device)

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

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        readers: List[Optional["ray.actor.ActorHandle"]],
    ) -> type:
        if self.requires_nccl():
            from ray.experimental.channel.torch_tensor_nccl_channel import (
                TorchTensorNcclChannel,
            )

            return TorchTensorNcclChannel(writer, readers, self)

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

        shape = self.shape
        if isinstance(shape, int):
            shape = (shape,)

        num_elements = 1
        for dim in shape:
            num_elements *= dim
        element_size_bytes = TORCH_DTYPE_ITEMSIZE_MAP[self.dtype]
        buffer_size_bytes = int(num_elements * element_size_bytes)
        buffer_size_bytes += TENSOR_METADATA_SIZE_BYTES

        return Channel(writer, readers, buffer_size_bytes)

    def requires_nccl(self) -> bool:
        return self.transport == self.NCCL

    def set_nccl_group_id(self, group_id: str) -> None:
        self._nccl_group_id = group_id

    @property
    def nccl_group_id(self) -> str:
        return self._nccl_group_id
