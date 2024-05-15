from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelContext, ChannelOutputType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch

# Add this much padding to all shared memory buffers used to store tensors.
# This shouldn't affect performance because the buffer is allocated once and
# when transferring across nodes, only the serialized buffer is copied.
TENSOR_BUFFER_PADDING_FRACTION = 0.2
# 100KB so that we have room to store an exception if needed.
MIN_TENSOR_BUFFER_SIZE = 100_000


def _get_default_torch_device() -> "torch.device":
    from ray.air._internal import torch_utils

    if ray.get_gpu_ids():
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
        shape: Union[int, Tuple[int], str],
        dtype: "torch.dtype",
        transport: Optional[str] = None,
    ):
        super().__init__()

        self.shape = shape
        self.dtype = dtype
        self.transport = transport
        self.nccl_group_id: Optional[str] = None

    def register_custom_serializer(self) -> None:
        super().register_custom_serializer()

        import torch

        default_device = _get_default_torch_device()
        ctx = ChannelContext.get_current()
        ctx.serialization_context.set_torch_device(default_device)

        ray.util.serialization.register_serializer(
            torch.Tensor,
            serializer=ctx.serialization_context.serialize_tensor,
            deserializer=ctx.serialization_context.deserialize_tensor,
        )

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        readers: List[Optional["ray.actor.ActorHandle"]],
    ) -> type:
        if self.transport == self.NCCL:
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
        buffer_size_bytes = int(
            (num_elements * element_size_bytes) * (1 + TENSOR_BUFFER_PADDING_FRACTION)
        )
        buffer_size_bytes = max(buffer_size_bytes, MIN_TENSOR_BUFFER_SIZE)

        return Channel(writer, readers, buffer_size_bytes)

    def requires_nccl(self) -> bool:
        return self.transport == self.NCCL

    def set_nccl_group_id(self, group_id: str) -> None:
        self.nccl_group_id = group_id


# class _TorchTensorPlaceholder:
#    def __init__(
#        self,
#        typ: TorchTensorType,
#    ):
#        import torch
#
#        if not isinstance(tensor, torch.Tensor):
#            raise ValueError(
#                "DAG nodes wrapped with ray.experimental.TorchTensor must return a "
#                "torch.Tensor."
#            )
#        if typ.shape != TorchTensorType.AUTO and tensor.shape != typ.shape:
#            raise ValueError(
#                "DAG node wrapped with ray.experimental.TorchTensor(shape="
#                f"{typ.shape}) returned "
#                f"a torch.Tensor of the shape {tensor.shape}"
#            )
#        if typ.shape != TorchTensorType.AUTO and tensor.dtype != typ.dtype:
#            raise ValueError(
#                "DAG node wrapped with ray.experimental.TorchTensor(dtype="
#                f"{typ.dtype}) returned "
#                f"a torch.Tensor of the dtype {tensor.dtype}"
#            )
#
#        self.tensor = tensor
#        self.typ = typ
