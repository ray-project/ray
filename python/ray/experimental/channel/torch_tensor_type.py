import logging
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.channel import ChannelContext, ChannelOutputType
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch

    from ray.experimental.channel.shared_memory_channel import Channel
    from ray.experimental.channel.torch_tensor_nccl_channel import TorchTensorAllocator

logger = logging.getLogger(__name__)


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
        static_shape: bool = False,
        static_non_tensor_data: bool = False,
        transport: Optional[str] = AUTO,
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
        # TODO(swang)
            shape: The expected shape of the torch.Tensor. "auto" (default)
                means that the shape will be dynamically inferred. For tensors
                passed via host memory (default), the shape is a hint for the
                maximum size of the tensor. If a DAG node's returned serialized
                tensor exceeds this size, the task will error. For tensors
                passed via NCCL, the returned tensor must *match* the given
                shape; if it does not match, the task will error. Specifying
                the shape and dtype ahead of time will eliminate the
                performance overhead from an additional metadata transfer.
            dtype: The expected dtype of the torch.Tensor. Similar to the
                shape, this may be statically or dynamically declared.
            transport: "auto" (default) means that tensors will be passed via
                host memory, using numpy as the serialization format. Pass
                TorchTensorType.NCCL or "nccl" to use NCCL instead, avoiding
                the host memory copy.
            direct_return: Whether the tensor is sent directly or inside of
                other data. If a non-default `transport` is used, this allows
                the sender and receiver to eliminate performance overhead from
                an additional data transfer.
        """
        super().__init__()

        self._static_shape = static_shape
        self._static_non_tensor_data = static_non_tensor_data

        if transport not in [self.AUTO, self.NCCL]:
            raise ValueError(
                "`transport` must be TorchTensorType.AUTO or TorchTensorType.NCCL"
            )
        self.transport = transport

        self._nccl_group_id: Optional[str] = None

        if self.static_non_tensor_data and self.transport == self.AUTO:
            logger.info(
                "TorchTensorType(direct_return=True) has no effect when "
                "`transport` is TorchTensorType.AUTO (default)."
            )

    @property
    def static_shape(self):
        return self._static_shape

    @property
    def static_non_tensor_data(self):
        return self._static_non_tensor_data

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
        readers: List[Optional["ray.actor.ActorHandle"]],
        _non_tensor_data_typ: Optional[SharedMemoryType] = None,
        _tensor_metadata_channel: Optional["Channel"] = None,
        _torch_tensor_allocator: Optional["TorchTensorAllocator"] = None,
    ) -> type:
        if self.requires_nccl():
            from ray.experimental.channel.torch_tensor_nccl_channel import (
                TorchTensorNcclChannel,
                _TorchTensorNcclChannel,
            )

            tensor_data_channel = _TorchTensorNcclChannel(
                writer,
                readers,
                self._nccl_group_id,
                self._static_shape,
                _meta_channel=_tensor_metadata_channel,
                _torch_tensor_allocator=_torch_tensor_allocator,
            )

            if _non_tensor_data_typ is None:
                _non_tensor_data_typ = SharedMemoryType()
            non_tensor_data_channel = _non_tensor_data_typ.create_channel(
                writer, readers
            )

            return TorchTensorNcclChannel(
                writer,
                readers,
                tensor_data_channel,
                non_tensor_data_channel,
            )

        # Data does not require NCCL. Transfer via host memory using a
        # shared-memory channel.
        # TODO(swang): Allow the initial max buffer size to be overridden.
        typ = SharedMemoryType()
        return typ.create_channel(writer, readers)

    def requires_nccl(self) -> bool:
        return self.transport == self.NCCL

    def set_nccl_group_id(self, group_id: str) -> None:
        self._nccl_group_id = group_id

    @property
    def nccl_group_id(self) -> str:
        return self._nccl_group_id
