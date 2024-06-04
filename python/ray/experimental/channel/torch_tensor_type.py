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
        transport: Optional[str] = AUTO,
        static_shape: bool = False,
        static_non_tensor_data: bool = False,
    ):
        """
        A type hint that can be used to annotate DAG nodes that return a
        torch.Tensor.

        NOTE: Use of this type in the DAG will register a custom serializer for
        torch.Tensor that moves the tensor to the correct device on the
        receiver. If you are using ray.cloudpickle to serialize objects and you
        do not want this behavior, deregister the custom serializer using
        ray.util.serialization.deregister_serializer(torch.Tensor).

        Args:
            transport: "auto" (default) means that tensors will be passed via
                host memory, using numpy as the serialization format. Pass
                TorchTensorType.NCCL or "nccl" to use NCCL instead, avoiding
                the host memory copy.
            static_shape: A hint indicating whether the shape(s) and dtype(s)
                of tensor(s) contained in this value always remain the same
                across different executions of the DAG.
            static_non_tensor_data: A hint indicating whether the non-tensor
                data contained in this value always remains the same across
                different executions of the DAG. For example, if the value
                always has the form `{"my_tensor": torch.Tensor(...)}`, then
                this can be set to True, even if the size of the contained
                tensor changes.

        NOTE: Setting static_shape=True and/or static_non_tensor_data=True can
        improve performance if a non-default transport is used. However, if
        either flag is set, then the user must ensure that the condition is
        met. Also, for values containing multiple tensors, the user must ensure
        that the (ray.cloudpickle) serialization order of the value is
        deterministic. Otherwise, reads may return different values from those
        written. For example, if returning a dictionary with multiple tensors,
        use Python 3.6+ and ensure that the insertion order is the same.
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
        _non_tensor_data_channel: Optional["Channel"] = None,
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

            if _non_tensor_data_channel is None:
                _non_tensor_data_channel = SharedMemoryType().create_channel(
                    writer, readers
                )

            return TorchTensorNcclChannel(
                writer,
                readers,
                tensor_data_channel,
                _non_tensor_data_channel,
                self.static_non_tensor_data,
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
