import logging
from typing import TYPE_CHECKING, List, Optional, Tuple

import ray
from ray.experimental.channel import ChannelContext, ChannelOutputType
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.experimental.channel.shared_memory_channel import Channel


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TorchTensorType(ChannelOutputType):
    AUTO = "auto"
    NCCL = "nccl"

    def __init__(
        self,
        transport: Optional[str] = AUTO,
        _static_shape: bool = False,
        _static_non_tensor_data: bool = False,
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
            _static_shape: A hint indicating whether the shape(s) and dtype(s)
                of tensor(s) contained in this value always remain the same
                across different executions of the DAG.
            _static_non_tensor_data: A hint indicating whether the non-tensor
                data contained in this value always remains the same across
                different executions of the DAG. For example, if the value
                always has the form `{"my_tensor": torch.Tensor(...)}`, then
                this can be set to True, even if the size of the contained
                tensor changes.

        NOTE: Setting static_shape=True and/or static_non_tensor_data=True can
        improve performance if a non-default transport is used. However, if
        either flag is set, then the user must ensure that the condition is
        met. Also, for values containing multiple tensors, the
        user must ensure that the (ray.cloudpickle) serialization order of the
        value is deterministic. Otherwise, reads may return different values
        from those written. For example, if returning a dictionary with
        multiple tensors, use Python 3.6+ and ensure that the insertion order
        is the same.

        If using this type as an accelerated DAG annotation, an exception will
        be thrown in the following cases, and the DAG will be torn down. To
        continue execution, a new DAG must be created:
        1. If static_shape=True, and the found tensors don't match the
           previous shape or dtype(s).
        2. If static_non_tensor_data=True, and a different number of tensors is
           found.
        """
        super().__init__()

        self._static_shape = _static_shape
        self._static_non_tensor_data = _static_non_tensor_data

        if transport is None:
            transport = self.AUTO
        if transport not in [self.AUTO, self.NCCL]:
            raise ValueError(
                "`transport` must be TorchTensorType.AUTO or TorchTensorType.NCCL"
            )
        self.transport = transport

        self._nccl_group_id: Optional[str] = None

        if self._static_shape and self.transport == self.AUTO:
            logger.info(
                "TorchTensorType(_static_shape=True) has no effect when "
                "`transport` is TorchTensorType.AUTO (default)."
            )
        if self._static_non_tensor_data and self.transport == self.AUTO:
            logger.info(
                "TorchTensorType(_static_non_tensor_data=True) has no effect when "
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
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        _non_tensor_data_channel: Optional["Channel"] = None,
        _tensor_metadata_channel: Optional["Channel"] = None,
    ) -> type:
        if self.requires_nccl():
            from ray.experimental.channel.torch_tensor_nccl_channel import (
                TorchTensorNcclChannel,
                _TorchTensorNcclChannel,
            )

            tensor_data_channel = _TorchTensorNcclChannel(
                writer,
                reader_and_node_list,
                self,
                _meta_channel=_tensor_metadata_channel,
            )

            if _non_tensor_data_channel is None:
                _non_tensor_data_channel = SharedMemoryType().create_channel(
                    writer, reader_and_node_list
                )

            return TorchTensorNcclChannel(
                writer,
                reader_and_node_list,
                tensor_data_channel,
                _non_tensor_data_channel,
                self.static_non_tensor_data,
            )

        # Data does not require NCCL. Transfer via host memory using a
        # shared-memory channel.
        # TODO(swang): Allow the initial max buffer size to bereaders overridden.
        typ = SharedMemoryType()
        return typ.create_channel(writer, reader_and_node_list)

    def requires_nccl(self) -> bool:
        return self.transport == self.NCCL

    def set_nccl_group_id(self, group_id: str) -> None:
        self._nccl_group_id = group_id

    @property
    def nccl_group_id(self) -> str:
        return self._nccl_group_id
