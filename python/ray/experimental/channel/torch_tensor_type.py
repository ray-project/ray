import logging
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelContext, ChannelOutputType
from ray.experimental.channel.gpu_communicator import GPUCommunicator
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
        transport: Optional[Union[str, GPUCommunicator]] = AUTO,
        _static_shape: bool = False,
        _direct_return: Optional[bool] = False,
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
            _direct_return: Whether the tensor is sent directly or inside of
                other data. If a non-default `transport` is used, this allows
                the sender and receiver to eliminate performance overhead from
                an additional data transfer.

        NOTE: Setting static_shape=True and _direct_return=True can improve
        performance if a non-default transport is used. However, if either flag
        is set, then the user must ensure that the condition is met.

        If using this type as an accelerated DAG annotation, an exception will
        be thrown in the following cases, and the DAG will be torn down. To
        continue execution, a new DAG must be created:
        1. If _static_shape=True, and the found tensors don't match the
           previous shape or dtype(s).
        2. If _direct_return=True, and the returned value is not a
           torch.Tensor.
        """
        super().__init__()

        self._static_shape = _static_shape
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

        if self._static_shape and self.transport == self.AUTO:
            logger.info(
                "TorchTensorType(_static_shape=True) has no effect when "
                "`transport` is TorchTensorType.AUTO (default)."
            )
        if self._direct_return and self.transport == self.AUTO:
            logger.info(
                "TorchTensorType(_direct_return=True) has no effect when "
                "`transport` is TorchTensorType.AUTO (default)."
            )

    @property
    def static_shape(self):
        return self._static_shape

    @property
    def direct_return(self):
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
        driver_actor_id: Optional[str] = None,
        _cpu_data_channel: Optional["Channel"] = None,
        _tensor_metadata_channel: Optional["Channel"] = None,
    ) -> type:
        if self.requires_nccl():
            from ray.experimental.channel.torch_tensor_nccl_channel import (
                TorchTensorNcclChannel,
                _TorchTensorNcclChannel,
            )

            gpu_data_channel = _TorchTensorNcclChannel(
                writer,
                reader_and_node_list,
                self,
                _meta_channel=_tensor_metadata_channel,
            )

            if _cpu_data_channel is None and not self._direct_return:
                # Create a CPU channel to send non-tensor data.
                _cpu_data_channel = SharedMemoryType().create_channel(
                    writer, reader_and_node_list, driver_actor_id
                )

            return TorchTensorNcclChannel(
                writer,
                reader_and_node_list,
                self,
                gpu_data_channel,
                _cpu_data_channel,
            )

        # Data does not require NCCL. Transfer via host memory using a
        # shared-memory channel.
        # TODO(swang): Allow the initial max buffer size to be overridden.
        typ = SharedMemoryType()
        return typ.create_channel(writer, reader_and_node_list, driver_actor_id)

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
            transport=self.transport,
            _static_shape=self._static_shape,
            _direct_return=self._direct_return,
        )
        copy._custom_nccl_group = self._custom_nccl_group
        copy._nccl_group_id = self._nccl_group_id
        return copy
