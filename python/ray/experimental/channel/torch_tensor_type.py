import logging
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import ray
from ray.experimental.channel import ChannelContext, ChannelOutputType
from ray.experimental.channel.communicator import Communicator
from ray.experimental.channel.shared_memory_channel import SharedMemoryType
from ray.experimental.util.types import Device
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.experimental.channel.shared_memory_channel import Channel

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TorchTensorType(ChannelOutputType):
    AUTO = "auto"
    CPU = "cpu"
    ACCELERATOR = "accelerator"

    def __init__(
        self,
        transport: Optional[Union[str, Communicator]] = AUTO,
        device: Device = Device.DEFAULT,
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
                TorchTensorType.ACCELERATOR or "accelerator" to use accelerator
                instead, avoiding the host memory copy.
            device: Target device for tensor transport. Options:
                - "default": Retains the same device type as the sender.
                - "cpu": Moves tensor to CPU on the receiver. Not compatible
                  with accelerator transport.
                - "gpu" or "cuda": Moves tensor to GPU on the receiver.
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

        If using this type as a Compiled Graph annotation, an exception will
        be thrown in the following cases, and the DAG will be torn down. To
        continue execution, a new DAG must be created:
        1. If _static_shape=True, and the found tensors don't match the
           previous shape or dtype(s).
        2. If _direct_return=True, and the returned value is not a
           torch.Tensor.
        """
        super().__init__()

        self._device = device
        self._static_shape = _static_shape
        self._direct_return = _direct_return

        self._communicator: Optional[Communicator] = None
        if isinstance(transport, Communicator):
            self._communicator = transport
            transport = transport.get_transport_name()

        if transport not in [self.AUTO, self.CPU, self.ACCELERATOR]:
            raise ValueError(
                "`transport` must be TorchTensorType.AUTO, TorchTensorType.ACCELERATOR "
                "or TorchTensorType.CPU"
            )
        if device == Device.CPU and transport == self.ACCELERATOR:
            raise ValueError(
                "accelerator transport is not supported with CPU target device."
            )
        self.transport = transport

        self._communicator_id: Optional[str] = None

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
    def device(self) -> Device:
        return self._device

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
            return ctx.serialization_context.deserialize_tensor(b, self.device)

        ray.util.serialization.register_serializer(
            torch.Tensor,
            serializer=serialize,
            deserializer=deserialize,
        )

    def create_channel(
        self,
        writer: Optional["ray.actor.ActorHandle"],
        reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
        driver_actor_id: Optional[str] = None,
        _cpu_data_channel: Optional["Channel"] = None,
        _tensor_metadata_channel: Optional["Channel"] = None,
    ) -> type:
        if self.requires_accelerator():
            from ray.experimental.channel.torch_tensor_accelerator_channel import (
                TorchTensorAcceleratorChannel,
            )

            return TorchTensorAcceleratorChannel(
                writer,
                reader_and_node_list,
                self,
                driver_actor_id,
                _tensor_metadata_channel,
                _cpu_data_channel,
            )

        # Data does not require accelerator. Transfer via host memory using a
        # shared-memory channel.
        # TODO(swang): Allow the initial max buffer size to be overridden.
        typ = SharedMemoryType()
        return typ.create_channel(writer, reader_and_node_list, driver_actor_id)

    def requires_accelerator(self) -> bool:
        return self.transport == self.ACCELERATOR

    def get_custom_communicator(self) -> Optional[Communicator]:
        """
        Return the communicator group if one is specified.
        """
        return self._communicator

    def set_communicator_id(self, group_id: str) -> None:
        self._communicator_id = group_id

    @property
    def communicator_id(self) -> Optional[str]:
        return self._communicator_id

    def __deepcopy__(self, memo):
        """
        Deep copy all the fields except for the communicator group. The communicator
        group should not be deep copied because it can be shared across `TorchTensorType`
        instances.
        """
        copy = TorchTensorType(
            transport=self.transport,
            _static_shape=self._static_shape,
            _direct_return=self._direct_return,
        )
        copy._communicator = self._communicator
        copy._communicator_id = self._communicator_id
        return copy
