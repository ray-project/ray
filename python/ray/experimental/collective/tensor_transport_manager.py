from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

import ray
from ray._private.custom_types import TensorTransportEnum
from ray.util.collective.types import (
    Backend,
    CommunicatorMetadata,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch


class TensorTransportManager(ABC):
    @property
    @abstractmethod
    def tensor_transport_backend(self) -> Backend:
        """The tensor transport backend, e.g., NCCL.

        Returns:
            Backend: The backend of the tensor transport.
        """

    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        """Whether the backend is one-sided.

        Returns:
            bool: True if the backend is one-sided, False otherwise.
        """

    @abstractmethod
    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        """Whether the actor has the tensor transport available.

        Args:
            actor: The actor to check.

        Returns:
            bool: True if the actor has the tensor transport available, False otherwise.
        """

    @staticmethod
    @abstractmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport: TensorTransportEnum,
    ) -> TensorTransportMetadata:
        """
        Get the tensor transport metadata for the GPU object.
        This function retrieves metadata about tensors stored in the GPU object store,
        including their shapes, dtypes, and any transport-specific metadata, e.g., NIXL descriptors.

        Args:
            src_actor: The actor that runs this function.
            obj_id: The ID of the GPU object to get metadata for
            tensor_transport: The tensor transport protocol to use for the GPU object.

        Returns:
            TensorTransportMetadata: A named tuple containing the tensor metadata.
        """

    @staticmethod
    @abstractmethod
    def get_communicator_metadata(
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        """
        Get the communicator metadata (e.g. communicator name, src/dst rank) for the send/recv operation.
        This function is called before sending the GPU object.

        Args:
            src_actor: The actor that runs this function.
            dst_actor: The actor that runs this function.
            backend: The backend to use for the collective operation.

        Returns:
            CommunicatorMetadata: The communicator metadata.
        """

    @staticmethod
    def send_object(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        communicator_metadata_ref: CommunicatorMetadata,
    ):
        """
        Send the GPU object to the destination actor.

        Args:
            src_actor: The actor that runs this function.
            obj_id: The ID of the GPU object to send.
            tensor_transport_meta: The tensor transport metadata for the GPU object.
            communicator_metadata_ref: The ObjectRef of communicator metadata for the send/recv operation.
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_send__

        # Send tensors stored in the `src_actor`'s GPU object store to the
        # destination rank `dst_rank`.
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking the data transfer.
        src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_send__,
            obj_id,
            tensor_transport_meta,
            communicator_metadata_ref,
        )

    @staticmethod
    def recv_object(
        dst_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata_ref: TensorTransportMetadata,
        communicator_metadata_ref: CommunicatorMetadata,
    ):
        """
        Receive the GPU object from the source actor.
        This function receives tensors from the source rank and stores them in the
        `dst_actor`'s GPU object store.

        Args:
            dst_actor: The actor that runs this function.
            obj_id: The ID of the GPU object to receive.
            tensor_transport_metadata_ref: The ObjectRef of tensor transport metadata for the GPU object.
            communicator_metadata_ref: The ObjectRef of communicator metadata for the send/recv operation.
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_recv__

        # Receive tensors from the source rank and store them in the
        # `dst_actor`'s GPU object store.
        #
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking the data transfer. Technically,
        # this is only needed for the sender task, but we put the receiver task
        # on the same background thread to ensure that all communication
        # operations are executed in a global order.
        dst_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_recv__,
            obj_id,
            tensor_transport_metadata_ref,
            communicator_metadata_ref,
        )

    @staticmethod
    @abstractmethod
    def recv_multiple_tensors(
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Receive multiple tensors from the source actor.

        Args:
            tensors: The pre-allocated tensor space to receive the tensors.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.

        """

    @staticmethod
    @abstractmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Send multiple tensors to the destination actor.

        Args:
            tensors: The tensors to send.
            communicator_metadata: The communicator metadata for the send/recv operation.
        """
