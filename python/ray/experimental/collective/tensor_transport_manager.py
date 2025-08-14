from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING
from ray.util.collective.types import TensorTransportMetadata
from ray._private.custom_types import TensorTransportEnum

import ray

if TYPE_CHECKING:
    import torch


class TensorTransportManager(ABC):
    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        """Whether the backend is one-sided.

        Returns:
            bool: True if the backend is one-sided, False otherwise.
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
    def get_collective_metadata(
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        tensor_transport_metadata: TensorTransportMetadata,
        backend: Optional[str] = None,
    ) -> TensorTransportMetadata:
        """
        Update the collective metadata (e.g. communicator name, src/dst rank) in
        `tensor_transport_metadata` and return the updated metadata.
        This function is called before sending the GPU object.

        Args:
            src_actor: The actor that runs this function.
            dst_actor: The actor that runs this function.
            tensor_transport_metadata: The reference of tensor transport metadata for the GPU object.
            backend: The backend to use for the collective operation.

        Returns:
            TensorTransportMetadata: The updated tensor transport metadata. Note that it's not a reference.
        """

    @staticmethod
    def send_object(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
    ):
        """
        Send the GPU object to the destination actor.
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_send__

        # Send tensors stored in the `src_actor`'s GPU object store to the
        # destination rank `dst_rank`.
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking the data transfer.
        src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_send__,
            obj_id,
            tensor_transport_metadata,
        )

    @staticmethod
    def recv_object(
        dst_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
    ):
        """
        Receive the GPU object from the source actor.
        This function receives tensors from the source rank and stores them in the
        `dst_actor`'s GPU object store.

        Args:
            dst_actor: The actor that runs this function.
            obj_id: The ID of the GPU object to receive.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
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
            __ray_recv__, obj_id, tensor_transport_metadata
        )

    @staticmethod
    @abstractmethod
    def recv_multiple_tensors(
        tensors: List["torch.Tensor"],
        metadata: TensorTransportMetadata,
        group_name: str = "default",
    ):
        """
        Receive multiple tensors from the source actor.

        Args:
            tensors: The pre-allocated tensor space to receive the tensors.
            metadata: The tensor transport metadata for the GPU object.
            group_name: The name of the collective group.

        """

    @staticmethod
    @abstractmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        metadata: TensorTransportMetadata,
        device: "torch.device",
        group_name: str = "default",
    ):
        """
        Send multiple tensors to the destination actor.

        Args:
            tensors: The tensors to send.
            metadata: The tensor transport metadata for the GPU object.
            device: The device to send the tensors to.
            group_name: The name of the collective group.
        """
