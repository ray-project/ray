from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

import ray
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
    ) -> TensorTransportMetadata:
        """
        Get the tensor transport metadata for the GPU object.
        This function retrieves metadata about tensors stored in the GPU object store,
        including their shapes, dtypes, and any transport-specific metadata, e.g., NIXL descriptors.

        Args:
            src_actor: The actor that runs this function.
            obj_id: The ID of the GPU object to get metadata for

        Returns:
            TensorTransportMetadata: A named tuple containing the tensor metadata.
        """

    @staticmethod
    @abstractmethod
    def extract_tensor_transport_metadata(
        obj_id: str,
        gpu_object: List["torch.Tensor"],
    ) -> TensorTransportMetadata:
        """
        Extract the tensor transport metadata from the GPU object.

        Args:
            obj_id: The ID of the GPU object to extract the tensor transport metadata from.
            gpu_object: The GPU object to extract the tensor transport metadata from.

        Returns:
            TensorTransportMetadata: The tensor transport metadata.
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

    @staticmethod
    @abstractmethod
    def garbage_collect(obj_id: str, tensor_transport_meta: TensorTransportMetadata):
        """
        Garbage collect for the tensor transport after the GPU object is freed.

        Args:
            obj_id: The ID of the GPU object to garbage collect.
            tensor_transport_meta: The tensor transport metadata.
        """
