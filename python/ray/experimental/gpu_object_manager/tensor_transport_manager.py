from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.gpu_object_manager.types import (
    CommunicatorMetadata,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch


class TensorTransportManager(ABC):
    @property
    @abstractmethod
    def tensor_transport_backend(self) -> str:
        """The tensor transport backend, e.g., NCCL.

        Returns:
            str: The backend of the tensor transport.
        """

    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        """Whether the backend is one-sided.

        Returns:
            bool: True if the backend is one-sided, False otherwise.
        """

    @staticmethod
    @abstractmethod
    def can_abort_transport() -> bool:
        """
        Whether the backend can abort the transport.
        If this returns False, then Ray will kill involved actors upon system errors to avoid hanging.

        Returns:
            bool: True if the backend can abort the transport.
        """

    @abstractmethod
    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        """Whether the actor has the tensor transport available.

        Args:
            actor: The actor to check.

        Returns:
            bool: True if the actor has the tensor transport available, False otherwise.
        """

    @abstractmethod
    def extract_tensor_transport_metadata(
        self,
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

    @abstractmethod
    def get_communicator_metadata(
        self,
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

    @abstractmethod
    def recv_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Receive multiple tensors from the source actor.

        Args:
            tensors: The pre-allocated tensor space to receive the tensors.
            obj_id: The object ID for related GPU object.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.

        """

    @abstractmethod
    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Send multiple tensors to the destination actor.

        Args:
            tensors: The tensors to send.
            tensor_transport_metadata: The tensor transport metadata for the RDT object.
            communicator_metadata: The communicator metadata for the send/recv operation.
        """

    @abstractmethod
    def garbage_collect(
        self, obj_id: str, tensor_transport_meta: TensorTransportMetadata
    ):
        """
        Garbage collect for the tensor transport after the GPU object is freed.

        Args:
            obj_id: The ID of the GPU object to garbage collect.
            tensor_transport_meta: The tensor transport metadata.
        """

    @abstractmethod
    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Abort the transport.

        Args:
            obj_id: The object ID for related GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.
        """
