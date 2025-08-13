from abc import ABC, abstractmethod
from typing import Optional
from ray._private.custom_types import TensorTransportEnum
from ray.util.collective.types import TensorTransportMetadata

import ray


class TensorTransportManager(ABC):
    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        """Whether the backend is one-sided."""

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
        including their shapes, dtypes, and NIXL-specific metadata (when NIXL transport
        is enabled).

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
        Update the collective metadata (e.g. communicator name, src/dst rank)
        before sending the GPU object.

        Args:
            src_actor: The actor that runs this function.
            dst_actor: The actor that runs this function.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
            backend: The backend to use for the collective operation.

        Returns:
            TensorTransportMetadata: A named tuple containing the tensor metadata.
        """

    @staticmethod
    @abstractmethod
    def send_object(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
    ):
        """
        Send the GPU object to the destination actor.
        """

    @staticmethod
    @abstractmethod
    def recv_object(
        dst_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
    ):
        """
        Receive the GPU object from the source actor.
        """
