from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

if TYPE_CHECKING:
    import numpy as np
    import torch

    import ray


# NOTE: This is a public facing abstract interface for custom tensor transports.
# Be sure to update the direct-transport docs when making changes to this interface, especially if changing the path to the file.


@dataclass
class CommunicatorMetadata:
    """Metadata for the communicator."""


@dataclass
class TensorTransportMetadata:
    """Metadata for tensors stored in the GPU object store.

    Args:
        tensor_meta: A list of tuples, each containing the shape and dtype of a tensor.
        tensor_device: The device of the tensor. Currently, we require all tensors in the
        list have the same device type.
    """

    tensor_meta: List[
        Union[Tuple["torch.Size", "torch.dtype"], Tuple[Tuple[int, ...], "np.dtype"]]
    ]
    tensor_device: Optional[str] = None


@dataclass
class TransferMetadata:
    """Base class for in-flight tensor transfer state.

    This class holds the minimal state needed to track an async transfer.
    Backend-specific implementations should extend this class with additional fields.

    Args:
        tensors: The tensors being transferred.
    """

    tensors: List[Any]


@dataclass
class FetchRequest:
    """Represents a pending or completed tensor fetch operation.

    The default fetch/wait implementation stores the tensors here directly
    after a synchronous recv. Transports with true async capability may
    subclass this to carry additional state needed by wait_fetch_complete.

    Args:
        obj_id: The object ID for the fetch operation.
        tensors: The fetched tensors.
    """

    obj_id: str
    tensors: List[Any]


class TensorTransportManager(ABC):
    """
    Interface with which to implement custom tensor transports.
    """

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
        rdt_object: List[Any],
    ) -> TensorTransportMetadata:
        """
        Extract the tensor transport metadata from the RDT object. This is called on the
        source actor once the actor task creates the result tensors.

        Args:
            obj_id: The ID of the RDT object to extract the tensor transport metadata from.
            rdt_object: The RDT object to extract the tensor transport metadata from.

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
        This function is called on the owner process before it orchestrates the transfer.

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
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List[Any]:
        """
        Receive multiple tensors from the source actor. This is called on the destination actor.

        Args:
            obj_id: The object ID for related GPU object.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.
            target_buffers: Pre-allocated buffers to receive the tensors into if possible.
        Returns:
            List[Any]: The received tensors.
        """

    def fetch_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> FetchRequest:
        """Initiate a fetch for multiple tensors without waiting for completion.

        The default implementation calls recv_multiple_tensors synchronously and
        stores the result in a FetchRequest. Transports with true async capability
        should override both this method and wait_fetch_complete.

        Call wait_fetch_complete(fetch_request) afterward to retrieve the tensors.

        Args:
            obj_id: The object ID for the related GPU object.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.
            target_buffers: Pre-allocated buffers to receive the tensors into if possible.

        Returns:
            A FetchRequest whose tensors field is already populated.
        """
        tensors = self.recv_multiple_tensors(
            obj_id, tensor_transport_metadata, communicator_metadata, target_buffers
        )
        return FetchRequest(obj_id=obj_id, tensors=tensors)

    def wait_fetch_complete(self, fetch_request: FetchRequest) -> List[Any]:
        """Wait for a previously initiated fetch to complete and return the tensors.

        The default implementation returns the tensors stored in the FetchRequest
        directly, since the default fetch_multiple_tensors is synchronous.

        Args:
            fetch_request: The FetchRequest returned by fetch_multiple_tensors.

        Returns:
            The received tensors.
        """
        return fetch_request.tensors

    @abstractmethod
    def send_multiple_tensors(
        self,
        tensors: List[Any],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Send multiple tensors or jax arrays to the destination actor. This is called on the source actor.

        Args:
            tensors: The tensors or jax arrays to send.
            tensor_transport_metadata: The tensor transport metadata for the RDT object.
            communicator_metadata: The communicator metadata for the send/recv operation.
        """

    @abstractmethod
    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[Any],
    ):
        """
        Garbage collect for the tensor transport after the GPU object is freed. This is only
        called on the source actor after Ray's distributed reference counting decides the object
        is out of scope.

        Args:
            obj_id: The ID of the GPU object to garbage collect.
            tensor_transport_meta: The tensor transport metadata.
            tensors: The tensors that are contained in the ObjectRef that is being freed.
        """

    @abstractmethod
    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Abort the transport. This is called on both the source and destination actors.

        Args:
            obj_id: The object ID for related GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.
        """
