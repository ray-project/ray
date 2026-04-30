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


class TensorTransportManager(ABC):
    """
    Interface with which to implement custom tensor transports.
    """

    @abstractmethod
    def tensor_transport_backend(self) -> str:
        """
        Returns the name of your tensor transport backend.
        Ray uses this name to match your transport with the ``tensor_transport`` argument
        on the method.

        Returns:
            str: The backend of the tensor transport.
        """

    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        """
        Indicates whether your transport uses one-sided communication where only the receiver
        initiates the transfer.

        One-sided transports: The receiver can directly read the sender's memory without the sender
        actively participating. NIXL and CUDA-IPC are examples.

        Two-sided transports: Both sender and receiver must actively participate in the transfer.
        Collective communication libraries like NCCL and GLOO are examples.

        This affects how Ray orchestrates the transfer and handles failures. Two-sided transports
        have extra limitations described in :ref:`limitations <limitations>`. Ray will not call
        `send_multiple_tensors` for one-sided transports; the transfer is expected to happen through
        just `recv_multiple_tensors`.

        Returns:
            bool: True if the backend is one-sided, False otherwise.
        """

    @staticmethod
    @abstractmethod
    def can_abort_transport() -> bool:
        """
        Indicates whether your transport can safely abort an in-progress transfer.

        If ``True``, Ray calls `abort_transport` on both the source and destination actors when a
        send / recv error, allowing your transport to clean up gracefully.

        If ``False``, Ray kills the involved actors to prevent deadlocks when errors occur during
        transfer.

        Return ``True`` only if your transport can reliably interrupt an in-progress send or receive
        operation without leaving either party in a blocked state.

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
        Implement this method to create the TensorTransportMetadata you defined previously.
        Ray calls this on the source actor immediately after the actor task creates the result tensors.
        Implement this to:

        1. Record tensor shapes, dtypes, and devices.
        2. Perform any transport-specific tensor registration such as registering memory for RDMA.
        3. Store any handles or identifiers needed for the transfer.

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
        Gets the CommunicatorMetadata for a send/recv. Ray calls this on the owner/driver process before
        orchestrating the transfer. You can typically implement this to return information both actors
        need to identify each other such as ranks in a collective group. Many forms of transports such
        as one-sided RDMA reads may be ok just returning empty CommunicatorMetadata here.

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
        Receives tensors on the destination actor. Ray calls this on the destination
        actor during the transfer.

        Args:
            obj_id: The object ID for related GPU object.
            tensor_transport_metadata: The tensor transport metadata for the GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.
            target_buffers: Pre-allocated buffers to receive the tensors into if possible.
        Returns:
            List[Any]: The received tensors.
        """

    @abstractmethod
    def send_multiple_tensors(
        self,
        tensors: List[Any],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """
        Sends tensors from the source actor to the destination actor. Ray calls this on the source actor
        during the transfer. Implement this to perform the actual data transfer using your transport's
        send mechanism. For one-sided transports, you can simply avoid implementing this method or even
        raise a NotImplementedError to ensure it's not being called.

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
        Clean up resources for an RDT object. Ray calls this on the source actor
        after Ray's distributed reference counting protocol determines the object is out of scope.

        Use this to release any resources your transport allocated, such as deregistering memory buffers.
        On the receiver side, no cleanup is needed — Ray does not hold onto the tensor after
        returning it to the user, so it is garbage collected normally when the user releases it.

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
        Aborts an in-progress transfer. Ray calls this on both the source and destination actors
        when a system error occurs if `can_abort_transport` returns ``True``.

        Args:
            obj_id: The object ID for related GPU object.
            communicator_metadata: The communicator metadata for the send/recv operation.
        """
