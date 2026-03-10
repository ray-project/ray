"""Abstract base classes for tensor transport.

Adapted from python/ray/experimental/gpu_object_manager/tensor_transport_manager.py
for standalone use in the Rust Ray backend.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union


@dataclass
class CommunicatorMetadata:
    """Metadata for the communicator."""


@dataclass
class TensorTransportMetadata:
    """Metadata for tensors stored in the GPU object store.

    Args:
        tensor_meta: A list of tuples, each containing the shape and dtype of a tensor.
        tensor_device: The device of the tensor (e.g. "cuda", "cpu").
    """

    tensor_meta: List[Tuple]
    tensor_device: Optional[str] = None


class TensorTransportManager(ABC):
    """Interface for custom tensor transports."""

    @abstractmethod
    def tensor_transport_backend(self) -> str:
        ...

    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        ...

    @staticmethod
    @abstractmethod
    def can_abort_transport() -> bool:
        ...

    @abstractmethod
    def actor_has_tensor_transport(self, actor) -> bool:
        ...

    @abstractmethod
    def extract_tensor_transport_metadata(
        self, obj_id: str, gpu_object: List[Any],
    ) -> TensorTransportMetadata:
        ...

    @abstractmethod
    def get_communicator_metadata(
        self, src_actor, dst_actor, backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        ...

    @abstractmethod
    def recv_multiple_tensors(
        self, obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ) -> List[Any]:
        ...

    @abstractmethod
    def send_multiple_tensors(
        self, tensors: List[Any],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        ...

    @abstractmethod
    def garbage_collect(
        self, obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[Any],
    ):
        ...

    @abstractmethod
    def abort_transport(
        self, obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        ...
