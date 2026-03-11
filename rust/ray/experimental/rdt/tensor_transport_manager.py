"""Tensor transport manager interface and metadata types.

TensorTransportMetadata stays as a Python dataclass because downstream code
(e.g. CudaIpcTransportMetadata) subclasses it with @dataclass, which is
incompatible with PyO3 #[pyclass] types.

TensorTransportManager ABC and CommunicatorMetadata also stay in Python.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

if TYPE_CHECKING:
    import numpy as np
    import torch

    from ray.actor import ActorHandle


@dataclass
class TensorTransportMetadata:
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]]
    tensor_device: Optional[str] = None


@dataclass
class CommunicatorMetadata:
    """Base metadata for communicators. Subclassed by transport backends."""
    pass


class TensorTransportManager(ABC):
    """Abstract base class for tensor transport implementations.

    Transport backends (NIXL, NCCL, GLOO, CUDA_IPC) implement this interface.
    """

    @property
    @abstractmethod
    def tensor_transport_backend(self) -> str:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def can_abort_transport() -> bool:
        raise NotImplementedError

    @abstractmethod
    def actor_has_tensor_transport(self, actor: "ActorHandle") -> bool:
        raise NotImplementedError

    @abstractmethod
    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[Any],
    ) -> "TensorTransportMetadata":
        raise NotImplementedError

    @abstractmethod
    def get_communicator_metadata(
        self,
        src_actor: "ActorHandle",
        dst_actor: "ActorHandle",
        tensor_transport_backend: str,
    ) -> "CommunicatorMetadata":
        raise NotImplementedError

    @abstractmethod
    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_meta: "TensorTransportMetadata",
        communicator_meta: "CommunicatorMetadata",
        target_buffers: Optional[
            List["torch.Tensor | np.ndarray"]
        ] = None,
    ) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def send_multiple_tensors(
        self,
        tensors: List[Any],
        tensor_transport_meta: "TensorTransportMetadata",
        communicator_meta: "CommunicatorMetadata",
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: "TensorTransportMetadata",
        tensors: List[Any],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def abort_transport(
        self,
        obj_id: str,
        communicator_meta: "CommunicatorMetadata",
    ) -> None:
        raise NotImplementedError
