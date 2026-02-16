from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import jax


@dataclass
class JaxCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the JAX communicator."""


@dataclass
class JaxTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for JAX transport."""


class JaxTensorTransport(TensorTransportManager):
    def __init__(self):
        pass

    @property
    def tensor_transport_backend(self) -> str:
        return "JAX"

    @staticmethod
    def is_one_sided() -> bool:
        return False

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        return True

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        gpu_object: List["jax.Array"],
    ) -> JaxTransportMetadata:
        pass

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> JaxCommunicatorMetadata:

        communicator_metadata = JaxCommunicatorMetadata()
        return communicator_metadata

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ) -> List["jax.Array"]:

        assert isinstance(tensor_transport_metadata, JaxTransportMetadata)
        assert isinstance(communicator_metadata, JaxCommunicatorMetadata)

        return []

    def send_multiple_tensors(
        self,
        tensors: List["jax.Array"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        assert isinstance(tensor_transport_metadata, JaxTransportMetadata)
        assert isinstance(communicator_metadata, JaxCommunicatorMetadata)
        pass

    def garbage_collect(
        self, obj_id: str, tensor_transport_meta: TensorTransportMetadata
    ):
        pass

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        pass
