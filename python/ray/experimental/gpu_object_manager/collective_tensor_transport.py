from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    TensorTransportManager,
)
from ray.experimental.gpu_object_manager.types import (
    CommunicatorMetadata,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch


@dataclass
class CollectiveTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for collective transport."""


@dataclass
class CollectiveCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the collective communicator (e.g. NCCL, GLOO).

    Args:
        src_rank: The rank of the source actor.
        dst_rank: The rank of the destination actor.
    """

    communicator_name: str = ""
    src_rank: Optional[int] = None
    dst_rank: Optional[int] = None


class CollectiveTensorTransport(TensorTransportManager):
    def __init__(self, tensor_transport_backend: str):
        self._tensor_transport_backend = tensor_transport_backend

    @property
    def tensor_transport_backend(self) -> str:
        return self._tensor_transport_backend

    @staticmethod
    def is_one_sided() -> bool:
        return False

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        from ray.experimental.collective import get_collective_groups

        communicators = get_collective_groups(
            [actor], backend=self.tensor_transport_backend
        )
        return len(communicators) > 0

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        gpu_object: List["torch.Tensor"],
    ) -> CollectiveTransportMetadata:
        tensor_meta = []
        device = None
        if gpu_object:
            device = gpu_object[0].device
            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                tensor_meta.append((t.shape, t.dtype))
        return CollectiveTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
        )

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CollectiveCommunicatorMetadata:

        from ray.experimental.collective import get_collective_groups

        communicators = get_collective_groups(
            [src_actor, dst_actor],
            backend=backend,
        )
        # TODO(kevin85421): Support multiple communicators.
        if len(communicators) == 0:
            raise ValueError(
                f"No communicators found for actors {src_actor} and {dst_actor}. "
                "Create a communicator with "
                "`ray.experimental.collective.create_collective_group` "
                "before calling actor tasks. with non-default tensor_transport."
            )
        elif len(communicators) > 1:
            raise ValueError(
                f"There are {len(communicators)} possible communicators that contain actors {src_actor} and {dst_actor}. "
                "Currently, RDT objects only support one communicator. Please make sure only "
                "one communicator exists."
            )
        communicator = communicators[0]
        src_rank = communicator.get_rank(src_actor)
        if src_rank == -1:
            raise ValueError(
                f"Sender actor {src_actor} not found in communicator. "
                "Please make sure the sender and receiver are in the same communicator."
            )
        dst_rank = communicator.get_rank(dst_actor)
        if dst_rank == -1:
            raise ValueError(
                f"Receiver actor {dst_actor} not found in communicator. "
                "Please make sure the sender and receiver are in the same communicator."
            )

        communicator_metadata = CollectiveCommunicatorMetadata(
            communicator_name=communicator.name,
            src_rank=src_rank,
            dst_rank=dst_rank,
        )
        return communicator_metadata

    def recv_multiple_tensors(
        self,
        tensors,
        obj_id: str,
        tensor_transport_metadata: CollectiveTransportMetadata,
        communicator_metadata: CollectiveCommunicatorMetadata,
    ):
        from ray.util.collective.collective import recv

        assert isinstance(
            tensor_transport_metadata, CollectiveTransportMetadata
        ), "metadata must be a CollectiveTransportMetadata object for non-NIXL transport"
        assert isinstance(
            communicator_metadata, CollectiveCommunicatorMetadata
        ), "metadata must be a CollectiveCommunicatorMetadata object for non-NIXL transport"

        for tensor in tensors:
            recv(
                tensor,
                communicator_metadata.src_rank,
                communicator_metadata.communicator_name,
            )

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: CollectiveTransportMetadata,
        communicator_metadata: CollectiveCommunicatorMetadata,
    ):
        import ray.util.collective as collective

        device = tensors[0].device if tensors else None

        for tensor in tensors:
            if tensor.device.type != device.type:
                raise ValueError(
                    f"tensor device {tensor.device} does not match device {device}"
                )
            collective.send(
                tensor,
                communicator_metadata.dst_rank,
                communicator_metadata.communicator_name,
            )

    def garbage_collect(
        self, obj_id: str, tensor_transport_meta: CollectiveTransportMetadata
    ):
        pass

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CollectiveCommunicatorMetadata,
    ):
        raise NotImplementedError(
            "Collective transport does not support abort_transport for now."
        )
