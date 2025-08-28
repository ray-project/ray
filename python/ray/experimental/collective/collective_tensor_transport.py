from typing import Optional, List, TYPE_CHECKING

import ray
from ray.experimental.collective.tensor_transport_manager import (
    TensorTransportManager,
    TensorTransportEnum,
)

from ray.util.collective.types import (
    CollectiveTransportMetadata,
    CollectiveCommunicatorMetadata,
)

if TYPE_CHECKING:
    import torch


class CollectiveTensorTransport(TensorTransportManager):
    @staticmethod
    def is_one_sided() -> bool:
        return False

    @staticmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport: TensorTransportEnum,
    ) -> CollectiveTransportMetadata:
        def __ray_get_tensor_transport_metadata__(
            self: "ray.actor.ActorHandle",
            obj_id: str,
            tensor_transport: TensorTransportEnum,
        ) -> CollectiveTransportMetadata:

            from ray._private.worker import global_worker
            from ray.util.collective.types import CollectiveTransportMetadata

            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            # NOTE: We do not specify a timeout here because the user task that returns
            # it could take arbitrarily long and we don't want to trigger a spurious
            # timeout.
            gpu_object = gpu_object_store.wait_and_get_object(obj_id)
            return CollectiveTransportMetadata(
                tensor_meta=[(t.shape, t.dtype) for t in gpu_object],
            )

        # Submit a Ray actor task to the source actor to get the tensor metadata.
        # The metadata is a list of tuples, where each tuple contains the shape and dtype
        # of a tensor in the GPU object store. This function returns an ObjectRef that
        # points to the tensor metadata.
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking this task.

        return src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_get_tensor_transport_metadata__, obj_id, tensor_transport
        )

    @staticmethod
    def get_communicator_metadata(
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
                "Currently, GPU objects only support one communicator. Please make sure only "
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

    @staticmethod
    def recv_multiple_tensors(
        tensors,
        tensor_transport_metadata: CollectiveTransportMetadata,
        communicator_metadata: CollectiveCommunicatorMetadata,
    ):
        from ray.util.collective import types
        from ray.util.collective.collective import recv

        assert isinstance(
            tensor_transport_metadata, types.CollectiveTransportMetadata
        ), "metadata must be a CollectiveTransportMetadata object for non-NIXL transport"
        assert isinstance(
            communicator_metadata, types.CollectiveCommunicatorMetadata
        ), "metadata must be a CollectiveCommunicatorMetadata object for non-NIXL transport"

        for tensor in tensors:
            recv(
                tensor,
                communicator_metadata.src_rank,
                communicator_metadata.communicator_name,
            )

    @staticmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        communicator_metadata: CollectiveCommunicatorMetadata,
        device: "torch.device",
    ):
        import ray.util.collective as collective

        for tensor in tensors:
            if tensor.device.type != device.type:
                # TODO(swang): Right now there is no way to catch this error
                # and the receiving Ray task will hang.
                raise ValueError(
                    f"tensor device {tensor.device} does not match device {device}"
                )
            collective.send(
                tensor,
                communicator_metadata.dst_rank,
                communicator_metadata.communicator_name,
            )
