from typing import Optional, List, TYPE_CHECKING

import ray
from ray.experimental.collective.tensor_transport_manager import (
    TensorTransportManager,
    TensorTransportEnum,
)
from ray.util.collective.types import NIXL_GROUP_NAME, NixlTransportMetadata

if TYPE_CHECKING:
    import torch


class NixlTensorTransport(TensorTransportManager):
    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport: TensorTransportEnum,
    ) -> NixlTransportMetadata:
        from ray.util.collective.collective_group.nixl_backend import NixlBackend

        def __ray_get_tensor_transport_metadata__(
            self: "ray.actor.ActorHandle",
            obj_id: str,
            tensor_transport: TensorTransportEnum,
        ) -> NixlTransportMetadata:

            from ray._private.worker import global_worker
            from ray.util.collective.types import NixlTransportMetadata

            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            # NOTE: We do not specify a timeout here because the user task that returns
            # it could take arbitrarily long and we don't want to trigger a spurious
            # timeout.
            gpu_object = gpu_object_store.wait_and_get_object(obj_id)
            from ray.util.collective.collective import get_group_handle

            nixl_backend: NixlBackend = get_group_handle(NIXL_GROUP_NAME)
            if gpu_object:
                serialized_descs, agent_meta = nixl_backend.get_nixl_metadata(
                    gpu_object
                )
            else:
                serialized_descs, agent_meta = None, None
            return NixlTransportMetadata(
                tensor_meta=[(t.shape, t.dtype) for t in gpu_object],
                nixl_serialized_descs=serialized_descs,
                nixl_agent_meta=agent_meta,
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
    def get_collective_metadata(
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        tensor_transport_metadata: NixlTransportMetadata,
        backend: Optional[str] = None,
    ) -> NixlTransportMetadata:
        """
        Update the communicator name before sending the GPU object.
        """

        def __ray_update_collective_metadata__(
            self: "ray.actor.ActorHandle",
            tensor_transport_metadata: NixlTransportMetadata,
            communicator_name: str,
        ):
            tensor_transport_metadata.communicator_name = communicator_name
            return tensor_transport_metadata

        return src_actor.__ray_call__.remote(
            __ray_update_collective_metadata__,
            tensor_transport_metadata,
            NIXL_GROUP_NAME,
        )

    @staticmethod
    def recv_multiple_tensors(
        tensors,
        metadata: NixlTransportMetadata,
        group_name: str = "default",
    ):
        from ray.util.collective.collective import get_group_handle
        from ray.util.collective import types

        if tensors:
            g = get_group_handle(group_name)

            assert isinstance(
                metadata, types.NixlTransportMetadata
            ), "metadata must be a NixlTransportMetadata object for NIXL transport"
            g.recv(tensors, metadata.nixl_serialized_descs, metadata.nixl_agent_meta)

    @staticmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        metadata: NixlTransportMetadata,
        device: "torch.device",
        group_name: str = "default",
    ):
        pass
