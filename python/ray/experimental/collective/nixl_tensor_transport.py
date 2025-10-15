from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.collective.tensor_transport_manager import (
    TensorTransportManager,
)
from ray.util.collective.types import (
    NIXL_GROUP_NAME,
    Backend,
    NixlCommunicatorMetadata,
    NixlTransportMetadata,
)

if TYPE_CHECKING:
    import torch


class NixlTensorTransport(TensorTransportManager):
    @property
    def tensor_transport_backend(self) -> Backend:
        return Backend.NIXL

    @staticmethod
    def is_one_sided() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __ray_actor_has_tensor_transport__(
            self: "ray.actor.ActorHandle",
        ) -> bool:
            try:
                from ray.util.collective.collective import get_group_handle

                nixl_backend = get_group_handle(NIXL_GROUP_NAME)
                return nixl_backend is not None
            except Exception:
                return False

        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_actor_has_tensor_transport__
            )
        )

    @staticmethod
    def extract_tensor_transport_metadata(
        gpu_object: List["torch.Tensor"],
    ) -> NixlTransportMetadata:
        from ray.util.collective.collective import get_group_handle
        from ray.util.collective.collective_group.nixl_backend import NixlBackend
        from ray.util.collective.types import NixlTransportMetadata

        nixl_backend: NixlBackend = get_group_handle(NIXL_GROUP_NAME)
        device = None
        tensor_meta = []
        if gpu_object:
            reg_descs, serialized_descs, agent_meta = nixl_backend.get_nixl_metadata(
                gpu_object
            )
            # We assume all tensors in one GPU object have the same device type.
            device = gpu_object[0].device
            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                tensor_meta.append((t.shape, t.dtype))
        else:
            reg_descs, serialized_descs, agent_meta = None, None, None
        return NixlTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
            nixl_reg_descs=reg_descs,
            nixl_serialized_descs=serialized_descs,
            nixl_agent_meta=agent_meta,
        )

    @staticmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
    ) -> NixlTransportMetadata:
        def __ray_get_tensor_transport_metadata__(
            self: "ray.actor.ActorHandle",
            obj_id: str,
        ) -> NixlTransportMetadata:

            from ray._private.worker import global_worker

            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            # NOTE: We do not specify a timeout here because the user task that returns
            # it could take arbitrarily long and we don't want to trigger a spurious
            # timeout.
            gpu_object = gpu_object_store.wait_and_get_object(obj_id)

            return NixlTensorTransport.extract_tensor_transport_metadata(gpu_object)

        # Submit a Ray actor task to the source actor to get the tensor metadata.
        # The metadata is a list of tuples, where each tuple contains the shape and dtype
        # of a tensor in the GPU object store. This function returns an ObjectRef that
        # points to the tensor metadata.
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking this task.

        return src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_get_tensor_transport_metadata__, obj_id
        )

    @staticmethod
    def get_communicator_metadata(
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> NixlCommunicatorMetadata:

        communicator_metadata = NixlCommunicatorMetadata(
            communicator_name=NIXL_GROUP_NAME,
        )

        return communicator_metadata

    @staticmethod
    def recv_multiple_tensors(
        tensors,
        tensor_transport_metadata: NixlTransportMetadata,
        communicator_metadata: NixlCommunicatorMetadata,
    ):
        from ray.util.collective import types
        from ray.util.collective.collective import get_group_handle

        if tensors:
            g = get_group_handle(communicator_metadata.communicator_name)

            assert isinstance(
                tensor_transport_metadata, types.NixlTransportMetadata
            ), "metadata must be a NixlTransportMetadata object for NIXL transport"
            assert isinstance(
                communicator_metadata, types.NixlCommunicatorMetadata
            ), "metadata must be a NixlCommunicatorMetadata object for NIXL transport"

            g.recv(
                tensors,
                tensor_transport_metadata.nixl_serialized_descs,
                tensor_transport_metadata.nixl_agent_meta,
            )

    @staticmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        communicator_metadata: NixlCommunicatorMetadata,
        device: "torch.device",
    ):
        raise NotImplementedError(
            "NIXL transport does not support send_multiple_tensors, since it is a one-sided transport."
        )

    @staticmethod
    def garbage_collect(tensor_transport_meta: NixlTransportMetadata):
        from ray.util.collective.collective import get_group_handle
        from ray.util.collective.collective_group.nixl_backend import NixlBackend

        descs = tensor_transport_meta.nixl_reg_descs
        if descs is not None:
            nixl_backend: NixlBackend = get_group_handle(NIXL_GROUP_NAME)
            nixl_backend.deregister_memory(descs)
