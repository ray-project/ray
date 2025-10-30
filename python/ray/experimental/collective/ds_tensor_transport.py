from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.collective.tensor_transport_manager import (
    TensorTransportManager,
)
from ray.util.collective.types import (
    DS_GROUP_NAME,
    Backend,
    DSCommunicatorMetadata,
    DSTransportMetadata,
)

if TYPE_CHECKING:
    import torch


class DSTensorTransport(TensorTransportManager):
    @property
    def tensor_transport_backend(self) -> Backend:
        return Backend.DS

    @staticmethod
    def is_one_sided() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __ray_actor_has_tensor_transport__(
            self: "ray.actor.ActorHandle",
        ) -> bool:
            try:
                from ray.util.collective.collective import get_group_handle

                ds_backend = get_group_handle(DS_GROUP_NAME)
                return ds_backend is not None
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
    ) -> DSTransportMetadata:
        from ray.util.collective.collective import get_group_handle
        from ray.util.collective.collective_group.ds_backend import DSBackend
        from ray.util.collective.types import DSTransportMetadata

        ds_backend: DSBackend = get_group_handle(DS_GROUP_NAME)
        device = None
        tensor_meta = []
        if gpu_object:
            serialized_keys = ds_backend.get_ds_metadata(gpu_object)
            # We assume all tensors in one GPU object have the same device type.
            device = gpu_object[0].device
            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                tensor_meta.append((t.shape, t.dtype))
        else:
            serialized_keys = None
        return DSTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
            ds_serialized_keys=serialized_keys,
        )

    @staticmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
    ) -> DSTransportMetadata:
        def __ray_get_tensor_transport_metadata__(
            self: "ray.actor.ActorHandle",
            obj_id: str,
        ) -> DSTransportMetadata:

            from ray._private.worker import global_worker

            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            # NOTE: We do not specify a timeout here because the user task that returns
            # it could take arbitrarily long and we don't want to trigger a spurious
            # timeout.
            gpu_object = gpu_object_store.wait_and_get_object(obj_id)

            return DSTensorTransport.extract_tensor_transport_metadata(gpu_object)

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
    ) -> DSCommunicatorMetadata:

        communicator_metadata = DSCommunicatorMetadata(
            communicator_name=DS_GROUP_NAME,
        )

        return communicator_metadata

    @staticmethod
    def recv_multiple_tensors(
        tensors,
        tensor_transport_metadata: DSTransportMetadata,
        communicator_metadata: DSCommunicatorMetadata,
    ):
        from ray.util.collective import types
        from ray.util.collective.collective import get_group_handle

        if tensors:
            g = get_group_handle(communicator_metadata.communicator_name)

            assert isinstance(
                tensor_transport_metadata, types.DSTransportMetadata
            ), "metadata must be a DSTransportMetadata object for DS transport"
            assert isinstance(
                communicator_metadata, types.DSCommunicatorMetadata
            ), "metadata must be a DSCommunicatorMetadata object for DS transport"

            g.recv(
                tensors,
                tensor_transport_metadata.ds_serialized_keys,
            )

    @staticmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        communicator_metadata: DSCommunicatorMetadata,
        device: "torch.device",
    ):
        raise NotImplementedError(
            "DS transport does not support send_multiple_tensors, since it is a one-sided transport."
        )

    @staticmethod
    def garbage_collect(tensor_transport_meta: DSTransportMetadata):
        from ray.util.collective.collective import get_group_handle
        from ray.util.collective.collective_group.ds_backend import DSBackend

        keys = tensor_transport_meta.ds_serialized_keys
        if keys is not None:
            ds_backend: DSBackend = get_group_handle(DS_GROUP_NAME)
            ds_backend.deregister_memory(keys)
