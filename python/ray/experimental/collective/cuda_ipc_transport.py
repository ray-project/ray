from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.collective.tensor_transport_manager import (
    TensorTransportManager,
)
from ray.util.collective.types import (
    CUDA_IPC_GROUP_NAME,
    Backend,
    CudaIpcCommunicatorMetadata,
    CudaIpcTransportMetadata,
)

if TYPE_CHECKING:
    import torch


class CudaIpcTransport(TensorTransportManager):
    def __init__(self, tensor_transport_backend: Backend):
        self._tensor_transport_backend = tensor_transport_backend

    @property
    def tensor_transport_backend(self) -> Backend:
        return self._tensor_transport_backend

    @staticmethod
    def is_one_sided() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        try:
            import torch

            return torch.cuda.is_available()
        except Exception:
            return False

    @staticmethod
    def extract_tensor_transport_metadata(
        gpu_object: List["torch.Tensor"],
    ) -> CudaIpcTransportMetadata:

        tensor_meta = []
        cuda_ipc_meta = []
        device = None
        uuid = None
        if gpu_object:
            import torch

            device = gpu_object[0].device
            uuid = str(torch.cuda.get_device_properties(device).uuid)
            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                storage = t._typed_storage()
                (
                    storage_device,
                    storage_handle,
                    storage_size_bytes,
                    storage_offset_bytes,
                    ref_counter_handle,
                    ref_counter_offset,
                    event_handle,
                    event_sync_required,
                ) = storage._share_cuda_()
                # Fields specified in https://github.com/pytorch/pytorch/blob/1495b35d29512f303ab37780760c5e692158514b/torch/multiprocessing/reductions.py#L155

                t_ipc_meta = {
                    "tensor_cls": type(t),
                    "tensor_size": t.size(),
                    "tensor_stride": t.stride(),
                    "tensor_offset": t.storage_offset(),
                    "storage_cls": type(storage),
                    "dtype": t.dtype,
                    "storage_device": storage_device,
                    "storage_handle": storage_handle,
                    "storage_size_bytes": storage_size_bytes,
                    "storage_offset_bytes": storage_offset_bytes,
                    "requires_grad": t.requires_grad,
                    "ref_counter_handle": ref_counter_handle,
                    "ref_counter_offset": ref_counter_offset,
                    "event_handle": event_handle,
                    "event_sync_required": event_sync_required,
                }
                tensor_meta.append((t.shape, t.dtype))
                cuda_ipc_meta.append(t_ipc_meta)
        return CudaIpcTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
            cuda_ipc_meta=cuda_ipc_meta,
            cuda_ipc_device_uuid=uuid,
        )

    @staticmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
    ) -> CudaIpcTransportMetadata:
        def __ray_get_tensor_transport_metadata__(
            self: "ray.actor.ActorHandle",
            obj_id: str,
        ) -> CudaIpcTransportMetadata:

            from ray._private.worker import global_worker

            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            # NOTE: We do not specify a timeout here because the user task that returns
            # it could take arbitrarily long and we don't want to trigger a spurious
            # timeout.
            gpu_object = gpu_object_store.wait_and_get_object(obj_id)
            return CudaIpcTransport.extract_tensor_transport_metadata(gpu_object)

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
    ) -> CudaIpcCommunicatorMetadata:

        communicator_metadata = CudaIpcCommunicatorMetadata(
            communicator_name=CUDA_IPC_GROUP_NAME,
        )
        return communicator_metadata

    @staticmethod
    def recv_multiple_tensors(
        tensors,
        tensor_transport_metadata: CudaIpcTransportMetadata,
        communicator_metadata: CudaIpcCommunicatorMetadata,
    ):
        from ray.util.collective import types

        assert isinstance(
            tensor_transport_metadata, types.CudaIpcTransportMetadata
        ), "metadata must be a CudaIpcTransportMetadata object for CUDA IPC transport"
        assert isinstance(
            communicator_metadata, types.CudaIpcCommunicatorMetadata
        ), "metadata must be a CudaIpcCommunicatorMetadata object for CUDA IPC transport"

        if tensors:
            import torch
            from torch.multiprocessing.reductions import rebuild_cuda_tensor

            device = torch.cuda.current_device()
            received_uuid = str(torch.cuda.get_device_properties(device).uuid)
            sender_uuid = tensor_transport_metadata.cuda_ipc_device_uuid
            if received_uuid != sender_uuid:
                raise ValueError(
                    f"CUDA IPC transport only supports tensors on the same GPU, but the sender (GPU UUID: {sender_uuid}) and receiver (GPU UUID: {received_uuid}) are on different GPUs."
                )
            for i, ipc_meta in enumerate(tensor_transport_metadata.cuda_ipc_meta):
                tensor = rebuild_cuda_tensor(**ipc_meta)
                tensors[i] = tensor

    @staticmethod
    def send_multiple_tensors(
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: CudaIpcTransportMetadata,
        communicator_metadata: CudaIpcCommunicatorMetadata,
    ):
        raise NotImplementedError(
            "CUDA IPC transport does not support send_multiple_tensors, since it is a one-sided transport."
        )

    @staticmethod
    def garbage_collect(tensor_transport_meta: CudaIpcTransportMetadata):
        pass
