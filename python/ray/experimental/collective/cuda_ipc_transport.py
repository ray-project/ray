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
        return True

    @staticmethod
    def extract_tensor_transport_metadata(
        gpu_object: List["torch.Tensor"],
    ) -> CudaIpcTransportMetadata:

        tensor_meta = []
        cuda_ipc_handles = []
        device = None
        uuid = None
        if gpu_object:
            import torch
            from torch.multiprocessing.reductions import reduce_tensor

            # Create an interprocess-shareable CUDA event
            # This event can be shared across processes via IPC
            event = torch.cuda.Event(interprocess=True)
            # Record the event on the current stream
            # This marks the point when all prior operations (including tensor computation) are complete
            torch.cuda.current_stream().record_event(event)

            device = gpu_object[0].device
            uuid = str(torch.cuda.get_device_properties(device).uuid)

            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                tensor_meta.append((t.shape, t.dtype))
                ipc_handle = reduce_tensor(t)
                cuda_ipc_handles.append(ipc_handle)

            event_ipc_handle = event.ipc_handle()

        return CudaIpcTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
            cuda_ipc_handles=cuda_ipc_handles,
            cuda_ipc_event_ipc_handle=event_ipc_handle,
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
        obj_id: str,
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

            device = torch.cuda.current_device()
            received_uuid = str(torch.cuda.get_device_properties(device).uuid)
            sender_uuid = tensor_transport_metadata.cuda_ipc_device_uuid
            if received_uuid != sender_uuid:
                raise ValueError(
                    f"CUDA IPC transport only supports tensors on the same GPU, but the sender (GPU UUID: {sender_uuid}) and receiver (GPU UUID: {received_uuid}) are on different GPUs."
                )
            event_ipc_handle = tensor_transport_metadata.cuda_ipc_event_ipc_handle
            if event_ipc_handle is not None:
                # Reconstruct the event from IPC handle
                event_remote = torch.cuda.Event.from_ipc_handle(
                    device=device, handle=event_ipc_handle
                )

                # Make current stream wait for the sender's event
                # This ensures sender's computation is complete before we use the tensor
                # This is asynchronous - doesn't block CPU, only GPU stream
                torch.cuda.current_stream().wait_event(event_remote)
            for i, ipc_handle in enumerate(tensor_transport_metadata.cuda_ipc_handles):
                # Reconstruct the tensor
                func, args = ipc_handle
                list_args = list(args)
                # Fields specified in https://github.com/pytorch/pytorch/blob/1495b35d29512f303ab37780760c5e692158514b/torch/multiprocessing/reductions.py#L155
                # Update device ID to match current process's device mapping
                list_args[6] = device
                tensor = func(*list_args)
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
