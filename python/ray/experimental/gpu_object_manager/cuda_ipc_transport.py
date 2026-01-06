from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

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
class CudaIpcCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the CUDA IPC communicator."""


@dataclass
class CudaIpcTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for CUDA IPC transport."""

    # List of tuples, each containing the function and metadata to reconstruct the tensor.
    cuda_ipc_handles: Optional[List[Any]] = None
    # The IPC handle of the event that is used to synchronize the sender and receiver.
    cuda_ipc_event_ipc_handle: Optional[bytes] = None
    # The index of the GPU that the tensors are on. This requires that the GPU is
    # assigned by Ray, e.g., using @ray.remote(num_gpus=1).
    ray_gpu_idx: Optional[int] = None
    # The node that the GPU that the tensors are on is on.
    ray_node_id: Optional[str] = None


class CudaIpcTransport(TensorTransportManager):
    def __init__(self, tensor_transport_backend: str):
        pass

    @property
    def tensor_transport_backend(self) -> str:
        return "CUDA_IPC"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        # TODO: Ideally we would check if torch.cuda.is_available() on the actor
        # and if so, return True. But we want to avoid blocking in ray.get() in
        # this method since it gets called before submitting an actor task.
        return True

    @staticmethod
    def extract_tensor_transport_metadata(
        obj_id: str,
        gpu_object: List["torch.Tensor"],
    ) -> CudaIpcTransportMetadata:

        tensor_meta = []
        device = None
        cuda_ipc_handles = []
        event_ipc_handle = None
        ray_gpu_idx = None
        ray_node_id = None
        if gpu_object:
            import torch
            from torch.multiprocessing.reductions import reduce_tensor

            # Create an interprocess-shareable CUDA event so that the receiver
            # can wait for the sender's computations to complete.
            event = torch.cuda.Event(interprocess=True)
            torch.cuda.current_stream().record_event(event)

            device = gpu_object[0].device
            ray_gpu_idx = ray.get_gpu_ids()[device.index]
            ray_node_id = ray.get_runtime_context().get_node_id()

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
            ray_gpu_idx=ray_gpu_idx,
            ray_node_id=ray_node_id,
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
            return CudaIpcTransport.extract_tensor_transport_metadata(
                obj_id, gpu_object
            )

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

        communicator_metadata = CudaIpcCommunicatorMetadata()
        return communicator_metadata

    @staticmethod
    def recv_multiple_tensors(
        tensors,
        obj_id: str,
        tensor_transport_metadata: CudaIpcTransportMetadata,
        communicator_metadata: CudaIpcCommunicatorMetadata,
    ):

        assert isinstance(
            tensor_transport_metadata, CudaIpcTransportMetadata
        ), "metadata must be a CudaIpcTransportMetadata object for CUDA IPC transport"
        assert isinstance(
            communicator_metadata, CudaIpcCommunicatorMetadata
        ), "metadata must be a CudaIpcCommunicatorMetadata object for CUDA IPC transport"

        if tensors:
            import torch

            cur_node_id = ray.get_runtime_context().get_node_id()
            if cur_node_id != tensor_transport_metadata.ray_node_id:
                raise ValueError(
                    f"CUDA IPC transport only supports tensors on the same node, but the current node ID: {cur_node_id} and the sender node ID: {tensor_transport_metadata.ray_node_id} are different."
                )

            device = tensors[0].device
            cur_gpu_idx = ray.get_gpu_ids()[device.index]
            if cur_gpu_idx != tensor_transport_metadata.ray_gpu_idx:
                raise ValueError(
                    f"CUDA IPC transport only supports tensors on the same GPU, but the sender (GPU: {tensor_transport_metadata.ray_gpu_idx}) and receiver (GPU: {cur_gpu_idx}) are on different GPUs. To use the CUDA IPC RDT transport, ensure that CUDA_VISIBLE_DEVICES is set to `ray.get_gpu_ids()`, the GPUs assigned by Ray (this is the default behavior)."
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
                if not isinstance(list_args[6], int):
                    raise RuntimeError(
                        f"Expected CUDA IPC tensor reconstruction list_args[6] to be device ID, but got {list_args[6]}. Please file an issue at https://github.com/ray-project/ray/issues/new/choose."
                    )
                list_args[6] = device.index
                try:
                    tensor = func(*list_args)
                except Exception as e:
                    raise RuntimeError(
                        "Error reconstructing CUDA IPC tensor. Source actor may have failed."
                    ) from e
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
    def garbage_collect(obj_id: str, tensor_transport_meta: CudaIpcTransportMetadata):
        pass

    @staticmethod
    def abort_transport(
        obj_id: str,
        communicator_metadata: CudaIpcCommunicatorMetadata,
    ):
        # TODO: Implement CUDA IPC abort transport.
        raise NotImplementedError(
            "CUDA IPC transport does not support abort_transport for now."
        )
