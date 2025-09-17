import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import ray.util.collective as collective
from ray._private.custom_types import TensorTransportEnum
from ray.experimental.collective import get_tensor_transport_manager
from ray.experimental.collective.util import device_match_transport
from ray.util.collective.types import (
    Backend,
    CommunicatorMetadata,
    TensorTransportMetadata,
)
import warnings

try:
    import torch
except ImportError:
    raise ImportError(
        "`tensor_transport` requires PyTorch. "
        "Please install torch with 'pip install torch' to use this feature."
    )

TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND = {
    TensorTransportEnum.NCCL: Backend.NCCL,
    TensorTransportEnum.GLOO: Backend.TORCH_GLOO,
    TensorTransportEnum.NIXL: Backend.NIXL,
}


def _tensor_transport_to_collective_backend(
    tensor_transport: TensorTransportEnum,
) -> Backend:
    try:
        return TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND[tensor_transport]
    except KeyError:
        raise ValueError(
            f"Invalid tensor transport {tensor_transport.name}, must be one of {list(TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND.keys())}."
        )


def __ray_send__(
    self,
    obj_id: str,
    tensor_transport_meta: TensorTransportMetadata,
    communicator_meta: CommunicatorMetadata,
):
    """Helper function that runs on the src actor to send tensors to the dst actor."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager._gpu_object_store
    assert gpu_object_store.has_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"

    tensors = gpu_object_store.get_object(obj_id)

    backend = collective.get_group_handle(communicator_meta.communicator_name).backend()

    tensor_transport_manager = get_tensor_transport_manager(backend)
    if tensors and not device_match_transport(tensors[0].device, backend):
        raise ValueError(
            f"Tensor transport backend {backend} does not support tensor transfer on device {tensors[0].device}."
        )
    tensor_transport_manager.send_multiple_tensors(
        tensors,
        tensor_transport_meta,
        communicator_meta,
    )


def __ray_recv__(
    self,
    obj_id: str,
    tensor_transport_meta: TensorTransportMetadata,
    communicator_meta: CommunicatorMetadata,
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    backend = collective.get_group_handle(communicator_meta.communicator_name).backend()

    device = tensor_transport_meta.tensor_device
    tensor_meta = tensor_transport_meta.tensor_meta

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    if tensor_meta and not device_match_transport(device, backend):
        raise ValueError(
            f"Tensor transport backend {backend} does not support tensor transfer on device {device}."
        )
    tensors = []
    for meta in tensor_meta:
        shape, dtype = meta
        tensor = torch.zeros(shape, dtype=dtype, device=device)
        tensors.append(tensor)

    tensor_transport_manager = get_tensor_transport_manager(backend)
    tensor_transport_manager.recv_multiple_tensors(
        tensors,
        tensor_transport_meta,
        communicator_meta,
    )

    gpu_object_store.add_object(obj_id, tensors)


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    gpu_object = gpu_object_store.get_object(obj_id)
    return gpu_object


# Helper invoked on an actor process to report its current CUDA device id.
# Returns -1 if CUDA is not available.
def __ray_get_cuda_device__(self) -> int:
    try:
        import torch as _torch

        if _torch.cuda.is_available():
            return int(_torch.cuda.current_device())
    except Exception:
        pass
    return -1


def _extract_cuda_metadata(tensor: torch.Tensor):
    storage = tensor._typed_storage()
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
    return {
        "tensor_cls": type(tensor),
        "tensor_size": tensor.size(),
        "tensor_stride": tensor.stride(),
        "tensor_offset": tensor.storage_offset(),
        "storage_cls": type(storage),
        "dtype": tensor.dtype,
        "storage_device": storage_device,
        "storage_handle": storage_handle,
        "storage_size_bytes": storage_size_bytes,
        "storage_offset_bytes": storage_offset_bytes,
        "requires_grad": tensor.requires_grad,
        "ref_counter_handle": ref_counter_handle,
        "ref_counter_offset": ref_counter_offset,
        "event_handle": event_handle,
        "event_sync_required": event_sync_required,
    }


# Export CUDA IPC handles for tensors stored under obj_id.
# Only used when tensors are CUDA tensors and source/target actors reside on the same GPU.
# Returns a list of metadata tuples per tensor.
def __ray_cuda_ipc_export__(self, obj_id: str):
    from ray._private.worker import global_worker

    tensors = global_worker.gpu_object_manager.gpu_object_store.get_object(obj_id)

    metas = []
    for t in tensors:
        tensor_meta = _extract_cuda_metadata(t)
        metas.append(tensor_meta)

    return metas


# Import CUDA IPC handles and reconstruct tensors into the local GPU object store.
# Expects the metadata returned by __ray_cuda_ipc_export__.
def __ray_cuda_ipc_import__(self, obj_id: str, metas):
    from torch.multiprocessing.reductions import rebuild_cuda_tensor

    tensors = []
    for i, meta in enumerate(metas):
        try:
            t = rebuild_cuda_tensor(**meta)
            # Validate tensor is accessible by trying to get its device
            # This will trigger an error if the memory is invalid
            _ = t.device
            tensors.append(t)
        except (RuntimeError, OSError, Exception) as e:
            warnings.warn(
                f"Failed to import CUDA IPC tensor {i+1}/{len(metas)} for object {obj_id}. "
                f"The source actor may have crashed or been terminated. "
                f"Error: {str(e)}. "
                f"Falling back to network transfer if possible."
            )
            raise RuntimeError(
                f"CUDA IPC import failed for object {obj_id}. "
                f"Source actor appears to be dead or memory is invalid. "
                f"Original error: {e}"
            ) from e

    from ray._private.worker import global_worker

    global_worker.gpu_object_manager.gpu_object_store.add_object(obj_id, tensors)


@dataclass
class _GPUObject:
    # A list of tensors representing the GPU object.
    data: List["torch.Tensor"]
    # Whether the GPU object is the primary copy.
    is_primary: bool


class GPUObjectStore:
    """
    This class is thread-safe. The GPU object store is meant to be read and
    written by the following threads:
    1. The main thread, which is executing user code. This thread may get, put,
    and pop objects.
    2. The background _ray_system thread, which executes data transfers. This
    thread may get and put objects.
    3. The background CoreWorker server thread, which executes garbage
    collection callbacks that pop objects that are no longer in use.
    """

    def __init__(self):
        # A dictionary that maps from an object ID to a queue of tensor lists.
        #
        # Note: Currently, `_gpu_object_store` is only supported for Ray Actors.
        self._gpu_object_store: Dict[str, deque[_GPUObject]] = defaultdict(deque)
        # Mapping from tensor to the IDs of objects that contain it.
        self._tensor_to_object_ids: Dict["torch.Tensor", Set[str]] = defaultdict(set)
        # Synchronization for GPU object store.
        self._lock = threading.RLock()
        # Signal when an object becomes present in the object store.
        self._object_present_cv = threading.Condition(self._lock)
        # Signal when an object is freed from the object store.
        self._object_freed_cv = threading.Condition(self._lock)

    def has_object(self, obj_id: str) -> bool:
        with self._lock:
            existed = obj_id in self._gpu_object_store
            if existed:
                return len(self._gpu_object_store[obj_id]) > 0
            return existed

    def has_tensor(self, tensor: "torch.Tensor") -> bool:
        with self._lock:
            return tensor in self._tensor_to_object_ids

    def get_object(self, obj_id: str) -> Optional[List["torch.Tensor"]]:
        with self._lock:
            return self._gpu_object_store[obj_id][0].data

    def add_object(
        self,
        obj_id: str,
        gpu_object: List["torch.Tensor"],
        is_primary: bool = False,
    ):
        """
        Add a GPU object to the GPU object store.

        Args:
            obj_id: The object ID of the GPU object.
            gpu_object: A list of tensors representing the GPU object.
            is_primary: Whether the GPU object is the primary copy.
        """
        with self._object_present_cv:
            for tensor in gpu_object:
                self._tensor_to_object_ids[tensor].add(obj_id)
            # Append to the queue instead of overwriting
            self._gpu_object_store[obj_id].append(
                _GPUObject(
                    gpu_object,
                    is_primary,
                )
            )
            self._object_present_cv.notify_all()

    def is_primary_copy(self, obj_id: str) -> bool:
        with self._lock:
            return (
                self.has_object(obj_id) and self._gpu_object_store[obj_id][0].is_primary
            )

    def wait_and_get_object(
        self, obj_id: str, timeout: Optional[float] = None
    ) -> List["torch.Tensor"]:
        """Atomically waits for the GPU object to be present in the GPU object
        store, then gets it. If the object is not present after the optional
        timeout, raise a TimeoutError.

        Args:
            obj_id: The object ID to wait for.
            timeout: The maximum time in seconds to wait for the object to be
                present in the GPU object store. If not specified, wait indefinitely.

        Returns:
            The tensors in the GPU object.
        """
        with self._lock:
            self._wait_object(obj_id, timeout)
            return self.get_object(obj_id)

    def wait_and_pop_object(
        self, obj_id: str, timeout: Optional[float] = None
    ) -> List["torch.Tensor"]:
        """Atomically waits for the GPU object to be present in the GPU object
        store, then pops it.  If the object is not present after the optional
        timeout, raise a TimeoutError.

        Args:
            obj_id: The object ID to wait for.
            timeout: The maximum time in seconds to wait for the object to be
                present in the GPU object store. If not specified, wait
                indefinitely.

        Returns:
            The GPU object.
        """
        with self._lock:
            self._wait_object(obj_id, timeout)
            return self.pop_object(obj_id)

    def _wait_object(self, obj_id: str, timeout: Optional[float] = None) -> None:
        """Helper method to wait for the GPU object to be present in the GPU object store.
        If the object is not present after the optional timeout, raise a
        TimeoutError.

        Args:
            obj_id: The object ID to wait for.
            timeout: The maximum time in seconds to wait for the object to be
                present in the GPU object store. If not specified, wait
                indefinitely.
        """
        with self._object_present_cv:
            if not self._object_present_cv.wait_for(
                lambda: self.has_object(obj_id),
                timeout=timeout,
            ):
                raise TimeoutError(
                    f"ObjectRef({obj_id}) not found in RDT object store after {timeout}s, transfer may have failed. Please report this issue on GitHub: https://github.com/ray-project/ray/issues/new/choose"
                )

    def pop_object(self, obj_id: str) -> List["torch.Tensor"]:
        with self._lock:
            assert self.has_object(
                obj_id
            ), f"obj_id={obj_id} not found in GPU object store"
            queue = self._gpu_object_store.get(obj_id)
            gpu_object = queue.popleft()
            if len(queue) == 0:
                del self._gpu_object_store[obj_id]
            for tensor in gpu_object.data:
                self._tensor_to_object_ids[tensor].remove(obj_id)
                if len(self._tensor_to_object_ids[tensor]) == 0:
                    self._tensor_to_object_ids.pop(tensor)
            self._object_freed_cv.notify_all()
            return gpu_object.data

    def wait_tensor_freed(
        self, tensor: "torch.Tensor", timeout: Optional[float] = None
    ) -> None:
        """
        Wait for the object to be freed from the GPU object store.
        """
        with self._object_freed_cv:
            if not self._object_freed_cv.wait_for(
                lambda: tensor not in self._tensor_to_object_ids, timeout=timeout
            ):
                raise TimeoutError(
                    f"Tensor {tensor} not freed from RDT object store after {timeout}s. The tensor will not be freed until all ObjectRefs containing the tensor have gone out of scope."
                )

    def get_num_objects(self) -> int:
        """
        Return the number of objects in the GPU object store.
        """
        with self._lock:
            # Count total objects across all queues
            return sum(len(queue) for queue in self._gpu_object_store.values())
