import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Union

from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportMetadata,
)
from ray.experimental.gpu_object_manager.util import (
    device_match_transport,
    get_tensor_transport_manager,
)

try:
    import torch
except ImportError:
    raise ImportError(
        "`tensor_transport` requires PyTorch. "
        "Please install torch with 'pip install torch' to use this feature."
    )


def __ray_send__(
    self,
    obj_id: str,
    tensor_transport_meta: TensorTransportMetadata,
    communicator_meta: CommunicatorMetadata,
    backend: str,
):
    """Helper function that runs on the src actor to send tensors to the dst actor."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager._gpu_object_store
    assert gpu_object_store.has_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"

    tensors = gpu_object_store.get_object(obj_id)

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
    backend: str,
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    try:
        device = tensor_transport_meta.tensor_device
        tensor_meta = tensor_transport_meta.tensor_meta

        if tensor_meta and not device_match_transport(device, backend):
            raise ValueError(
                f"Tensor transport backend {backend} does not support tensor transfer on device {device}."
            )

        tensor_transport_manager = get_tensor_transport_manager(backend)
        tensors = tensor_transport_manager.recv_multiple_tensors(
            obj_id,
            tensor_transport_meta,
            communicator_meta,
        )
        assert len(tensors) == len(tensor_meta)
        gpu_object_store.add_object(obj_id, tensors)
    except Exception as e:
        # Store the error as a gpu object if the recv fails, so waiters will raise the error.
        gpu_object_store.add_object(obj_id, e)


def __ray_abort_transport__(
    self, obj_id: str, communicator_meta: CommunicatorMetadata, backend: str
):
    """Helper function that can run on an actor doing a send or recv to abort the transport."""
    tensor_transport_manager = get_tensor_transport_manager(backend)
    tensor_transport_manager.abort_transport(obj_id, communicator_meta)


def __ray_free__(
    self,
    obj_id: str,
    tensor_transport_backend: str,
    tensor_transport_meta: TensorTransportMetadata,
):
    try:
        from ray._private.worker import global_worker

        tensor_transport_manager = get_tensor_transport_manager(
            tensor_transport_backend
        )
        tensor_transport_manager.garbage_collect(obj_id, tensor_transport_meta)

        gpu_object_manager = global_worker.gpu_object_manager
        gpu_object_store = gpu_object_manager.gpu_object_store
        gpu_object_store.pop_object(obj_id)
    except AssertionError:
        # This could fail if this is a retry and it's already been freed.
        pass


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    gpu_object = gpu_object_store.wait_and_get_object(obj_id)
    return gpu_object


@dataclass
class _GPUObject:
    # A list of tensors representing the GPU object.
    data: List["torch.Tensor"]
    # Whether the GPU object is the primary copy.
    is_primary: bool
    # If a recv failed, we store the error here.
    error: Optional[Exception] = None


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
        # Mapping from tensor data pointer to the IDs of objects that contain it.
        self._tensor_to_object_ids: Dict[int, Set[str]] = defaultdict[int, Set[str]](
            set
        )
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
            return tensor.data_ptr() in self._tensor_to_object_ids

    def get_object(self, obj_id: str) -> Optional[List["torch.Tensor"]]:
        with self._lock:
            if self._gpu_object_store[obj_id][0].error:
                raise self._gpu_object_store[obj_id][0].error
            return self._gpu_object_store[obj_id][0].data

    def add_object(
        self,
        obj_id: str,
        gpu_object: Union[List["torch.Tensor"], Exception],
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
            if isinstance(gpu_object, Exception):
                self._gpu_object_store[obj_id].append(
                    _GPUObject([], is_primary, error=gpu_object)
                )
            else:
                for tensor in gpu_object:
                    self._tensor_to_object_ids[tensor.data_ptr()].add(obj_id)
                # Append to the queue instead of overwriting
                self._gpu_object_store[obj_id].append(
                    _GPUObject(
                        gpu_object,
                        is_primary,
                    )
                )
            self._object_present_cv.notify_all()

    def add_object_primary(
        self, obj_id: str, tensors: List["torch.Tensor"], tensor_transport: str
    ) -> TensorTransportMetadata:
        self.add_object(obj_id, tensors, is_primary=True)
        tensor_transport_manager = get_tensor_transport_manager(tensor_transport)
        tensor_transport_meta = (
            tensor_transport_manager.extract_tensor_transport_metadata(obj_id, tensors)
        )
        return tensor_transport_meta

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

    def get_duplicate_objects(
        self,
        src_obj_id: str,
        src_gpu_object: List["torch.Tensor"],
    ) -> Optional[str]:
        """
        Get another object ID of the GPU object that duplicates the given GPU object.
        Returns the object id if there is a duplicate, None if there is no duplicate.
        """
        with self._lock:
            if len(src_gpu_object) == 0:
                return None
            obj_id_set = set()
            for tensor in src_gpu_object:
                for obj_id in self._tensor_to_object_ids[tensor.data_ptr()]:
                    obj_id_set.add(obj_id)

            for dst_obj_id in obj_id_set:
                if dst_obj_id != src_obj_id:
                    dst_gpu_object = self._gpu_object_store[dst_obj_id][0].data
                    is_same_tensors = len(src_gpu_object) == len(
                        dst_gpu_object
                    ) and all(
                        t1.data_ptr() == t2.data_ptr()
                        for t1, t2 in zip(src_gpu_object, dst_gpu_object)
                    )
                    if is_same_tensors:
                        return dst_obj_id
            return None

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
            queue = self._gpu_object_store.get(obj_id)
            assert queue is not None, f"obj_id={obj_id} not found in GPU object store"
            gpu_object = queue.popleft()
            if len(queue) == 0:
                del self._gpu_object_store[obj_id]
            if gpu_object.error:
                raise gpu_object.error
            for tensor in gpu_object.data:
                self._tensor_to_object_ids[tensor.data_ptr()].remove(obj_id)
                if len(self._tensor_to_object_ids[tensor.data_ptr()]) == 0:
                    self._tensor_to_object_ids.pop(tensor.data_ptr())
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
                lambda: tensor.data_ptr() not in self._tensor_to_object_ids,
                timeout=timeout,
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
