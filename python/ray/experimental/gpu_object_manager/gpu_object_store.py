from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import threading

import ray.util.collective as collective
from ray._private.custom_types import TensorTransportEnum
from ray.util.collective.types import Backend


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
}

COLLECTIVE_BACKEND_TO_TORCH_DEVICE = {
    Backend.NCCL: torch.device("cuda"),
    Backend.TORCH_GLOO: torch.device("cpu"),
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


def __ray_send__(self, communicator_name: str, obj_id: str, dst_rank: int):
    """Helper function that runs on the src actor to send tensors to the dst actor."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager._gpu_object_store
    assert gpu_object_store.has_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_object(obj_id).data

    backend = collective.get_group_handle(communicator_name).backend()
    device = COLLECTIVE_BACKEND_TO_TORCH_DEVICE[backend]

    for tensor in tensors:
        if tensor.device.type != device.type:
            # TODO(swang): Right now there is no way to catch this error
            # and the receiving Ray task will hang.
            raise ValueError(
                f"tensor device {tensor.device} does not match device {device}"
            )
        collective.send(tensor, dst_rank, group_name=communicator_name)


def __ray_recv__(
    self,
    communicator_name: str,
    obj_id: str,
    src_rank: int,
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    backend = collective.get_group_handle(communicator_name).backend()
    device = COLLECTIVE_BACKEND_TO_TORCH_DEVICE[backend]

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    tensors = []
    for meta in tensor_meta:
        shape, dtype = meta
        tensor = torch.zeros(shape, dtype=dtype, device=device)
        collective.recv(tensor, src_rank, group_name=communicator_name)
        tensors.append(tensor)
    gpu_object_store.add_object(obj_id, tensors)


def __ray_get_tensor_meta__(self, obj_id: str):
    """Helper function that runs on the src actor to get the tensor metadata."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    # NOTE: We do not specify a timeout here because the user task that returns
    # it could take arbitrarily long and we don't want to trigger a spurious
    # timeout.
    gpu_object = gpu_object_store.wait_and_get_object(obj_id)
    return [(t.shape, t.dtype) for t in gpu_object.data]


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    gpu_object = gpu_object_store.get_object(obj_id)
    return gpu_object.data


@dataclass
class GPUObject:
    # A list of tensors representing the GPU object.
    data: List["torch.Tensor"]
    # Whether the GPU object is the primary copy.
    is_primary: bool
    # The number of reads allowed to the GPU object before it will be GCed from this actor.
    # This is used to implement garbage collection for receiver actors,
    # handling cases where the same GPU object reference is passed to the
    # same actor task multiple times. For sender actors, we still rely on
    # the object store's reference counting mechanism.
    num_readers: int = 0


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
        # A dictionary that maps from an object ID to a list of tensors.
        #
        # Note: Currently, `gpu_object_store` is only supported for Ray Actors.
        self._gpu_object_store: Dict[str, GPUObject] = {}
        # Synchronization for GPU object store.
        self._lock = threading.RLock()
        # Signal when an object becomes present in the object store.
        self._object_present_cv = threading.Condition(self._lock)

    def has_object(self, obj_id: str) -> bool:
        with self._lock:
            return obj_id in self._gpu_object_store

    def get_object(self, obj_id: str) -> Optional[GPUObject]:
        with self._lock:
            return self._gpu_object_store[obj_id]

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
            self._gpu_object_store[obj_id] = GPUObject(
                gpu_object,
                is_primary,
            )
            self._object_present_cv.notify_all()

    def is_primary_copy(self, obj_id: str) -> bool:
        with self._lock:
            return (
                obj_id in self._gpu_object_store
                and self._gpu_object_store[obj_id].is_primary
            )

    def wait_and_get_object(
        self, obj_id: str, timeout: Optional[float] = None
    ) -> GPUObject:
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
    ) -> GPUObject:
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
            present = self._object_present_cv.wait_for(
                lambda: obj_id in self._gpu_object_store, timeout=timeout
            )
            if not present:
                raise TimeoutError(
                    f"ObjectRef({obj_id}) not found in GPU object store after {timeout}s, transfer may have failed. Please report this issue on GitHub: https://github.com/ray-project/ray/issues/new/choose"
                )

    def pop_object(self, obj_id: str) -> GPUObject:
        with self._lock:
            assert (
                obj_id in self._gpu_object_store
            ), f"obj_id={obj_id} not found in GPU object store"
            return self._gpu_object_store.pop(obj_id)

    def get_num_objects(self) -> int:
        """
        Return the number of objects in the GPU object store.
        """
        with self._lock:
            return len(self._gpu_object_store)
