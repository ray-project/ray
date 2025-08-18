from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set
import threading
from collections import defaultdict

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
    tensors = gpu_object_store.get_object(obj_id)

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
    return [(t.shape, t.dtype) for t in gpu_object]


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


# Export CUDA IPC handles for tensors stored under obj_id.
# Only used when tensors are CUDA tensors and source/target actors reside on the same GPU.
# Returns a list of metadata tuples per tensor.
def __ray_cuda_ipc_export__(self, obj_id: str):
    from ray._private.worker import global_worker
    tensors = global_worker.gpu_object_manager.gpu_object_store.get_object(obj_id)
    metas = []
    for t in tensors:
        if t.device.type != "cuda":
            raise RuntimeError("CUDA IPC export requires CUDA tensors")
        # Use untyped storage to avoid dtype-specific paths.
        storage = t.untyped_storage()
        size_bytes = int(storage.nbytes())
        # The following private API is the standard way in PyTorch to create an IPC handle.
        # It may change across versions; guarded by try/except to provide a clear error.
        try:
            h = storage._share_cuda_()
            # Normalize to a 64-byte cudaIpcMemHandle_t for CuPy.
            handle_bytes = None
            # Case 1: Attribute holder (e.g., .handle)
            if hasattr(h, "handle"):
                try:
                    print("handle type")
                    print(type(h.handle))
                    handle_bytes = bytes(h.handle)
                except Exception:
                    pass
            # Case 2: Already bytes-like
            if handle_bytes is None and isinstance(h, (bytes, bytearray, memoryview)):
                handle_bytes = bytes(h)
            # Case 3: Tuple forms
            if handle_bytes is None and isinstance(h, tuple):
                # Common: (bytes, ...)
                if h and isinstance(h[0], (bytes, bytearray, memoryview)):
                    handle_bytes = bytes(h[0])
                else:
                    # Try flattening ints/nested ints
                    flat_ints: list[int] = []
                    def _flatten_ints(val):
                        if isinstance(val, int):
                            flat_ints.append(val)
                        elif isinstance(val, (tuple, list)):
                            for vv in val:
                                _flatten_ints(vv)
                    _flatten_ints(h)
                    try:
                        handle_bytes = bytes(flat_ints)
                    except Exception:
                        handle_bytes = None
            # Last resort: bytes() on the object
            if handle_bytes is None:
                try:
                    handle_bytes = bytes(h)
                except Exception:
                    handle_bytes = None
            # Validate handle size: cudaIpcMemHandle_t is 64 bytes
            if not handle_bytes or len(handle_bytes) != 64:
                raise RuntimeError(
                    f"Invalid CUDA IPC handle size. Expected 64 bytes, got {0 if not handle_bytes else len(handle_bytes)} (raw={type(h)})."
                )

            metas.append(
                {
                    "handle": handle_bytes,
                    "size_bytes": size_bytes,
                    "dtype": str(t.dtype),
                    "shape": tuple(t.size()),
                    "stride": tuple(t.stride()),
                    "storage_offset": int(t.storage_offset()),
                    "device_index": int(t.device.index),
                }
            )
        except Exception as e:
            raise RuntimeError("PyTorch does not support CUDA IPC on this build") from e

    return metas


# Import CUDA IPC handles and reconstruct tensors into the local GPU object store.
# Expects the metadata returned by __ray_cuda_ipc_export__.
def __ray_cuda_ipc_import__(self, obj_id: str, metas):
    try:
        import torch as _torch
    except ImportError:
        raise

    tensors = []
    for meta in metas:
        if not _torch.cuda.is_available():
            raise RuntimeError("CUDA is required to import CUDA IPC handles")
        device = _torch.device("cuda", meta["device_index"]) if meta.get("device_index") is not None else _torch.device("cuda")
        # Try PyTorch-native IPC path first, then fall back to CuPy-based import if unavailable.
        try:
            storage = _torch.UntypedStorage.from_cuda_ipc_handle(meta["size_bytes"], meta["handle"])  # type: ignore[attr-defined]
            dtype = getattr(_torch, str(meta["dtype"]).split(".")[-1])
            t = _torch.empty(0, device=device, dtype=dtype)
            t = t.set_(storage, meta["storage_offset"], meta["shape"], meta["stride"])  # type: ignore[attr-defined]
            tensors.append(t)
            continue
        except Exception:
            pass

        # CuPy-based fallback: open the CUDA IPC mem handle via runtime API and wrap with DLPack.
        try:
            import cupy as _cp  # Requires cupy-cuda12x in the environment

            handle = meta["handle"]
            # Ensure bytes-like handle of correct size for cudaIpcMemHandle_t (typically 64 bytes)
            if isinstance(handle, memoryview):
                handle = handle.tobytes()
            ptr = _cp.cuda.runtime.ipcOpenMemHandle(handle, 0)
            mem = _cp.cuda.UnownedMemory(ptr, meta["size_bytes"], owner=None)
            mptr = _cp.cuda.MemoryPointer(mem, 0)
            # Map dtype and strides (CuPy expects strides in bytes)
            dtype_name = str(meta["dtype"]).split(".")[-1]
            cp_dtype = getattr(_cp, dtype_name)
            itemsize = _cp.dtype(cp_dtype).itemsize
            strides_bytes = tuple(s * itemsize for s in meta["stride"]) if meta.get("stride") else None
            arr = _cp.ndarray(shape=tuple(meta["shape"]), dtype=cp_dtype, memptr=mptr, strides=strides_bytes)
            # Convert to torch via DLPack (zero-copy)
            t = _torch.utils.dlpack.from_dlpack(arr.toDlpack())
            tensors.append(t)
            # Note: We are not closing the IPC handle here; rely on process teardown or future GC integration.
        except Exception as e:
            raise RuntimeError(f"CUDA IPC import via CuPy failed: {e}")

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
        # A dictionary that maps from an object ID to a list of tensors.
        #
        # Note: Currently, `gpu_object_store` is only supported for Ray Actors.
        self._gpu_object_store: Dict[str, _GPUObject] = {}
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
            return obj_id in self._gpu_object_store

    def has_tensor(self, tensor: "torch.Tensor") -> bool:
        with self._lock:
            return tensor in self._tensor_to_object_ids

    def get_object(self, obj_id: str) -> Optional[List["torch.Tensor"]]:
        with self._lock:
            return self._gpu_object_store[obj_id].data

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
            self._gpu_object_store[obj_id] = _GPUObject(
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
                lambda: obj_id in self._gpu_object_store, timeout=timeout
            ):
                raise TimeoutError(
                    f"ObjectRef({obj_id}) not found in GPU object store after {timeout}s, transfer may have failed. Please report this issue on GitHub: https://github.com/ray-project/ray/issues/new/choose"
                )

    def pop_object(self, obj_id: str) -> List["torch.Tensor"]:
        with self._lock:
            assert (
                obj_id in self._gpu_object_store
            ), f"obj_id={obj_id} not found in GPU object store"
            gpu_object = self._gpu_object_store.pop(obj_id)
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
                    f"Tensor {tensor} not freed from GPU object store after {timeout}s. The tensor will not be freed until all ObjectRefs containing the tensor have gone out of scope."
                )

    def get_num_objects(self) -> int:
        """
        Return the number of objects in the GPU object store.
        """
        with self._lock:
            return len(self._gpu_object_store)
