"""RDT Manager and Store for the Rust Ray backend.

Provides per-worker tensor storage and lifecycle management for NIXL transport.
Adapted from python/ray/experimental/gpu_object_manager/gpu_object_store.py
and gpu_object_manager.py.
"""

import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from ray.experimental.rdt.tensor_transport_manager import TensorTransportMetadata


@dataclass
class _RDTObject:
    data: List[Any]
    is_primary: bool
    error: Optional[Exception] = None


class RDTStore:
    """Thread-safe store for GPU tensors on a worker.

    Tracks tensor→object_id mappings for GC and provides
    condition-variable-based waiting.
    """

    def __init__(self):
        self._store: Dict[str, deque[_RDTObject]] = defaultdict(deque)
        self._tensor_to_object_ids: Dict[int, Set[str]] = defaultdict(set)
        self._lock = threading.RLock()
        self._object_present_cv = threading.Condition(self._lock)
        self._object_freed_cv = threading.Condition(self._lock)

    def has_object(self, obj_id: str) -> bool:
        with self._lock:
            if obj_id in self._store:
                return len(self._store[obj_id]) > 0
            return False

    def has_tensor(self, tensor: Any) -> bool:
        with self._lock:
            return id(tensor) in self._tensor_to_object_ids

    def get_object(self, obj_id: str) -> List[Any]:
        with self._lock:
            if self._store[obj_id][0].error:
                raise self._store[obj_id][0].error
            return self._store[obj_id][0].data

    def add_object(
        self,
        obj_id: str,
        gpu_object,
        is_primary: bool = False,
    ):
        with self._object_present_cv:
            if isinstance(gpu_object, Exception):
                self._store[obj_id].append(
                    _RDTObject([], is_primary, error=gpu_object)
                )
            else:
                for tensor in gpu_object:
                    self._tensor_to_object_ids[id(tensor)].add(obj_id)
                self._store[obj_id].append(
                    _RDTObject(gpu_object, is_primary)
                )
            self._object_present_cv.notify_all()

    def pop_object(self, obj_id: str) -> List[Any]:
        with self._lock:
            queue = self._store.get(obj_id)
            assert queue is not None, f"obj_id={obj_id} not found in RDT store"
            gpu_object = queue.popleft()
            if len(queue) == 0:
                del self._store[obj_id]
            if gpu_object.error:
                raise gpu_object.error
            for tensor in gpu_object.data:
                self._tensor_to_object_ids[id(tensor)].remove(obj_id)
                if len(self._tensor_to_object_ids[id(tensor)]) == 0:
                    self._tensor_to_object_ids.pop(id(tensor))
            self._object_freed_cv.notify_all()
            return gpu_object.data

    def wait_and_get_object(
        self, obj_id: str, timeout: Optional[float] = None,
    ) -> List[Any]:
        with self._lock:
            self._wait_object(obj_id, timeout)
            return self.get_object(obj_id)

    def wait_tensor_freed(self, tensor: Any, timeout: Optional[float] = None):
        """Wait until the tensor is freed from all objects in the store."""
        with self._object_freed_cv:
            if not self._object_freed_cv.wait_for(
                lambda: id(tensor) not in self._tensor_to_object_ids,
                timeout=timeout,
            ):
                raise TimeoutError(
                    f"Tensor not freed from RDT object store after {timeout}s. "
                    "The tensor will not be freed until all ObjectRefs "
                    "containing the tensor have gone out of scope."
                )

    def get_num_objects(self) -> int:
        with self._lock:
            return sum(len(queue) for queue in self._store.values())

    def _wait_object(self, obj_id: str, timeout: Optional[float] = None):
        with self._object_present_cv:
            if not self._object_present_cv.wait_for(
                lambda: self.has_object(obj_id),
                timeout=timeout,
            ):
                raise TimeoutError(
                    f"ObjectRef({obj_id}) not found in RDT object store after {timeout}s"
                )


class RDTManager:
    """Manages RDT objects and tensor transport metadata for a worker.

    Each GPU worker process has one RDTManager that coordinates between
    the actor instance, the RDT store, and the NIXL transport.
    """

    def __init__(self):
        self.rdt_store = RDTStore()
        self._metadata: Dict[str, TensorTransportMetadata] = {}
        self._metadata_lock = threading.Lock()
        # Mapping from ObjectRef hex → rdt_obj_id for lookups by ref hex
        self._ref_hex_to_obj_id: Dict[str, str] = {}
        # Driver-side tracking (keyed by ref hex)
        self._driver_tracking: Dict[str, str] = {}

    def is_managed_object(self, obj_id: str) -> bool:
        """Check if an object is managed by RDT.

        Accepts either rdt_obj_id or ObjectRef hex.
        """
        with self._metadata_lock:
            if obj_id in self._metadata:
                return True
            # Check ref_hex → rdt_obj_id mapping
            rdt_obj_id = self._ref_hex_to_obj_id.get(obj_id)
            if rdt_obj_id and rdt_obj_id in self._metadata:
                return True
            # Check driver-side tracking
            return obj_id in self._driver_tracking

    def get_rdt_metadata(self, obj_id: str) -> Optional[TensorTransportMetadata]:
        """Get the transport metadata for an object.

        Accepts either rdt_obj_id or ObjectRef hex.
        """
        with self._metadata_lock:
            if obj_id in self._metadata:
                return self._metadata[obj_id]
            # Check ref_hex → rdt_obj_id mapping
            rdt_obj_id = self._ref_hex_to_obj_id.get(obj_id)
            if rdt_obj_id and rdt_obj_id in self._metadata:
                return self._metadata[rdt_obj_id]
            # Check driver-side tracking
            if obj_id in self._driver_tracking:
                rdt_obj_id = self._driver_tracking[obj_id]
                return self._metadata.get(rdt_obj_id)
            return None

    def register_ref_mapping(self, ref_hex: str, rdt_obj_id: str):
        """Register a mapping from ObjectRef hex to rdt_obj_id."""
        with self._metadata_lock:
            self._ref_hex_to_obj_id[ref_hex] = rdt_obj_id

    def put_object(
        self, obj_id: str, tensors: List[Any], transport_name: str,
    ) -> TensorTransportMetadata:
        """Store tensors as primary copy and extract transport metadata."""
        from ray.experimental.rdt.util import get_tensor_transport_manager

        self.rdt_store.add_object(obj_id, tensors, is_primary=True)
        transport = get_tensor_transport_manager(transport_name)
        meta = transport.extract_tensor_transport_metadata(obj_id, tensors)
        with self._metadata_lock:
            self._metadata[obj_id] = meta
        return meta

    def free_object(self, obj_id: str):
        """Free an object: garbage-collect NIXL registration and remove from store."""
        from ray.experimental.rdt.util import get_tensor_transport_manager

        with self._metadata_lock:
            meta = self._metadata.pop(obj_id, None)
            # Find the linked rdt_obj_id (if obj_id is a ref_hex alias)
            linked_id = self._ref_hex_to_obj_id.get(obj_id)
            if linked_id:
                # obj_id is ref_hex, linked_id is the original temp_id
                linked_meta = self._metadata.pop(linked_id, None)
                meta = meta or linked_meta
                del self._ref_hex_to_obj_id[obj_id]
            # Also clean up any ref_hex entries that point to obj_id
            hex_keys_to_remove = [
                k for k, v in self._ref_hex_to_obj_id.items() if v == obj_id
            ]
            for k in hex_keys_to_remove:
                self._metadata.pop(k, None)
                del self._ref_hex_to_obj_id[k]

        if meta is None:
            return

        # Try to find the object in rdt_store under either key
        store_id = obj_id
        if not self.rdt_store.has_object(store_id) and linked_id:
            store_id = linked_id

        if self.rdt_store.has_object(store_id):
            tensors = self.rdt_store.pop_object(store_id)
            try:
                transport = get_tensor_transport_manager("NIXL")
                transport.garbage_collect(store_id, meta, tensors)
                # Also clean up alias in _managed_meta_nixl
                if linked_id:
                    transport._managed_meta_nixl.pop(obj_id, None)
                else:
                    # Check if there's a ref_hex alias to clean
                    transport._managed_meta_nixl.pop(obj_id, None)
            except Exception:
                pass

    # -- Driver-side object tracking (for test_nixl_del_before_creating) ---

    def track_driver_object(self, ref_hex: str, rdt_obj_id: str):
        """Track an NIXL object on the driver side by its ObjectRef hex."""
        with self._metadata_lock:
            if rdt_obj_id:
                self._metadata[ref_hex] = rdt_obj_id  # Just store the mapping

    def free_driver_tracking(self, ref_hex: str):
        """Remove driver-side tracking for an ObjectRef."""
        with self._metadata_lock:
            self._metadata.pop(ref_hex, None)
