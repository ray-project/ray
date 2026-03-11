"""Compatibility RDTManager for the Rust backend.

Wraps the Python rdt modules (rdt_store, util, nixl_tensor_transport)
to provide the simplified interface the Rust backend's ray/__init__.py expects.
The actual tensor storage and NIXL transport logic lives in the Python rdt files.
"""

import threading
from typing import Any, Dict, List, Optional

from ray.experimental.rdt.rdt_store import RDTStore
from ray.experimental.rdt.tensor_transport_manager import TensorTransportMetadata


class RDTManager:
    """Simplified RDT manager for the Rust backend.

    Uses RDTStore from the Python rdt modules for tensor storage
    and delegates transport operations to the Python transport managers.
    """

    def __init__(self):
        self.rdt_store = RDTStore()
        self._metadata: Dict[str, TensorTransportMetadata] = {}
        self._metadata_lock = threading.Lock()
        # Mapping from ObjectRef hex -> rdt_obj_id for lookups by ref hex
        self._ref_hex_to_obj_id: Dict[str, str] = {}
        # Driver-side tracking (keyed by ref hex)
        self._driver_tracking: Dict[str, str] = {}

    def is_managed_object(self, obj_id: str) -> bool:
        with self._metadata_lock:
            if obj_id in self._metadata:
                return True
            rdt_obj_id = self._ref_hex_to_obj_id.get(obj_id)
            if rdt_obj_id and rdt_obj_id in self._metadata:
                return True
            return obj_id in self._driver_tracking

    def get_rdt_metadata(self, obj_id: str) -> Optional[TensorTransportMetadata]:
        with self._metadata_lock:
            if obj_id in self._metadata:
                return self._metadata[obj_id]
            rdt_obj_id = self._ref_hex_to_obj_id.get(obj_id)
            if rdt_obj_id and rdt_obj_id in self._metadata:
                return self._metadata[rdt_obj_id]
            if obj_id in self._driver_tracking:
                rdt_obj_id = self._driver_tracking[obj_id]
                return self._metadata.get(rdt_obj_id)
            return None

    def register_ref_mapping(self, ref_hex: str, rdt_obj_id: str):
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
            linked_id = self._ref_hex_to_obj_id.get(obj_id)
            if linked_id:
                linked_meta = self._metadata.pop(linked_id, None)
                meta = meta or linked_meta
                del self._ref_hex_to_obj_id[obj_id]
            hex_keys_to_remove = [
                k for k, v in self._ref_hex_to_obj_id.items() if v == obj_id
            ]
            for k in hex_keys_to_remove:
                self._metadata.pop(k, None)
                del self._ref_hex_to_obj_id[k]

        if meta is None:
            return

        store_id = obj_id
        if not self.rdt_store.has_object(store_id) and linked_id:
            store_id = linked_id

        if self.rdt_store.has_object(store_id):
            tensors = self.rdt_store.pop_object(store_id)
            try:
                transport = get_tensor_transport_manager("NIXL")
                transport.garbage_collect(store_id, meta, tensors)
                if linked_id:
                    transport._managed_meta_nixl.pop(obj_id, None)
                else:
                    transport._managed_meta_nixl.pop(obj_id, None)
            except Exception:
                pass

    def track_driver_object(self, ref_hex: str, rdt_obj_id: str):
        with self._metadata_lock:
            if rdt_obj_id:
                self._metadata[ref_hex] = rdt_obj_id

    def free_driver_tracking(self, ref_hex: str):
        with self._metadata_lock:
            self._metadata.pop(ref_hex, None)


def reset_transport_managers():
    """Reset all transport managers (called on ray.shutdown)."""
    from ray.experimental.rdt import util as rdt_util

    with rdt_util.transport_managers_lock:
        rdt_util.transport_managers.clear()
        rdt_util.transport_manager_info.clear()
        rdt_util._default_transports_registered = False
