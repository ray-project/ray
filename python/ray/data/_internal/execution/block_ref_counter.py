import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Dict

import ray

if TYPE_CHECKING:
    pass


class BlockRefCounter:
    """Tracks object-store memory usage per operator via Ray Core callbacks.

    The callback fires when:
    - All Python ObjectRefs wrapping the block's ObjectID are garbage-collected, AND
    - All Ray tasks that received the block as an argument have completed.
    """

    def __init__(self):
        # Registered object ID binaries (bytes); prevents double-counting when
        # the same ObjectRef appears in multiple RefBundles.
        self._registered_ids: set = set()
        # (producer_id -> total live bytes); maintained incrementally for O(1) reads.
        self._bytes_by_producer: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def on_block_produced(
        self,
        block_ref: "ray.ObjectRef",
        size_bytes: int,
        producer_id: str,
    ) -> None:
        """Register a block and attribute its memory to producer_id.

        Registers a Ray Core out-of-scope callback so that when all references
        to block_ref are gone the bytes are automatically removed from the
        producer's usage. Duplicate calls for the same ObjectRef (e.g. the
        same ref appearing in two RefBundles) are deduplicated.
        """
        id_binary = block_ref.binary()
        with self._lock:
            if id_binary in self._registered_ids:
                # Same block already tracked; additional RefBundle slots don't
                # change memory consumption.
                return
            self._registered_ids.add(id_binary)
            self._bytes_by_producer[producer_id] += size_bytes

        def _on_out_of_scope(id_bytes: bytes) -> None:
            with self._lock:
                if id_bytes not in self._registered_ids:
                    # Already cleared (e.g. by clear()), nothing to do.
                    return
                self._registered_ids.discard(id_bytes)
                self._bytes_by_producer[producer_id] -= size_bytes

        core_worker = ray._private.worker.global_worker.core_worker
        registered = core_worker.add_object_out_of_scope_callback(
            block_ref, _on_out_of_scope
        )
        if not registered:
            # Object was already out of scope when we tried to register;
            # the trampoline will never fire. Undo the bytes we just added.
            with self._lock:
                self._registered_ids.discard(id_binary)
                self._bytes_by_producer[producer_id] -= size_bytes

    def get_object_store_memory_usage(self, producer_id: str) -> int:
        """Total bytes of live blocks attributed to producer_id."""
        with self._lock:
            return self._bytes_by_producer.get(producer_id, 0)

    def clear(self) -> None:
        """Reset all accounting, e.g. on executor shutdown.

        Any previously registered Ray Core callbacks firing after clear()
        will be silently ignored because _registered_ids is empty.
        """
        with self._lock:
            self._registered_ids.clear()
            self._bytes_by_producer.clear()
