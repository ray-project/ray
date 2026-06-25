import threading
from collections import defaultdict
from typing import Callable, Dict, Optional

import ray
from ray._private.worker import global_worker


class BlockRefCounter:
    """Tracks object-store memory usage per operator via Ray Core callbacks.

    The callback fires when:
    - All Python ObjectRefs wrapping the block's ObjectID are garbage-collected, AND
    - All Ray tasks that received the block as an argument have completed.
    """

    def __init__(
        self,
        add_object_out_of_scope_callback: Optional[
            Callable[["ray.ObjectRef", Callable[[bytes], None]], bool]
        ] = None,
    ):
        if add_object_out_of_scope_callback is None:
            add_object_out_of_scope_callback = (
                global_worker.core_worker.add_object_out_of_scope_callback
            )
        self._add_callback_fn = add_object_out_of_scope_callback
        # IDs of live blocks. Stale callbacks (fired after clear()) check
        # membership here and no-op, preventing negative _bytes_by_producer.
        self._registered_ids: set[bytes] = set()
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
        producer's usage.

        Idempotent: calling twice with the same block_ref is a no-op.
        """
        id_binary = block_ref.binary()
        with self._lock:
            if id_binary in self._registered_ids:
                return
            self._registered_ids.add(id_binary)
            self._bytes_by_producer[producer_id] += size_bytes

        def _on_object_freed(id_bytes: bytes) -> None:
            with self._lock:
                if id_bytes not in self._registered_ids:
                    # Already cleared (e.g. by clear()), nothing to do.
                    return
                self._registered_ids.discard(id_bytes)
                self._bytes_by_producer[producer_id] -= size_bytes

        registered = self._add_callback_fn(block_ref, _on_object_freed)
        if not registered:
            _on_object_freed(id_binary)

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
