import threading
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from typing_extensions import override

from .base import QueueWithRemoval
from .hash_link import HashLinkedQueue
from ray.data._internal.utils.object_utils import all_objects_exist_for_bundle
from ray.experimental import locations

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


DEFAULT_UPDATE_FREQUENCY_S = 30


class ObjectStoreAwareBundleQueue(QueueWithRemoval):
    """FIFO queue that serves bundles whose blocks still reside in the object
    store before bundles whose blocks have been lost (e.g. evicted, or on a
    drained node) and would need lineage reconstruction.

    Bundles are otherwise kept in insertion order. A bundle whose blocks are
    missing is rotated to the back of the queue instead of being dropped, and
    is served once its blocks are back or nothing resident remains.

    This class is thread-safe.
    """

    def __init__(self, update_frequency_s: float = DEFAULT_UPDATE_FREQUENCY_S):
        super().__init__()
        self._update_frequency_s = update_frequency_s

        self._hash_linked = HashLinkedQueue()
        # Object store bytes per distinct bundle. Seeded from the bundle's own
        # metadata and periodically refreshed from actual object locations.
        self._bundle_nbytes: Dict["RefBundle", int] = {}
        self._last_size_refresh_ts = time.time()
        self._total_nbytes = 0
        self._lock = threading.RLock()

    @override
    def __len__(self) -> int:
        with self._lock:
            return len(self._hash_linked)

    @override
    def __contains__(self, bundle: "RefBundle") -> bool:
        with self._lock:
            return bundle in self._hash_linked

    @override
    def add(self, bundle: "RefBundle", **kwargs: Any) -> None:
        with self._lock:
            if bundle not in self._hash_linked:
                self._bundle_nbytes[bundle] = bundle.size_bytes()
                self._total_nbytes += self._bundle_nbytes[bundle]
            self._hash_linked.add(bundle)

    @override
    def get_next(self) -> "RefBundle":
        with self._lock:
            if not self._hash_linked:
                raise IndexError("You can't pop from an empty queue")

            self._try_ensure_first_bundle_exists()
            bundle = self._hash_linked.peek_next()
            if bundle is None:
                raise IndexError("Unexpected empty queue")
            return self.remove(bundle)

    @override
    def peek_next(self) -> Optional["RefBundle"]:
        with self._lock:
            self._try_ensure_first_bundle_exists()
            return self._hash_linked.peek_next()

    @override
    def has_next(self) -> bool:
        return self.peek_next() is not None

    @override
    def has_resident_next(self) -> bool:
        # Rotation brings any resident bundle to the front, so this answers
        # whether the queue holds at least one resident bundle.
        with self._lock:
            return self._try_ensure_first_bundle_exists()

    @override
    def remove(self, bundle: "RefBundle") -> "RefBundle":
        with self._lock:
            if bundle not in self._bundle_nbytes:
                raise ValueError(f"Bundle {bundle} not found in the queue")

            # If the same bundle was added multiple times, this only removes the
            # first instance.
            removed = self._hash_linked.remove(bundle)

            # Duplicate instances share the same objects, so the size is only
            # released once the last instance is gone.
            if bundle not in self._hash_linked:
                nbytes = self._bundle_nbytes.pop(bundle)
                self._total_nbytes -= nbytes
                assert self._total_nbytes >= 0, (
                    "Expected the total size of objects in the queue to be "
                    f"non-negative, but got {self._total_nbytes} bytes instead."
                )
            return removed

    @override
    def clear(self) -> None:
        with self._lock:
            self._hash_linked.clear()
            self._bundle_nbytes.clear()
            self._total_nbytes = 0

    @override
    def estimate_size_bytes(self) -> int:
        with self._lock:
            now = time.time()
            # Bundle sizes change when Ray loses objects or creates replicas, so
            # re-derive them from object locations every `_update_frequency_s`.
            if now - self._last_size_refresh_ts >= self._update_frequency_s:
                self._refresh_bundle_sizes()
                self._total_nbytes = sum(self._bundle_nbytes.values())
                self._last_size_refresh_ts = now
            return self._total_nbytes

    def _try_ensure_first_bundle_exists(self) -> bool:
        """Rotate bundles with missing blocks to the back until a fully resident
        bundle is at the front, or every queued instance has been checked.

        Returns:
            Whether the bundle now at the front is fully resident.
        """
        num_bundles_skipped = 0
        while num_bundles_skipped < len(self._hash_linked):
            first_bundle = self._hash_linked.peek_next()
            if first_bundle is None:
                return False

            if all_objects_exist_for_bundle(first_bundle):
                return True

            self._hash_linked.get_next()
            self._hash_linked.add(first_bundle)
            num_bundles_skipped += 1
        return False

    def _refresh_bundle_sizes(self) -> None:
        for bundle in self._bundle_nbytes:
            object_locs = locations.get_local_object_locations(
                bundle.block_refs  # pyrefly: ignore[bad-argument-type]
            )

            nbytes = 0
            for object_info in object_locs.values():
                if object_info["object_size"] is not None:
                    # An object can have copies on multiple nodes; each copy
                    # occupies object store memory on its node.
                    nbytes += len(object_info["node_ids"]) * object_info["object_size"]

            assert nbytes >= 0, nbytes
            self._bundle_nbytes[bundle] = nbytes

    @override
    def num_blocks(self) -> int:
        with self._lock:
            return self._hash_linked.num_blocks()

    @override
    def num_rows(self) -> int:
        with self._lock:
            return self._hash_linked.num_rows()

    @override
    def num_bundles(self) -> int:
        with self._lock:
            return self._hash_linked.num_bundles()
