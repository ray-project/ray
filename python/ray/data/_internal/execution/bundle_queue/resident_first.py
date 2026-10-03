import threading
from typing import TYPE_CHECKING, Any, Dict, Optional

from typing_extensions import override

from .base import QueueWithRemoval
from .hash_link import HashLinkedQueue
from ray.data._internal.utils.cached_ray_internals import get_node_loss_version
from ray.data._internal.utils.object_utils import all_objects_exist_for_bundle

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class ResidentFirstBundleQueue(QueueWithRemoval):
    """FIFO queue that serves bundles whose blocks still reside in the object
    store before bundles whose blocks have been lost (e.g. evicted, or on a
    drained node) and would need lineage reconstruction.

    Bundles are otherwise kept in insertion order. A bundle whose blocks are
    missing is rotated to the back of the queue instead of being dropped, and
    is served once its blocks are back or nothing resident remains.

    Sizes come from block metadata, as in ``HashLinkedQueue``. Deriving them
    from object locations would cost one core-worker lookup per queued bundle
    on the scheduler thread; a shuffle input queue can hold hundreds of
    thousands of bundles.

    This class is thread-safe.
    """

    def __init__(self):
        super().__init__()

        self._hash_linked = HashLinkedQueue()
        # Bytes per distinct bundle, so duplicate entries count once.
        self._bundle_nbytes: Dict["RefBundle", int] = {}
        # Node-loss version at which each bundle is known resident. Blocks are
        # pinned while queued, so they only go missing when a node dies or
        # drains.
        self._resident_version: Dict["RefBundle", int] = {}
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
                if get_node_loss_version() == 0:
                    self._resident_version[bundle] = 0
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
                self._resident_version.pop(bundle, None)
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
            self._resident_version.clear()
            self._total_nbytes = 0

    @override
    def estimate_size_bytes(self) -> int:
        with self._lock:
            return self._total_nbytes

    def _try_ensure_first_bundle_exists(self) -> bool:
        """Rotate bundles with missing blocks to the back until a fully resident
        bundle is at the front, or every queued instance has been checked.

        Returns:
            Whether the bundle now at the front is fully resident.
        """
        version = get_node_loss_version()
        num_bundles_skipped = 0
        while num_bundles_skipped < len(self._hash_linked):
            first_bundle = self._hash_linked.peek_next()
            if first_bundle is None:
                return False

            if self._resident_version.get(first_bundle) == version:
                return True
            if all_objects_exist_for_bundle(first_bundle):
                self._resident_version[first_bundle] = version
                return True

            self._hash_linked.get_next()
            self._hash_linked.add(first_bundle)
            num_bundles_skipped += 1
        return False

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
