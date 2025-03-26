import time
from typing import TYPE_CHECKING, Dict, Optional

import ray
from ray._private.ray_constants import DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE
from ray.data._internal.execution.bundle_queue import BundleQueue, FIFOBundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class LocationAwareBundleQueue(BundleQueue):
    """Queue that prioritizes bundles that reside in Object Store memory."""

    UPDATE_FREQUENCY_S = 30

    def __init__(self):
        self._fifo_queue = FIFOBundleQueue()
        self._bundle_nbytes: Dict["RefBundle", int] = {}
        self._last_size_refresh_ts = time.time()
        self._total_nbytes = 0

    def __len__(self) -> int:
        return len(self._fifo_queue)

    def __contains__(self, bundle: "RefBundle") -> bool:
        return bundle in self._fifo_queue

    def add(self, bundle: "RefBundle") -> None:
        self._fifo_queue.add(bundle)

        # Use `"RefBundle".size_bytes()` as an initial estimate.
        self._bundle_nbytes[bundle] = bundle.size_bytes()
        self._total_nbytes += self._bundle_nbytes[bundle]

    def pop(self) -> "RefBundle":
        if not self._fifo_queue:
            raise IndexError("You can't pop from an empty queue")

        self._try_ensure_first_bundle_exists()
        bundle = self._fifo_queue.peek()
        self.remove(bundle)
        return bundle

    def peek(self) -> Optional["RefBundle"]:
        self._try_ensure_first_bundle_exists()
        return self._fifo_queue.peek()

    def remove(self, bundle: "RefBundle") -> None:
        if bundle not in self._bundle_nbytes:
            raise ValueError(f"Bundle {bundle} not found in the queue")

        self._fifo_queue.remove(bundle)

        nbytes = self._bundle_nbytes[bundle]
        self._total_nbytes -= nbytes
        assert self._total_nbytes >= 0, (
            "Expected the total size of objects in the queue to be non-negative, but "
            f"got {self._total_nbytes} bytes instead."
        )

        if bundle not in self._fifo_queue:
            del self._bundle_nbytes[bundle]

    def clear(self):
        self._fifo_queue.clear()
        self._bundle_nbytes.clear()
        self._total_nbytes = 0

    def estimate_size_bytes(self) -> int:
        now = time.time()
        # Bundle sizes can change if Ray loses objects or creates replicas. So, we
        # update the sizes every `UPDATE_FREQUENCY_S` seconds.
        if now - self._last_size_refresh_ts > self.UPDATE_FREQUENCY_S:
            self._refresh_bundle_sizes()
            self._total_nbytes = sum(self._bundle_nbytes.values())
            self._last_size_refresh_ts = now
        return self._total_nbytes

    def _try_ensure_first_bundle_exists(self):
        if not self._fifo_queue:
            return

        num_bundles_skipped = 0
        while num_bundles_skipped < len(self._bundle_nbytes):
            first_bundle = self._fifo_queue.peek()
            assert first_bundle is not None

            if self._objects_exist(first_bundle):
                break

            self._fifo_queue.pop()
            self._fifo_queue.add(first_bundle)
            num_bundles_skipped += 1

    def _objects_exist(self, bundle) -> bool:
        # 'get_local_object_locations' †ells us which nodes each object resides on. If
        # "node_ids" is an empty list, it means that block isn't in object store memory.
        object_locs = ray.experimental.get_local_object_locations(bundle.block_refs)
        return all(
            # If the object is small enough, the object isn't placed in the object
            # store and the list of node IDs is empty.
            (
                object_info["object_size"] is not None
                and object_info["object_size"] < DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE
            )
            or len(object_info["node_ids"]) > 0
            for object_info in object_locs.values()
        )

    def _refresh_bundle_sizes(self) -> None:
        for bundle in self._bundle_nbytes:
            object_locs = ray.experimental.get_local_object_locations(bundle.block_refs)

            nbytes = 0
            for object_info in object_locs.values():
                if object_info["object_size"] is None:
                    nbytes = 0
                elif object_info["object_size"] < DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE:
                    # If the object is smaller than the threshold, then 'node_ids'
                    # is an empty list.
                    nbytes += object_info["object_size"]
                else:
                    # There can be copies of the object on multiple nodes. So, to
                    # calculate the total size of the object in shared object store
                    # memory, we multiply the object size by the number of nodes the
                    # object resides on.
                    nbytes += len(object_info["node_ids"]) * object_info["object_size"]

            assert nbytes >= 0, nbytes
            self._bundle_nbytes[bundle] = nbytes

    def is_empty(self):
        return not self._fifo_queue and not self._bundle_nbytes
