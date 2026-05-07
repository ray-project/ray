import dataclasses
import threading
from collections import defaultdict
from typing import Dict

import ray


@dataclasses.dataclass
class _BlockEntry:
    size_bytes: int
    producer_id: str
    ref_count: int
    queue_holds: int = 1


class BlockRefCounter:
    """Centralized reference counter for block object-store memory.

    Tracks which PhysicalOperator produced each block and how many
    tasks currently hold it as input.

    Lifecycle of a block:
      1. Produced by operator A -> on_block_produced(ref, size, A.id)   (ref_count = 1, queue_holds = 1)
         Duplicate call (same ref in another bundle):                    (ref_count += 1, queue_holds += 1)
      2. Each task dispatch     -> on_block_dispatched_to_task(ref)
           queue_holds > 0: queue_holds -= 1, ref_count unchanged (task takes queue's slot)
           queue_holds == 0: ref_count += 1  (strict repartition)
      3. Each holder releases   -> on_block_released(ref)                (ref_count -= 1)
           when ref_count reaches 0, block is removed and usage is updated

    This means:
      - blocks in queues (not yet dispatched): size_bytes counted once toward their producer
      - blocks held by N concurrent tasks (strict repartition): ref_count incremented
        once per task (so ref_count = N), but size_bytes counted once toward their producer
      - ref_count reaches 0 only when all tasks holding the block have completed
    """

    def __init__(self):
        self._entries: Dict["ray.ObjectRef", _BlockEntry] = {}
        # (producer_id -> total live bytes); maintained incrementally for O(1) reads.
        self._bytes_by_producer: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def on_block_produced(
        self,
        block_ref: "ray.ObjectRef",
        size_bytes: int,
        producer_id: str,
    ) -> None:
        """Called when a block is produced by an operator.

        Sets ref_count = 1 on the first call. Duplicate calls for the same
        ObjectRef (e.g. from_pandas_refs([ref, ref])) increment ref_count and
        queue_holds without double-counting size_bytes:
            queue: [bundle_A(ref), bundle_B(ref)]
            on_block_produced(ref) x2 => qh=2, rc=2
        Each bundle is a separate queue slot, so both tasks will consume ref.
        """
        with self._lock:
            if block_ref in self._entries:
                entry = self._entries[block_ref]
                entry.ref_count += 1
                entry.queue_holds += 1
                return
            self._entries[block_ref] = _BlockEntry(
                size_bytes=size_bytes, producer_id=producer_id, ref_count=1
            )
            self._bytes_by_producer[producer_id] += size_bytes

    def on_block_dispatched_to_task(self, block_ref: "ray.ObjectRef") -> None:
        """Called each time a task takes block_ref as input.

        If queue_holds > 0: queue_holds is decremented, ref_count unchanged
        (task takes the queue's slot).

        If queue_holds == 0 (strict repartition): ref_count is incremented so
        the block is not removed when the first task completes.
        """
        with self._lock:
            assert (
                block_ref in self._entries
            ), f"on_block_dispatched_to_task called for untracked block {block_ref}."
            entry = self._entries[block_ref]
            if entry.queue_holds > 0:
                entry.queue_holds -= 1
            else:
                entry.ref_count += 1

    def on_block_queue_slot_added(self, block_ref: "ray.ObjectRef") -> None:
        """Called when an existing block gains an additional queue slot.

        Used when RebundleQueue slices a block across two task inputs, putting
        the same ObjectRef into both the dispatched bundle and the remaining
        pending bundle. Unlike on_block_produced, the extra slot is created
        internally by the bundler without a second production event:
            on_block_produced(ref) => qh=1, rc=1
            slice: ready(ref: rows 0-99), pending(ref: rows 100-149)
            on_block_queue_slot_added(ref) => qh=2, rc=2
        No-op if the entry has already been freed.
        """
        with self._lock:
            entry = self._entries.get(block_ref)
            if entry is None:
                return
            entry.queue_holds += 1
            entry.ref_count += 1

    def on_block_released(self, block_ref: "ray.ObjectRef") -> None:
        """Called when any holder of block_ref releases it.

        A holder is either a queue slot or an in-flight task. Concretely:
          - A Ray task completed (map/shuffle/etc.)
          - A block was dropped from an internal queue without being dispatched
            (e.g. LimitOperator early termination, OutputSplitter truncation, shutdown)
          - An inline operator (ZipOperator, AllToAllOperator) finished consuming a block
          - A sink consumed a block via _ClosingIterator.get_next()

        Decrements ref_count. When it reaches zero the block's contribution
        to its producer's usage is removed.
        """
        with self._lock:
            assert block_ref in self._entries, (
                f"on_block_released called for untracked block {block_ref}. "
                "This indicates a missing on_block_produced call or a double release."
            )
            entry = self._entries[block_ref]
            entry.ref_count -= 1
            if entry.ref_count == 0:
                del self._entries[block_ref]
                assert self._bytes_by_producer[entry.producer_id] >= entry.size_bytes, (
                    f"Usage for {entry.producer_id} would go negative: "
                    f"current={self._bytes_by_producer[entry.producer_id]}, "
                    f"removing={entry.size_bytes}"
                )
                self._bytes_by_producer[entry.producer_id] -= entry.size_bytes

    def get_object_store_memory_usage(self, producer_id: str) -> int:
        """Total bytes of blocks produced by producer_id that are still live."""
        with self._lock:
            return self._bytes_by_producer.get(producer_id, 0)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes_by_producer.clear()
