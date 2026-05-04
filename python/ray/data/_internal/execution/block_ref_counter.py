import dataclasses
import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Dict

import ray

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )


@dataclasses.dataclass
class _BlockEntry:
    size_bytes: int
    owner: "PhysicalOperator"
    ref_count: int
    queue_holds: int = 1


class BlockRefCounter:
    """Centralized reference counter for block object-store memory.

    Tracks which PhysicalOperator produced each block and how many
    tasks currently hold it as input.

    Lifecycle of a block:
      1. Produced by operator A -> on_block_produced(ref, size, A)   (ref_count = 1, queue_holds = 1)
         Duplicate call (same ref in another bundle):                 (ref_count += 1, queue_holds += 1)
      2. Each task dispatch     -> on_block_dispatched_to_task(ref)
           queue_holds > 0: queue_holds -= 1, ref_count unchanged (task takes queue's slot)
           queue_holds == 0: ref_count += 1  (strict repartition)
      3. Each task completion   -> on_task_completed(ref)             (ref_count -= 1)
           when ref_count reaches 0, block is removed and usage is updated

    This means:
      - blocks in queues (not yet dispatched): counted once toward their owner
      - blocks held by N concurrent tasks (strict repartition): counted once
        toward their owner regardless of N
      - ref_count reaches 0 only when all tasks holding the block have completed
    """

    def __init__(self):
        self._entries: Dict["ray.ObjectRef", _BlockEntry] = {}
        # (owner_op -> total live bytes); maintained incrementally for O(1) reads.
        self._bytes_by_owner: Dict["PhysicalOperator", int] = defaultdict(int)
        self._lock = threading.Lock()

    def on_block_produced(
        self,
        block_ref: "ray.ObjectRef",
        size_bytes: int,
        owner: "PhysicalOperator",
    ) -> None:
        """Called when a block is produced by an operator.

        Sets ref_count = 1 on the first call. Duplicate calls for the same
        ObjectRef (e.g. from_pandas_refs([ref, ref])) increment ref_count and
        queue_holds without double-counting size_bytes.
        """
        with self._lock:
            if block_ref in self._entries:
                entry = self._entries[block_ref]
                entry.ref_count += 1
                entry.queue_holds += 1
                return
            self._entries[block_ref] = _BlockEntry(
                size_bytes=size_bytes, owner=owner, ref_count=1
            )
            self._bytes_by_owner[owner] += size_bytes

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

    def on_task_completed(self, block_ref: "ray.ObjectRef") -> None:
        """Called when a task that held block_ref completes, or when an inline
        operator (LimitOperator, ZipOperator, AllToAllOperator) finishes
        consuming a block.

        *NOTE*: Also called from the user thread when sink output
        blocks are consumed via _ClosingIterator.get_next().

        Decrements ref_count. When it reaches zero the block's contribution
        to its owner's usage is removed.
        """
        with self._lock:
            assert block_ref in self._entries, (
                f"on_task_completed called for untracked block {block_ref}. "
                "This indicates a missing on_block_produced call or a double completion."
            )
            entry = self._entries[block_ref]
            entry.ref_count -= 1
            if entry.ref_count == 0:
                del self._entries[block_ref]
                assert self._bytes_by_owner[entry.owner] >= entry.size_bytes, (
                    f"Usage for {entry.owner} would go negative: "
                    f"current={self._bytes_by_owner[entry.owner]}, "
                    f"removing={entry.size_bytes}"
                )
                self._bytes_by_owner[entry.owner] -= entry.size_bytes

    def get_object_store_memory_usage(self, owner: "PhysicalOperator") -> int:
        """Total bytes of blocks produced by owner that are still live."""
        with self._lock:
            return self._bytes_by_owner.get(owner, 0)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes_by_owner.clear()
