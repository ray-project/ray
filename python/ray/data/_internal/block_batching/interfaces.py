import abc
from dataclasses import dataclass, field
from typing import Any, List

from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class BatchTimings:
    """Per-stage pipeline timing windows for one batch (issue #64132).

    Each stage records absolute ``(start_s, end_s)`` timestamps using
    ``time.perf_counter()``.  The training thread captures its own
    ``(blocked_start, blocked_end)`` window around ``next(batch_iter)``.
    Attribution for each stage is then:

        overlap = max(0, min(stage_end, blocked_end) - max(stage_start, blocked_start))

    This correctly handles prefetch_batches > 1: a stage that finished before
    the training thread started waiting gets zero credit, even if it took a
    long time — it ran in parallel and did not delay training.

    Thread-local storage in util.py carries ``fetch_start_s`` / ``fetch_end_s``
    from ``resolve_block_refs`` (background thread) to ``_BatchingIterator``
    (same thread).  After ``_BatchingIterator`` reads the values it resets them
    to 0.0, so a large block that feeds multiple batches is credited only to
    the first batch and not double-counted.

    A value of ``0.0`` means the stage was skipped (e.g. no ``collate_fn``).
    """

    # Block fetch window: ray.get() across all blocks that fed this batch.
    # fetch_start_s is set on the FIRST block only; fetch_end_s is updated on
    # every block so it ends up as the last block's ray.get() finish time.
    fetch_start_s: float = 0.0
    fetch_end_s: float = 0.0

    # Batching window: time inside _batcher.next_batch().
    batching_start_s: float = 0.0
    batching_done_s: float = 0.0

    # Set in the format/collate thread-pool worker.
    format_done_s: float = 0.0  # batch_format conversion finished
    collate_done_s: float = 0.0  # collate_fn finished (0.0 if no collate_fn)

    # Set in the iteration background thread after the thread-pool rejoins.
    finalize_done_s: float = 0.0  # finalize_fn finished (0.0 if no finalize_fn)

    # Row count for iter_rows_total
    num_rows: int = 0


@dataclass
class BatchMetadata:
    """Metadata associated with a batch.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations
            can maintain ordering.
        timings: Per-stage pipeline measurements for latency attribution.
    """

    batch_idx: int
    timings: BatchTimings = field(default_factory=BatchTimings)


@dataclass
class Batch:
    """A batch of data.

    Attributes:
        metadata: Metadata associated with this batch.
        data: The batch of data.
    """

    metadata: BatchMetadata
    data: DataBatch


class CollatedBatch(Batch):
    """A batch of collated data.

    Attributes:
        data: The batch of data which is the output of a user provided collate_fn
            Therefore, the type of this data can be Any.
    """

    data: Any


class BlockPrefetcher(metaclass=abc.ABCMeta):
    """Interface for prefetching blocks."""

    @abc.abstractmethod
    def prefetch_blocks(self, blocks: List[ObjectRef[Block]]):
        """Prefetch the provided blocks to this node."""
        pass

    def stop(self):
        """Stop prefetching and release resources."""
        pass
