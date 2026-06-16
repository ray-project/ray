import abc
from dataclasses import dataclass, field
from typing import Any, List

from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class BatchTimings:
    """Per-stage pipeline duration measurements for one batch.

    Stages that can touch multiple blocks (fetch) use **accumulated durations**
    so that multi-block batches are counted correctly and single-block batches
    that yield multiple downstream batches are not double-counted.

    Stages that happen exactly once per batch (format, collate, finalize) use
    **absolute timestamps** so consecutive deltas give their durations.

    All durations/timestamps use ``time.perf_counter()`` for high resolution.
    A value of ``0.0`` means the stage was skipped (e.g. no collate_fn).
    """

    # --- Accumulated durations (safe across multi-block batches) ---
    # Sum of ray.get() elapsed times for every block that fed this batch.
    # Accumulated by resolve_block_refs; reset to 0.0 when a batch is yielded.
    fetch_duration_s: float = 0.0

    # Time spent inside _batcher.next_batch() for this batch.
    batching_duration_s: float = 0.0

    # --- Absolute timestamps (each stage fires exactly once per batch) ---
    # Recorded when the batch leaves _BatchingIterator; used as the reference
    # point for computing format/collate/finalize deltas downstream.
    batching_done_s: float = 0.0

    # Set in the format/collate thread-pool worker.
    format_done_s: float = 0.0   # batch_format conversion finished
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
