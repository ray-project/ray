import abc
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from ray.data._internal.stats import IterationStage, TimeSpan
from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class BlockFetchTiming:
    """Fetch timing for a single block (production_wait + data_transfer)."""

    production_wait: Optional[TimeSpan] = None
    data_transfer: Optional[TimeSpan] = None


@dataclass
class BlockFetchResult:
    """A resolved block paired with its per-block fetch timing.

    ``fetch`` is None when no timing was recorded (e.g. blocks already
    resolved before entering the pipeline).
    """

    block: Block
    fetch: Optional[BlockFetchTiming] = None


@dataclass
class BatchTimings:
    """Per-batch timing windows for each iteration stage.

    Each field is the ``(start_s, end_s)`` window a stage was active, or
    None if the stage didn't run. Compared against the training thread's
    blocked window to attribute stall (see ``_attribute_blocked_time``).
    """

    production_wait: Optional[TimeSpan] = None
    data_transfer: Optional[TimeSpan] = None
    batching: Optional[TimeSpan] = None
    format: Optional[TimeSpan] = None
    collate: Optional[TimeSpan] = None
    finalize: Optional[TimeSpan] = None

    def stages(self) -> Iterable[Tuple[IterationStage, Optional[TimeSpan]]]:
        """Yield (stage, timing) pairs."""
        return (
            (IterationStage.PRODUCTION_WAIT, self.production_wait),
            (IterationStage.DATA_TRANSFER, self.data_transfer),
            (IterationStage.BATCHING, self.batching),
            (IterationStage.FORMAT, self.format),
            (IterationStage.COLLATE, self.collate),
            (IterationStage.FINALIZE, self.finalize),
        )

    def merge_fetch(self, src: BlockFetchTiming) -> None:
        """Merge per-block fetch timings into this batch's fetch windows."""
        self.production_wait = _merge_span(self.production_wait, src.production_wait)
        self.data_transfer = _merge_span(self.data_transfer, src.data_transfer)


def _merge_span(dst: Optional[TimeSpan], src: Optional[TimeSpan]) -> Optional[TimeSpan]:
    """Return the union of two optional windows (or the non-None one)."""
    if src is None:
        return dst
    if dst is None:
        return TimeSpan(start_s=src.start_s, end_s=src.end_s)
    return TimeSpan(
        start_s=min(dst.start_s, src.start_s),
        end_s=max(dst.end_s, src.end_s),
    )


@dataclass
class BatchMetadata:
    """Metadata associated with a batch.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        num_rows: Number of rows in this batch (for ``iter_rows_total``).
        timings: Per-stage timing windows.
    """

    batch_idx: int
    num_rows: int = 0
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
