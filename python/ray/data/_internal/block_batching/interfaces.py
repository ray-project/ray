import abc
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from ray.data._internal.stats import IterationStage, TimeSpan
from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class BlockFetchTiming:
    """Fetch timing for a single block (production_wait + data_transfer).

    Produced by :func:`resolve_block_refs` and merged into
    :class:`BatchTimings` by :class:`_BatchingIterator`.
    """

    production_wait: Optional[TimeSpan] = None
    data_transfer: Optional[TimeSpan] = None


@dataclass
class BlockFetchResult:
    """A resolved block paired with its per-block fetch timing.

    When ``fetch`` is ``None``, no fetch timing was recorded (e.g. blocks
    that were already resolved before entering the pipeline).
    """

    block: Block
    fetch: Optional[BlockFetchTiming] = None


@dataclass
class BatchTimings:
    """Per-batch pipeline-stage timing windows for overlap-based attribution.

    Each field records the ``(start_s, end_s)`` wall-clock window during which
    a particular pipeline stage was active for this batch.  The training thread
    later compares these windows against its own blocked window to determine
    how much each stage contributed to training-thread stall (see
    :meth:`BatchIterator._attribute_blocked_time`).

    A field value of ``None`` indicates the stage did not execute for this
    batch (e.g. no ``collate_fn`` provided).
    """

    production_wait: Optional[TimeSpan] = None
    data_transfer: Optional[TimeSpan] = None
    batching: Optional[TimeSpan] = None
    format: Optional[TimeSpan] = None
    collate: Optional[TimeSpan] = None
    finalize: Optional[TimeSpan] = None

    def stages(self) -> Iterable[Tuple[IterationStage, Optional[TimeSpan]]]:
        """Iterate over ``(stage, timing)`` pairs for all pipeline stages."""
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
    """Merge two optional ``TimeSpan`` windows into a spanning window.

    Returns ``dst`` unchanged if ``src`` is ``None`` (stage didn't run).
    Returns a copy of ``src`` if ``dst`` is ``None`` (first block).
    Otherwise returns a new ``TimeSpan`` spanning both windows.
    """
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
        timings: Pipeline-stage timing windows for this batch.
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
