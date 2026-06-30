import abc
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from ray.data._internal.stats import IterationStage, TimeSpan
from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class BlockStageTimings:
    """Per-block timing for production_wait + data_transfer."""

    production_wait: Optional[TimeSpan] = None
    data_transfer: Optional[TimeSpan] = None


@dataclass
class PendingBlock:
    """A block reference with partial stage timings.

    ``production_wait`` is set on the first block of each ref bundle by
    ``prefetch_batches_locally``; other blocks have it as None.
    ``data_transfer`` is filled in later by ``resolve_block_refs``.
    """

    ref: ObjectRef[Block]
    stage_timings: BlockStageTimings


@dataclass
class ResolvedBlock:
    """A resolved block paired with its per-block stage timings.

    ``stage_timings`` is None when no timing was recorded (e.g. blocks
    already resolved before entering the pipeline).
    """

    block: Block
    stage_timings: Optional[BlockStageTimings] = None


@dataclass
class BatchStageTimings:
    """Per-batch timing windows for each iteration stage.

    Each field is a list of ``(start_s, end_s)`` windows the stage was
    active. production_wait and data_transfer accumulate one span per
    block; other stages have at most one span per batch. The lists are
    compared against the training thread's blocked window to attribute
    stall. Spans within a list don't overlap, so summing overlaps
    doesn't double-count.
    """

    production_wait: List[TimeSpan] = field(default_factory=list)
    data_transfer: List[TimeSpan] = field(default_factory=list)
    batching: List[TimeSpan] = field(default_factory=list)
    format: List[TimeSpan] = field(default_factory=list)
    collate: List[TimeSpan] = field(default_factory=list)
    finalize: List[TimeSpan] = field(default_factory=list)

    def stages(self) -> Iterable[Tuple[IterationStage, List[TimeSpan]]]:
        """Yield (stage, spans) pairs."""
        return (
            (IterationStage.PRODUCTION_WAIT, self.production_wait),
            (IterationStage.DATA_TRANSFER, self.data_transfer),
            (IterationStage.BATCHING, self.batching),
            (IterationStage.FORMAT, self.format),
            (IterationStage.COLLATE, self.collate),
            (IterationStage.FINALIZE, self.finalize),
        )

    def accumulate_block_timings(self, src: BlockStageTimings) -> None:
        """Accumulate a block's stage timings into this batch's lists.

        A boundary block whose rows span multiple batches is attributed
        to the first batch it lands in.
        """
        if src.production_wait is not None:
            self.production_wait.append(src.production_wait)
        if src.data_transfer is not None:
            self.data_transfer.append(src.data_transfer)


@dataclass
class BatchMetadata:
    """Metadata associated with a batch.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        num_rows: Number of rows in this batch (for ``iter_rows_total``).
        stage_timings: Per-stage timing windows.
    """

    batch_idx: int
    num_rows: int = 0
    stage_timings: BatchStageTimings = field(default_factory=BatchStageTimings)


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
