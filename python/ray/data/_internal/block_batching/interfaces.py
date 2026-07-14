import abc
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from ray.data._internal.stats import IterationStage, TimeSpan
from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class BlockStageTimings:
    """Per-block timing for production_wait + data_transfer.

    Both fields are always populated when ``stage_timings`` is set on a
    ``ResolvedBlock``; the outer ``ResolvedBlock.stage_timings`` Optional
    encodes "no timing recorded" (e.g. blocks already resolved before
    entering the pipeline).
    """

    production_wait: TimeSpan
    data_transfer: TimeSpan


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

    Fetch stages (production_wait, data_transfer) accumulate one span per
    block, so they are ``List[TimeSpan]``. Other stages run at most once
    per batch, so they are ``Optional[TimeSpan]``. ``stages()`` yields
    ``List[TimeSpan]`` for all stages (single spans wrapped in a 1-element
    list) so ``_attribute_blocked_time`` can use uniform overlap logic.
    """

    production_wait: List[TimeSpan] = field(default_factory=list)
    data_transfer: List[TimeSpan] = field(default_factory=list)
    batching: Optional[TimeSpan] = None
    format: Optional[TimeSpan] = None
    collate: Optional[TimeSpan] = None
    finalize: Optional[TimeSpan] = None

    def stages(self) -> Iterable[Tuple[IterationStage, List[TimeSpan]]]:
        """Yield (stage, spans) pairs, wrapping single spans in a list."""
        return (
            (IterationStage.PRODUCTION_WAIT, self.production_wait),
            (IterationStage.DATA_TRANSFER, self.data_transfer),
            (
                IterationStage.BATCHING,
                [self.batching] if self.batching is not None else [],
            ),
            (IterationStage.FORMAT, [self.format] if self.format is not None else []),
            (
                IterationStage.COLLATE,
                [self.collate] if self.collate is not None else [],
            ),
            (
                IterationStage.FINALIZE,
                [self.finalize] if self.finalize is not None else [],
            ),
        )

    def accumulate_block_timings(self, src: BlockStageTimings) -> None:
        """Accumulate a block's fetch timings into this batch's lists.

        A boundary block whose rows span multiple batches is attributed
        to the first batch it lands in.
        """
        self.production_wait.append(src.production_wait)
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
