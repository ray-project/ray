import abc
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from ray.data._internal.stats import TimeSpan
from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


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

    Attributes:
        production_wait: Waiting for upstream data production (next on
            the ref bundle iterator).
        data_transfer: Cross-node transfer via ``ray.get()``.
        batching: Assembling blocks into a batch via ``_batcher.next_batch()``.
        format: Converting the batch to the requested format (numpy, pandas…).
        collate: Running the user-provided ``collate_fn``.
        finalize: Running the user-provided ``finalize_fn`` (e.g. host→device).
        num_rows: Number of rows in this batch (for ``iter_rows_total``).
    """

    production_wait: Optional[TimeSpan] = None
    data_transfer: Optional[TimeSpan] = None
    batching: Optional[TimeSpan] = None
    format: Optional[TimeSpan] = None
    collate: Optional[TimeSpan] = None
    finalize: Optional[TimeSpan] = None
    num_rows: int = 0

    def stages(self) -> Iterable[Tuple[str, Optional[TimeSpan]]]:
        """Iterate over ``(name, timing)`` pairs for all pipeline stages."""
        return (
            ("production_wait", self.production_wait),
            ("data_transfer", self.data_transfer),
            ("batching", self.batching),
            ("format", self.format),
            ("collate", self.collate),
            ("finalize", self.finalize),
        )

    def merge_fetch(self, other: "BatchTimings") -> None:
        """Merge fetch timings from another batch into this one.

        Expands each fetch sub-stage window to span from the earliest
        block's start to the latest block's end.
        """
        self.production_wait = _merge_span(self.production_wait, other.production_wait)
        self.data_transfer = _merge_span(self.data_transfer, other.data_transfer)


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
class BlockWithTiming:
    """A resolved block paired with its fetch timing window.

    Produced by :func:`resolve_block_refs` so that downstream pipeline stages
    can track how long each block took to fetch (upstream wait + ``ray.get()``).
    """

    block: Block
    timings: BatchTimings = field(default_factory=BatchTimings)


@dataclass
class BatchMetadata:
    """Metadata associated with a batch.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        timings: Pipeline-stage timing windows for this batch.
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
