import abc
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Tuple

from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class StageTiming:
    """Wall-clock window for a batch-processing stage."""

    start_s: float = 0.0
    end_s: float = 0.0

    def record(self, start_s: float, end_s: float) -> None:
        if self.start_s == 0.0:
            self.start_s = start_s
        self.end_s = end_s


@dataclass
class BatchTimings:
    fetch: StageTiming = field(default_factory=StageTiming)
    batching: StageTiming = field(default_factory=StageTiming)
    format: StageTiming = field(default_factory=StageTiming)
    collate: StageTiming = field(default_factory=StageTiming)
    finalize: StageTiming = field(default_factory=StageTiming)
    num_rows: int = 0

    def stages(self) -> Iterable[Tuple[str, StageTiming]]:
        return (
            ("fetch", self.fetch),
            ("batching", self.batching),
            ("format", self.format),
            ("collate", self.collate),
            ("finalize", self.finalize),
        )

    def merge_fetch(self, other: "BatchTimings") -> None:
        self._merge_stage(self.fetch, other.fetch)

    @staticmethod
    def _merge_stage(dst: StageTiming, src: StageTiming) -> None:
        if src.start_s == 0.0:
            return
        if dst.start_s == 0.0 or src.start_s < dst.start_s:
            dst.start_s = src.start_s
        if src.end_s > dst.end_s:
            dst.end_s = src.end_s


@dataclass
class BlockWithTiming:
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
