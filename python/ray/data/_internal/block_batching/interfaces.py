import abc
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Tuple

from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


@dataclass
class StageTiming:
    """Wall-clock window for a single batch-processing stage.

    Can be used as a context manager to automatically capture the start and
    end timestamps of a pipeline operation::

        with stage_timing:
            do_work()
        # stage_timing.start_s and stage_timing.end_s are now set
    """

    start_s: float = 0.0
    end_s: float = 0.0

    def __enter__(self):
        self.start_s = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end_s = time.perf_counter()

    @contextmanager
    def timer(self):
        """Alias for using as a context manager, matching Timer.timer() API."""
        self.start_s = time.perf_counter()
        try:
            yield
        finally:
            self.end_s = time.perf_counter()


@dataclass
class BatchTimings:
    """Per-batch pipeline-stage timing windows for overlap-based attribution.

    Each field records the ``(start_s, end_s)`` wall-clock window during which
    a particular pipeline stage was active for this batch.  The training thread
    later compares these windows against its own blocked window to determine
    how much each stage contributed to training-thread stall (see
    :meth:`BatchIterator._report_batch_timings`).

    Attributes:
        fetch: Waiting for upstream data production + ``ray.get()`` transfer.
        batching: Assembling blocks into a batch via ``_batcher.next_batch()``.
        format: Converting the batch to the requested format (numpy, pandas…).
        collate: Running the user-provided ``collate_fn``.
        finalize: Running the user-provided ``finalize_fn`` (e.g. host→device).
        num_rows: Number of rows in this batch (for ``iter_rows_total``).
    """

    fetch: StageTiming = field(default_factory=StageTiming)
    batching: StageTiming = field(default_factory=StageTiming)
    format: StageTiming = field(default_factory=StageTiming)
    collate: StageTiming = field(default_factory=StageTiming)
    finalize: StageTiming = field(default_factory=StageTiming)
    num_rows: int = 0

    def stages(self) -> Iterable[Tuple[str, StageTiming]]:
        """Iterate over ``(name, timing)`` pairs for all pipeline stages."""
        return (
            ("fetch", self.fetch),
            ("batching", self.batching),
            ("format", self.format),
            ("collate", self.collate),
            ("finalize", self.finalize),
        )

    def merge_fetch(self, other: "BatchTimings") -> None:
        """Merge fetch timings from another batch into this one.

        Expands the fetch window to span from the earliest block fetch start
        to the latest block fetch end. This represents the total time the
        training thread was blocked waiting for this batch, including any
        pipeline overhead between consecutive block fetches.
        """
        if other.fetch.start_s == 0.0:
            return
        if self.fetch.start_s == 0.0:
            # First block: copy the timing
            self.fetch.start_s = other.fetch.start_s
            self.fetch.end_s = other.fetch.end_s
        else:
            # Subsequent blocks: expand the window
            if other.fetch.start_s < self.fetch.start_s:
                self.fetch.start_s = other.fetch.start_s
            if other.fetch.end_s > self.fetch.end_s:
                self.fetch.end_s = other.fetch.end_s


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
