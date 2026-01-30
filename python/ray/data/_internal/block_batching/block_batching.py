from contextlib import nullcontext
from typing import Callable, Iterator, Optional, TypeVar

from ray.data._internal.block_batching.interfaces import Batch
from ray.data._internal.block_batching.util import (
    blocks_to_batches,
    collate,
    format_batches,
)
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, DataBatch

T = TypeVar("T")


def batch_blocks(
    blocks: Iterator[Block],
    *,
    stats: Optional[DatasetStats] = None,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], DataBatch]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from 1 or more blocks.

    This function takes in an iterator of already fetched blocks. Consequently, this
    function doesn't support block prefetching.
    """
    # Build the processing pipeline
    batch_iter = format_batches(
        blocks_to_batches(
            block_iter=blocks,
            stats=stats,
            batch_size=batch_size,
            drop_last=drop_last,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
            ensure_copy=ensure_copy,
        ),
        batch_format=batch_format,
        stats=stats,
    )

    if collate_fn is not None:
        batch_iter = collate(batch_iter, collate_fn=collate_fn, stats=stats)

    unwrapped = _UnwrappingIterator(batch_iter)

    # TODO restore
    # return _user_timed_iter(unwrapped, stats)
    return unwrapped


def _user_timed_iter(
    iter: Iterator[DataBatch], stats: Optional[DatasetStats]
) -> Iterator[DataBatch]:
    for batch in iter:
        # Track iteration's time spent in user code
        timer = stats.iter_user_s.timer() if stats else nullcontext()
        with timer:
            yield batch


class _UnwrappingIterator(Iterator[DataBatch]):
    """Iterator that unwraps `Batch` into underlying `DataBatch`."""

    def __init__(
        self,
        batches: Iterator["Batch"],
    ):
        self._batches_iter = batches

    def __iter__(self) -> "_UnwrappingIterator":
        return self

    def __next__(self) -> DataBatch:
        batch = next(self._batches_iter)

        return batch.data
