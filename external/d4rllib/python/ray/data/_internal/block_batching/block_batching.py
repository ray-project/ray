from contextlib import nullcontext
from typing import Callable, Iterator, Optional, TypeVar

from ray.data._internal.block_batching.util import (
    blocks_to_batches,
    collate,
    extract_data_from_batch,
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

    def _iterator_fn(base_iterator: Iterator[Block]) -> Iterator[DataBatch]:
        batch_iter = format_batches(
            blocks_to_batches(
                block_iter=base_iterator,
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

        batch_iter = extract_data_from_batch(batch_iter)
        yield from batch_iter

    batch_iter = _iterator_fn(blocks)

    for formatted_batch in batch_iter:
        user_timer = stats.iter_user_s.timer() if stats else nullcontext()
        with user_timer:
            yield formatted_batch
