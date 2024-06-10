import collections
import itertools
from contextlib import nullcontext
from typing import Callable, Iterator, Optional, TypeVar

from ray.data._internal.block_batching.interfaces import BlockPrefetcher
from ray.data._internal.block_batching.util import (
    blocks_to_batches,
    collate,
    extract_data_from_batch,
    format_batches,
)
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, DataBatch
from ray.types import ObjectRef

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


def _prefetch_blocks(
    block_ref_iter: Iterator[ObjectRef[Block]],
    prefetcher: BlockPrefetcher,
    num_blocks_to_prefetch: int,
    eager_free: bool = False,
    stats: Optional[DatasetStats] = None,
) -> Iterator[ObjectRef[Block]]:
    """Given an iterable of Block Object References, returns an iterator
    over these object reference while prefetching `num_block_to_prefetch`
    blocks in advance.

    Args:
        block_ref_iter: An iterator over block object references.
        num_blocks_to_prefetch: The number of blocks to prefetch ahead of the
            current block during the scan.
        stats: Dataset stats object used to store block wait time.
    """
    if num_blocks_to_prefetch == 0:
        for block_ref in block_ref_iter:
            yield block_ref
            trace_deallocation(
                block_ref, "block_batching._prefetch_blocks", free=eager_free
            )

    window_size = num_blocks_to_prefetch
    # Create the initial set of blocks to prefetch.
    sliding_window = collections.deque(
        itertools.islice(block_ref_iter, window_size), maxlen=window_size
    )
    with stats.iter_wait_s.timer() if stats else nullcontext():
        prefetcher.prefetch_blocks(list(sliding_window))

    while sliding_window:
        block_ref = sliding_window.popleft()
        try:
            sliding_window.append(next(block_ref_iter))
            with stats.iter_wait_s.timer() if stats else nullcontext():
                prefetcher.prefetch_blocks(list(sliding_window))
        except StopIteration:
            pass
        yield block_ref
        trace_deallocation(
            block_ref, "block_batching._prefetch_blocks", free=eager_free
        )
    prefetcher.stop()
