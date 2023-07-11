import collections
import itertools
from contextlib import nullcontext
from typing import Any, Callable, Iterator, Optional, TypeVar, Union

import ray
from ray.data._internal.block_batching.interfaces import BlockPrefetcher
from ray.data._internal.block_batching.util import (
    ActorBlockPrefetcher,
    WaitBlockPrefetcher,
    blocks_to_batches,
    collate,
    extract_data_from_batch,
    format_batches,
    resolve_block_refs,
)
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.stats import DatasetPipelineStats, DatasetStats
from ray.data.block import Block, DataBatch
from ray.data.context import DataContext
from ray.types import ObjectRef

T = TypeVar("T")


def batch_block_refs(
    block_refs: Iterator[ObjectRef[Block]],
    *,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    prefetch_blocks: int = 0,
    clear_block_after_read: bool = False,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from 1 or more block object references.

    This takes a block iterator and creates batch_size batches, slicing,
    unioning, shuffling, prefetching, and formatting blocks as needed.

    This is used by both Dataset.iter_batches()/DatasetPipeline.iter_batches()
    and Dataset.map_batches()/DatasetPipeline.map_batches().

    Args:
        block_refs: An iterator over block object references.
        prefetch_blocks: The number of blocks to prefetch ahead of the
            current block during the scan.
        clear_block_after_read: Whether to clear the block from object store
            manually (i.e. without waiting for Python's automatic GC) after it
            is read. Doing so will reclaim memory faster and hence reduce the
            memory footprint. However, the caller has to ensure the safety, i.e.
            the block will never be accessed again.
        batch_size: Record batch size, or None to let the system pick.
        batch_format: The format in which to return each batch.
            Specify "default" to use the current block format (promoting
            Arrow to pandas automatically), "pandas" to
            select ``pandas.DataFrame`` or "pyarrow" to select
            ``pyarrow.Table``. Default is "default".
        drop_last: Whether to drop the last batch if it's incomplete.
        collate_fn: A function to apply to each data batch before returning it.
        shuffle_buffer_min_size: If non-None, the data will be randomly shuffled using a
            local in-memory shuffle buffer, and this value will serve as the minimum
            number of rows that must be in the local in-memory shuffle buffer in order
            to yield a batch.
        shuffle_seed: The seed to use for the local random shuffle.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).

    Returns:
        An iterator over record batches.
    """
    if stats:
        stats._legacy_iter_batches = True
    context = DataContext.get_current()

    if (
        prefetch_blocks > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()

    eager_free = clear_block_after_read and DataContext.get_current().eager_free

    block_iter = resolve_block_refs(
        _prefetch_blocks(
            block_ref_iter=block_refs,
            prefetcher=prefetcher,
            num_blocks_to_prefetch=prefetch_blocks,
            eager_free=eager_free,
        ),
        stats=stats,
    )

    yield from batch_blocks(
        block_iter,
        stats=stats,
        batch_size=batch_size,
        batch_format=batch_format,
        drop_last=drop_last,
        collate_fn=collate_fn,
        shuffle_buffer_min_size=shuffle_buffer_min_size,
        shuffle_seed=shuffle_seed,
        ensure_copy=ensure_copy,
    )


def batch_blocks(
    blocks: Iterator[Block],
    *,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], DataBatch]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from 1 or more blocks.

    This is equivalent to batch_block_refs, except
    it takes in an iterator consisting of already fetched blocks.
    This means that this function does not support block prefetching.
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
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
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
