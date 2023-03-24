import collections
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, DataBatch
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.util import ActorBlockPrefetcher, WaitBlockPrefetcher, resolve_block_refs, blocks_to_batches, format_batches, collate
from ray.data._internal.stats import DatasetStats
from ray.data.context import DatasetContext

def iter_batches(
    block_refs: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    *,
    stats: Optional[DatasetStats] = None,
    clear_block_after_read: bool = False,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
    prefetch_batches: int = 0,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from an iterator of block object references and
    corresponding metadata.

    This takes a block iterator and creates batch_size batches, slicing,
    unioning, shuffling, prefetching, and formatting blocks as needed.


    The algorithm uses both pipeline parallelism and data parallelism:

    If prefetch_batches=2, these are all the batches in flight:

    [User thread] trains on Batch 0
    - [Fetch thread] Batch 1 in output queue
            - [Worker thread 1] Batch 2 formatting + collating
            - [Worker thread 2] Batch 3 formatting + collating
            - [Raylet] Batches 4 + 5 fetched to local object store memory

    At any point in time there are prefetch_batches+1 batches in local heap memory.
    And the next set of prefetch_batches in local object store memory.

    The actual steps are as follows:

    In a single async thread, do the following:
        1. Trigger Ray local prefetching of `prefetch_batches` worth of block object
            references.
        2. Resolve (i.e. call `ray.get()`) on the block references.
        3. Perform the necessary batch slicing to construct full batches, possibly
            shuffling if necessary.
        4. Then, in a threadpool consisting of `prefetch_batches` threads:
            3. Format the batches to the provided batch format.
            4. Apply the collate function.
        5. Fetch outputs from the threadpool, maintaining order of the batches.

    Args:
        block_refs: An iterator over block object references and their corresponding
            metadata.
        stats: DatasetStats object to record timing and other statistics.
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
        prefetch_batches: The number of batches to fetch ahead of the current batch to
            process. If set to greater than 0, a separate thread will be used to fetch
            the specified amount of formatted batches from blocks. This improves
            performance for non-CPU bound UDFs, allowing batch fetching compute and
            formatting to be overlapped with the UDF. Defaults to 0 (no prefetching
            enabled).

    Returns:
        An iterator over record batches.
    """
    context = DatasetContext.get_current()

    if (
        prefetch_batches > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()

    eager_free = clear_block_after_read and DatasetContext.get_current().eager_free

    def _async_iter_batches(
        block_refs: Iterator[ObjectRef[Block]],
    ) -> Iterator[DataBatch]:
        
        if prefetch_batches > 0:
            # Step 1: Prefetch logical batches locally.
            block_refs = prefetch_batches_locally(
                block_ref_iter=block_refs,
                prefetcher=prefetcher,
                num_batches_to_prefetch=prefetch_batches,
                batch_size=batch_size,
            )

        # Step 2: Resolve the blocks.
        block_iter = resolve_block_refs(
            block_ref_iter=block_refs, 
            eager_free=eager_free,
            stats=stats
        )
        
        # Step 3: Batch and shuffle the resolved blocks.
        batch_iter = blocks_to_batches(
            block_iter=block_iter,
            stats=stats,
            batch_size=batch_size,
            drop_last=drop_last,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
            ensure_copy=ensure_copy
        )

        # Step 4: Use a threadpool for formatting and collation.
        batch_iter = _batch_in_threadpool(
            batch_iter,
            stats=stats,
            batch_format=batch_format,
            collate_fn=collate_fn,
            ensure_copy=ensure_copy,
            num_threadpool_workers=prefetch_batches,
        )

        # Step 5: Restore original order.
        batch_iter: Iterator[Batch] = restore_from_original_order(batch_iter)

        for batch in batch_iter:
            yield batch.data

    # Run everything in a separate thread to not block the main thread when waiting
    # for streaming results.
    async_batch_iter = _make_async_gen(
        block_refs, fn=_async_iter_batches, num_workers=1
    )

    while True:
        with stats.iter_total_blocked_s.timer() if stats else nullcontext():
            try:
                next_batch = next(async_batch_iter)
            except StopIteration:
                break
        with stats.iter_user_s.timer() if stats else nullcontext():
            yield next_batch


def _batch_in_threadpool(
    logical_batch_iterator: Iterator[LogicalBatch],
    stats: DatasetStats,
    batch_format: str = "default",
    collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    ensure_copy: bool = False,
    num_threadpool_workers: int = 0,
) -> Iterator[Batch]:
    """Executes the batching, formatting, and collation logic in a threadpool.

    Args:
        logical_batch_iterator: An iterator over logical batches.
        stats: DatasetStats object to record timing and other statistics.
        batch_format: The format in which to return each batch.
            Specify "default" to use the current block format (promoting
            Arrow to pandas automatically), "pandas" to
            select ``pandas.DataFrame`` or "pyarrow" to select
            ``pyarrow.Table``. Default is "default".
        collate_fn: A function to apply to each data batch before returning it.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).
        num_threadpool_workers: The number of threads to use in the threadpool.
    """

    def threadpool_computations(
        logical_batch_iter: Iterator[LogicalBatch],
    ) -> Iterator[Batch]:
        # Step 4.1: Resolve the blocks.
        resolved_batch_iter = _resolve_logical_batch(logical_batch_iter, stats=stats)

        # Step 4.2: Slice the blocks to create the batch.
        batch_iter = _construct_batch_from_logical_batch(
            resolved_batch_iter, ensure_copy=ensure_copy, stats=stats
        )

        # Step 4.3: Format the batches.
        formatted_batch_iter = _format_batches(
            batch_iter, batch_format=batch_format, stats=stats
        )

        # Step 4.4: Apply the collate function if applicable.
        if collate_fn is not None:
            formatted_batch_iter = _collate(
                formatted_batch_iter, collate_fn=collate_fn, stats=stats
            )
        yield from formatted_batch_iter

    return _make_async_gen(
        base_iterator=logical_batch_iterator,
        fn=threadpool_computations,
        num_workers=num_threadpool_workers,
    )

def prefetch_batches_locally(
    block_ref_iter: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    prefetcher: BlockPrefetcher,
    num_batches_to_prefetch: int,
    batch_size: Optional[int],
) -> Iterator[List[ObjectRef[Block]]]:
    """Given an iterator of batched block references, returns an iterator over the same
    block references while prefetching `num_batches_to_prefetch` batches in advance.

    Args:
        block_ref_iter: An iterator over batched block references.
        prefetcher: The prefetcher to use.
        num_batches_to_prefetch: The number of batches to prefetch ahead of the
            current batch during the scan.
        batch_size: User specified batch size, or None to let the system pick.
    """

    sliding_window = collections.deque()
    current_window_size = 0

    if batch_size:
        num_rows_to_prefetch = num_batches_to_prefetch * batch_size

    # Create and fetch the initial window.
    while True:
        try:
            next_block_ref_and_metadata = next(block_ref_iter)
            sliding_window.append(next_block_ref_and_metadata)
            current_window_size += next_block_ref_and_metadata[1].num_rows
        except StopIteration:
            break
        if batch_size and current_window_size >= num_rows_to_prefetch:
            break
        elif not batch_size and len(sliding_window) >= num_batches_to_prefetch:
            break

    prefetcher.prefetch_blocks([block_ref for block_ref, _ in list(sliding_window)])

    while sliding_window:
        block_ref, metadata = sliding_window.popleft()
        current_window_size -= metadata.num_rows
        if not batch_size or current_window_size < num_rows_to_prefetch:
            try:
                sliding_window.append(next(block_ref_iter))
                prefetcher.prefetch_blocks(
                    [block_ref for block_ref, _ in list(sliding_window)]
                )
            except StopIteration:
                pass
        yield block_ref


def restore_from_original_order(batch_iter: Iterator[Batch]) -> Iterator[Batch]:
    """Restores the original order of the provided `batch_iter`

    This function will yield items from `base_iterator` in the correct order based on
    each batch's batch_idx. All indexes are expected to be unique.

    `batch_iter` is expected to not have any missing indexes. All indexes from 0 to len
    (base_iterator) must be present.
    """
    next_index_required = 0
    buffer: Dict[int, Batch] = {}
    for batch in batch_iter:
        assert batch.batch_idx not in buffer
        buffer[batch.batch_idx] = batch
        while next_index_required in buffer:
            yield buffer.pop(next_index_required)
            next_index_required += 1

    while next_index_required in buffer:
        yield buffer.pop(next_index_required)
        next_index_required += 1
