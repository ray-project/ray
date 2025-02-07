import collections
from contextlib import nullcontext
from typing import Any, Callable, Dict, Iterator, Optional

import ray
from ray.data._internal.block_batching.interfaces import Batch, BlockPrefetcher
from ray.data._internal.block_batching.util import (
    ActorBlockPrefetcher,
    WaitBlockPrefetcher,
    blocks_to_batches,
    collate,
    extract_data_from_batch,
    finalize_batches,
    format_batches,
    resolve_block_refs,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import make_async_gen
from ray.data.block import Block, DataBatch
from ray.data.context import DataContext
from ray.types import ObjectRef


def iter_batches(
    ref_bundles: Iterator[RefBundle],
    *,
    stats: Optional[DatasetStats] = None,
    clear_block_after_read: bool = False,
    batch_size: Optional[int] = None,
    batch_format: Optional[str] = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    finalize_fn: Optional[Callable[[Any], Any]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
    prefetch_batches: int = 1,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from an iterator of block object references and
    corresponding metadata.

    This takes a block iterator and creates batch_size batches, slicing,
    unioning, shuffling, prefetching, and formatting blocks as needed.

    The algorithm uses both pipeline parallelism and data parallelism:

    If prefetch_batches=2, these are all the batches in flight:

    [User thread] trains on Batch 0
    - [Fetch thread] Batch 1 finalization + move to output queue
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
            a. Format the batches to the provided batch format.
            b. Apply the collate function.
        5. Finalize each of the collated batches
        6. Fetch outputs from the threadpool, maintaining order of the batches.

    Args:
        ref_bundles: An iterator over RefBundles.
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
            ``pyarrow.Table``, or None to use entire blocks
            as batches. Default is "default".
        drop_last: Whether to drop the last batch if it's incomplete.
        collate_fn: A function to apply to each data batch before returning it.
        finalize_fn: A function to apply to each data batch after it has been collated.
            This function is not run in a threadpool so it can be used for
            memory-intensive operations such as GPU preloading.
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
            formatting to be overlapped with the UDF. Defaults to 1.

    Returns:
        An iterator over record batches.
    """
    context = DataContext.get_current()

    if (
        prefetch_batches > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()

    eager_free = clear_block_after_read and DataContext.get_current().eager_free

    def _async_iter_batches(
        ref_bundles: Iterator[RefBundle],
    ) -> Iterator[DataBatch]:
        # Step 1: Prefetch logical batches locally.
        block_iter = prefetch_batches_locally(
            ref_bundles=ref_bundles,
            prefetcher=prefetcher,
            num_batches_to_prefetch=prefetch_batches,
            batch_size=batch_size,
            eager_free=eager_free,
        )

        # Step 2: Resolve the blocks.
        block_iter = resolve_block_refs(block_ref_iter=block_iter, stats=stats)

        # Step 3: Batch and shuffle the resolved blocks.
        batch_iter = blocks_to_batches(
            block_iter=block_iter,
            stats=stats,
            batch_size=batch_size,
            drop_last=drop_last,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
            ensure_copy=ensure_copy,
        )

        # Step 4: Use a threadpool for formatting and collation.
        batch_iter = _format_in_threadpool(
            batch_iter,
            stats=stats,
            batch_format=batch_format,
            collate_fn=collate_fn,
            num_threadpool_workers=prefetch_batches,
        )

        # Step 5: Finalize each batch.
        if finalize_fn is not None:
            batch_iter = finalize_batches(
                batch_iter, finalize_fn=finalize_fn, stats=stats
            )

        # Step 6: Restore original order.
        batch_iter: Iterator[Batch] = restore_original_order(batch_iter)

        yield from extract_data_from_batch(batch_iter)

    # Run everything in a separate thread to not block the main thread when waiting
    # for streaming results.
    async_batch_iter = make_async_gen(
        ref_bundles, fn=_async_iter_batches, num_workers=1
    )

    while True:
        with stats.iter_total_blocked_s.timer() if stats else nullcontext():
            try:
                next_batch = next(async_batch_iter)
            except StopIteration:
                break
        with stats.iter_user_s.timer() if stats else nullcontext():
            yield next_batch


def _format_in_threadpool(
    batch_iter: Iterator[Batch],
    stats: DatasetStats,
    batch_format: Optional[str],
    collate_fn: Optional[Callable[[DataBatch], Any]],
    num_threadpool_workers: int,
) -> Iterator[Batch]:
    """Executes the batching, formatting, and collation logic in a threadpool.

    Args:
        logical_batch_iterator: An iterator over logical batches.
        stats: DatasetStats object to record timing and other statistics.
        batch_format: The format in which to return each batch.
            Specify "default" to use the current block format (promoting
            Arrow to pandas automatically), "pandas" to
            select ``pandas.DataFrame`` or "pyarrow" to select
            ``pyarrow.Table``, or None to use entire blocks
            as batches.
        collate_fn: A function to apply to each data batch before returning it.
        num_threadpool_workers: The number of threads to use in the threadpool.
    """

    def threadpool_computations_format_collate(
        batch_iter: Iterator[Batch],
    ) -> Iterator[Batch]:
        # Step 4a: Format the batches.
        formatted_batch_iter = format_batches(
            batch_iter, batch_format=batch_format, stats=stats
        )

        # Step 4b: Apply the collate function if applicable.
        if collate_fn is not None:
            formatted_batch_iter = collate(
                formatted_batch_iter, collate_fn=collate_fn, stats=stats
            )
        yield from formatted_batch_iter

    if num_threadpool_workers > 0:
        collated_iter = make_async_gen(
            base_iterator=batch_iter,
            fn=threadpool_computations_format_collate,
            num_workers=num_threadpool_workers,
        )
    else:
        collated_iter = threadpool_computations_format_collate(batch_iter)
    return collated_iter


def prefetch_batches_locally(
    ref_bundles: Iterator[RefBundle],
    prefetcher: BlockPrefetcher,
    num_batches_to_prefetch: int,
    batch_size: Optional[int],
    eager_free: bool = False,
) -> Iterator[ObjectRef[Block]]:
    """Given an iterator of batched RefBundles, returns an iterator over the
    corresponding block references while prefetching `num_batches_to_prefetch`
    batches in advance.

    Args:
        ref_bundles: An iterator over batched RefBundles.
        prefetcher: The prefetcher to use.
        num_batches_to_prefetch: The number of batches to prefetch ahead of the
            current batch during the scan.
        batch_size: User specified batch size, or None to let the system pick.
        eager_free: Whether to eagerly free the object reference from the object store.
    """

    sliding_window = collections.deque()
    current_window_size = 0

    if num_batches_to_prefetch <= 0:
        for ref_bundle in ref_bundles:
            for block_ref in ref_bundle.block_refs:
                yield block_ref
        return

    if batch_size is not None:
        num_rows_to_prefetch = num_batches_to_prefetch * batch_size
    else:
        num_rows_to_prefetch = None

    # Create and fetch the initial window.
    # Stop adding if the number of rows in this window is greater than requested
    # batch size, or if the batch size is None and the number of blocks in this window
    # is greater than requested batches to prefetch.
    while (batch_size is not None and current_window_size < num_rows_to_prefetch) or (
        batch_size is None and len(sliding_window) < num_batches_to_prefetch
    ):
        try:
            next_ref_bundle = next(ref_bundles)
            sliding_window.extend(next_ref_bundle.blocks)
            current_window_size += next_ref_bundle.num_rows()
        except StopIteration:
            break

    prefetcher.prefetch_blocks([block_ref for block_ref, _ in list(sliding_window)])

    while sliding_window:
        block_ref, metadata = sliding_window.popleft()
        current_window_size -= metadata.num_rows
        if batch_size is None or current_window_size < num_rows_to_prefetch:
            try:
                next_ref_bundle = next(ref_bundles)
                for block_ref_and_md in next_ref_bundle.blocks:
                    sliding_window.append(block_ref_and_md)
                    current_window_size += block_ref_and_md[1].num_rows
                prefetcher.prefetch_blocks(
                    [block_ref for block_ref, _ in list(sliding_window)]
                )
            except StopIteration:
                pass
        yield block_ref
        trace_deallocation(block_ref, loc="iter_batches", free=eager_free)
    prefetcher.stop()


def restore_original_order(batch_iter: Iterator[Batch]) -> Iterator[Batch]:
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
