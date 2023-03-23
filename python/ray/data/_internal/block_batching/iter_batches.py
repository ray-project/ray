import heapq
import random
import sys
from typing import Any, Callable, Iterator, List, Optional, Tuple

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockAccessor, DataBatch
from ray.data.context import DatasetContext
from ray.data._internal.block_batching.interfaces import (
    Batch,
    LogicalBatch,
    BlockPrefetcher,
)
from ray.data._internal.block_batching.util import (
    _calculate_ref_hits,
    _make_async_gen,
    ActorBlockPrefetcher,
    WaitBlockPrefetcher,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.stats import DatasetStats

if sys.version_info >= (3, 7):
    from contextlib import nullcontext
else:
    from contextlib import contextmanager

    @contextmanager
    def nullcontext(enter_result=None):
        yield enter_result


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

    This is used by both Dataset.iter_batches() and DatasetPipeline.iter_batches()

    The algorithm is as follows:

    In a single async thread, do the following:
    1. Construct logical batches. This creates groupings of the block object references
    based on the corresponding metadata.num_rows. The blocks are not resolved or sliced.
    2. If specified, locally shuffle the logical batches.
    3. Trigger local prefetching of the logical batches.
    4. Then, in a threadpool consisting of `prefetch_batches` threads:
        1. Resolve (i.e. call `ray.get()`) on the underlying block references for each
        logical batch.
        2. Perform the necessary batch slicing to construct full batches.
        3. Format the batches to the provided batch format.
        4. Apply the collate function
    5. Trace deallocation and eagerly clear block references if necessary.
    6. Fetch outputs from the threadpool, maintaining order of the batches.

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

    eager_free = clear_block_after_read and context.eager_free

    def _async_iter_batches(
        block_refs: Iterator[ObjectRef[Block]],
    ) -> Iterator[DataBatch]:
        # Step 1: Construct logical batches based on the metadata.
        batch_iter = _bundle_block_refs_to_logical_batches(
            block_refs, batch_size=batch_size, drop_last=drop_last
        )

        # Step 2: Shuffle the logical batches if applicable.
        if shuffle_buffer_min_size is not None:
            batch_iter = _local_shuffle_logical_batches(
                shuffle_buffer_min_size=shuffle_buffer_min_size,
                shuffle_seed=shuffle_seed,
            )

        # Step 3: Prefetch logical batches locally.
        if prefetch_batches > 0:
            batch_iter = _prefetch_batches_locally(
                batch_iter,
                prefetcher=prefetcher,
                num_batches_to_prefetch=prefetch_batches,
            )

        # Step 4: Use a threadpool for resolving blocks, slicing, formatting, and
        # collation.
        batch_iter = _batch_in_threadpool(
            batch_iter,
            stats=stats,
            batch_format=batch_format,
            collate_fn=collate_fn,
            ensure_copy=ensure_copy,
            num_threadpool_workers=prefetch_batches,
        )

        # Step 5: Trace deallocation
        batch_iter = _trace_deallocation(batch_iter, eager_free=eager_free)

        # Step 6: Restore original order.
        batch_iter: Iterator[Batch] = _restore_from_original_order(batch_iter)

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


def _bundle_block_refs_to_logical_batches(
    block_ref_iterator: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    batch_size: Optional[int],
    drop_last: bool = False,
) -> Iterator[LogicalBatch]:
    """Given an iterator of block object references, and their corresponding metadata,
    bundles the block object references into groups of the provided `batch_size`.

    The output iterator returns an iterator over LogicalBatch objects.

    This function does not do any slicing or creation of actual batch objects.
    """
    batch_buffer: List[ObjectRef[Block]] = []
    buffer_size = 0
    starting_index = 0

    global_index = 0
    num_rows_in_last_block = 0

    if batch_size is None:
        for block_ref, metadata in block_ref_iterator:
            yield LogicalBatch(global_index, [block_ref], 0, None, metadata.num_rows)
            global_index += 1
    else:
        while True:
            if buffer_size < batch_size:
                # Pull next block from iterator if current buffer is not enough to fill
                # a batch.
                try:
                    block_ref, metadata = next(block_ref_iterator)
                except StopIteration:
                    break
                batch_buffer.append(block_ref)
                buffer_size += metadata.num_rows
                num_rows_in_last_block = metadata.num_rows

            if buffer_size == batch_size:
                # If equal to batch size, then yield the full buffer.
                yield LogicalBatch(
                    global_index, batch_buffer, starting_index, None, buffer_size
                )
                batch_buffer = []
                buffer_size = 0
                starting_index = 0
                num_rows_in_last_block = 0
                global_index += 1

            if buffer_size > batch_size:
                # If current buffer is greater than batch size, then yield part of the
                # buffer, and carryover the remainder to the next batch.
                num_rows_to_leave_behind = buffer_size - batch_size
                ending_index = num_rows_in_last_block - num_rows_to_leave_behind
                assert ending_index > 0, ending_index
                yield LogicalBatch(
                    global_index, batch_buffer, starting_index, ending_index, batch_size
                )
                global_index += 1
                # Carryover to next batch.
                batch_buffer = [batch_buffer[-1]]
                buffer_size = num_rows_to_leave_behind
                starting_index = ending_index

    # Yield any leftover batches if necessary.
    if buffer_size > 0 and not drop_last:
        assert buffer_size < batch_size
        yield LogicalBatch(
            global_index, batch_buffer, starting_index, None, buffer_size
        )
        global_index += 1


def _local_shuffle_logical_batches(
    logical_batch_iterator: Iterator[LogicalBatch],
    shuffle_buffer_min_size: int,
    shuffle_seed: Optional[int] = None,
) -> Iterator[LogicalBatch]:
    """Shuffles the logical batch iterator using a buffer of the provided size."""

    if shuffle_seed is not None:
        random.seed(shuffle_seed)

    shuffle_buffer: List[LogicalBatch] = []
    shuffle_buffer_size = 0
    global_counter = 0

    for logical_batch in logical_batch_iterator:
        shuffle_buffer.append(logical_batch)
        shuffle_buffer_size += logical_batch.num_rows

        while shuffle_buffer_size >= shuffle_buffer_min_size:
            output_batch = shuffle_buffer.pop(
                random.randint(0, len(shuffle_buffer) - 1)
            )
            output_batch.batch_idx = global_counter
            yield output_batch
            shuffle_buffer_size -= output_batch.num_rows
            global_counter += 1

    # Yield any leftover.
    while len(shuffle_buffer) > 0:
        output_batch = shuffle_buffer.pop(random.randint(0, len(shuffle_buffer) - 1))
        output_batch.batch_idx = global_counter
        yield output_batch
        global_counter += 1


def _prefetch_batches_locally(
    logical_batch_iter: Iterator[LogicalBatch],
    prefetcher: BlockPrefetcher,
    num_batches_to_prefetch: int,
) -> Iterator[LogicalBatch]:
    """Given an iterator of logical batches, returns an iterator over the same logical
    batches, while prefetching `num_batches_to_prefetch` batches in advance.

    Args:
        logical_batch_iter: An iterator over logical batches.
        prefetcher: The prefetcher to use.
        num_batches_to_prefetch: The number of batches to prefetch ahead of the
            current batch during the scan.
    """

    def get_next_batches() -> Iterator[List[LogicalBatch]]:
        """Return lists of logical batches corresponding to `num_batches_to_prefetch`"""
        next_batches = []
        while True:
            try:
                next_batches.append(next(logical_batch_iter))
                if len(next_batches) == num_batches_to_prefetch:
                    yield next_batches
                    next_batches = []
            except StopIteration:
                break

        if len(next_batches) > 0:
            yield next_batches

    # Fetch the initial set of batches.
    batch_iterator = get_next_batches()
    try:
        batches = next(batch_iterator)
    except StopIteration:
        return

    block_refs = [block_ref for batch in batches for block_ref in batch.block_refs]
    prefetcher.prefetch_blocks(block_refs)

    for next_batches in batch_iterator:
        # Prefetch the next batches.
        block_refs = [
            block_ref for batch in next_batches for block_ref in batch.block_refs
        ]
        prefetcher.prefetch_blocks(block_refs)

        for batch in batches:
            yield batch

        batches = next_batches

    # Yield the final set of batches.
    for batch in batches:
        yield batch


def _resolve_logical_batch(
    logical_batch_iter: Iterator[LogicalBatch], stats: Optional[DatasetStats] = None
):
    """Resolves the block references for each logical batch."""

    hits = 0
    misses = 0
    unknowns = 0

    for logical_batch in logical_batch_iter:
        current_hit, current_miss, current_unknown = _calculate_ref_hits(
            logical_batch.block_refs
        )
        hits += current_hit
        misses += current_miss
        unknowns += current_unknown

        with stats.iter_get_s.timer() if stats else nullcontext():
            logical_batch.resolve()
        yield logical_batch

    if stats:
        stats.iter_blocks_local += hits
        stats.iter_blocks_remote += misses
        stats.iter_unknown_location += unknowns


def _construct_batch_from_logical_batch(
    resolved_logical_batch_iter: Iterator[LogicalBatch],
    ensure_copy: bool = False,
    stats: Optional[DatasetStats] = None,
) -> Iterator[Tuple[int, Block]]:
    """Given an iterator over logical batches, returns an iterator over actual
    constructed batches.

    Args:
        resolved_logical_batch_iter: An iterator over resolved logical batches.
        stats: Dataset stats object used to store block batching time.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).
        stats: An optional stats object to record formatting times.

    Returns:
        An iterator over batch index and batches of the given size.
    """

    for logical_batch in resolved_logical_batch_iter:
        with stats.iter_create_batch_s.timer() if stats else nullcontext():
            output = DelegatingBlockBuilder()
            slice_indices = [[0, None] for _ in range(len(logical_batch.blocks))]
            if logical_batch.starting_block_idx > 0:
                slice_indices[0][0] = logical_batch.starting_block_idx
            if logical_batch.ending_block_idx is not None:
                slice_indices[-1][1] = logical_batch.ending_block_idx

            for i, block in enumerate(logical_batch.blocks):
                accessor = BlockAccessor.for_block(block)
                slice_index = slice_indices[i]
                output.add_block(
                    accessor.slice(
                        slice_index[0],
                        slice_index[1]
                        if slice_index[1] is not None
                        else accessor.num_rows(),
                        copy=False,
                    )
                )

            batch = output.build()
            assert len(batch) == logical_batch.num_rows, (
                len(batch),
                logical_batch.num_rows,
            )
            if ensure_copy:
                # Need to ensure that the batch is a fresh copy.
                batch = BlockAccessor.for_block(batch)
                batch = batch.slice(0, batch.num_rows(), copy=True)

        yield Batch(logical_batch.batch_idx, batch, logical_batch)


def _format_batches(
    block_iter: Iterator[Batch],
    batch_format: str,
    stats: Optional[DatasetStats] = None,
) -> Iterator[Batch]:
    """Given an iterator of blocks, returns an iterator of formatted batches.

    Args:
        block_iter: An iterator over blocks.
        batch_format: The batch format to use.
        stats: An optional stats object to record formatting times.

    Returns:
        An iterator over batch index and the formatted batch.
    """
    for batch in block_iter:
        with stats.iter_format_batch_s.timer() if stats else nullcontext():
            formatted_batch = BlockAccessor.for_block(batch.data).to_batch_format(
                batch_format
            )
        batch.data = formatted_batch
        yield batch


def _collate(
    batch_iter: Iterator[Batch],
    collate_fn: Optional[Callable[[DataBatch], Any]],
    stats: Optional[DatasetStats] = None,
) -> Iterator[Tuple[int, Any]]:
    """Returns an iterator with the provided collate_fn applied to items of the batch
    iterator.

    Args:
        batch_iter: An iterator over formatted batches.
        collate_fn: The collate_fn to execute.
        stats: An optional stats object to record collation time.
    """
    for batch in batch_iter:
        with stats.iter_collate_batch_s.timer() if stats else nullcontext():
            batch.data = collate_fn(batch.data)
        yield batch


def _trace_deallocation(
    batch_iter: Iterator[Batch], eager_free: bool
) -> Iterator[Batch]:
    """Trace deallocation of the underlying block references for each batch.

    Args:
        batch_iter: An iterator over batches.
        eager_free: Whether to eagerly free the object reference from the object store.
    """
    for batch in batch_iter:
        block_refs = batch.logical_batch.block_refs
        for block_ref in block_refs:
            trace_deallocation(block_ref, loc="iter_batches", free=eager_free)
        yield batch


def _restore_from_original_order(batch_iter: Iterator[Batch]) -> Iterator[Batch]:
    """Restores the original order of the provided `batch_iter`

    This function will yield items from `base_iterator` in the correct order based on
    each batch's batch_idx. All indexes are expected to be unique.

    `batch_iter` is expected to not have any missing indexes. All indexes from 0 to len
    (base_iterator) must be present.
    """
    next_index_required = 0
    buffer: List[Batch] = []
    for batch in batch_iter:
        heapq.heappush(buffer, (batch.batch_idx, batch))
        if buffer[0][0] == next_index_required:
            yield heapq.heappop(buffer)[1]
            next_index_required += 1

    while len(buffer) > 0:
        yield heapq.heappop(buffer)[1]
