import heapq
import random
from typing import Any, Callable, Iterator, List, Optional, Tuple

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockAccessor, DataBatch
from ray.data._internal.block_batching.interfaces import (
    Batch,
    LogicalBatch,
    BlockPrefetcher,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.memory_tracing import trace_deallocation


def bundle_block_refs_to_logical_batches(
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


def local_shuffle_logical_batches(
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


def prefetch_batches_locally(
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


def resolve_logical_batch(
    logical_batch_iter: Iterator[LogicalBatch],
) -> Iterator[LogicalBatch]:
    """Resolves the block references for each logical batch."""
    for logical_batch in logical_batch_iter:
        logical_batch.resolve()
        yield logical_batch


def construct_batch_from_logical_batch(
    resolved_logical_batch_iter: Iterator[LogicalBatch],
    ensure_copy: bool = False,
) -> Iterator[Batch]:
    """Given an iterator over logical batches, returns an iterator over actual
    constructed batches.

    Args:
        resolved_logical_batch_iter: An iterator over resolved logical batches.
        stats: Dataset stats object used to store block batching time.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).

    Returns:
        An iterator over batch index and batches of the given size.
    """

    for logical_batch in resolved_logical_batch_iter:
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


def format_batches(
    block_iter: Iterator[Batch],
    batch_format: Optional[str],
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
        formatted_batch = BlockAccessor.for_block(batch.data).to_batch_format(
            batch_format
        )
        batch.data = formatted_batch
        yield batch


def collate(
    batch_iter: Iterator[Batch],
    collate_fn: Optional[Callable[[DataBatch], Any]],
) -> Iterator[Batch]:
    """Returns an iterator with the provided collate_fn applied to items of the batch
    iterator.

    Args:
        batch_iter: An iterator over formatted batches.
        stats: An optional stats object to record collation time.
    """
    for batch in batch_iter:
        batch.data = collate_fn(batch.data)
        yield batch


def trace_deallocation_for_batch(
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


def restore_from_original_order(batch_iter: Iterator[Batch]) -> Iterator[Batch]:
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
