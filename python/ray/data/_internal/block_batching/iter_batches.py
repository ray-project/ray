import collections
from typing import Dict, Iterator, List, Optional, Tuple

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BlockPrefetcher,
)


def bundle_block_refs_to_logical_batches(
    block_ref_iterator: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    batch_size: Optional[int],
    drop_last: bool = False,
) -> Iterator[List[ObjectRef[Block]]]:
    """Given an iterator of block object references, and their corresponding metadata,
    bundles the block object references into groups of at least `batch_size`.



    This function does not do any slicing or creation of actual batch objects.
    """
    batch_buffer: List[ObjectRef[Block]] = []
    buffer_size = 0
    original_batch_size = batch_size

    if batch_size is None:
        for block_ref, _ in block_ref_iterator:
            yield [block_ref]
    else:
        while True:
            if buffer_size < batch_size or buffer_size <= 0:
                # Pull next block from iterator if current buffer is not enough to fill
                # a batch.
                try:
                    block_ref, metadata = next(block_ref_iterator)
                except StopIteration:
                    break
                batch_buffer.append(block_ref)
                buffer_size += metadata.num_rows

            else:
                # If equal to or greater than batch size, then yield the full buffer.
                yield batch_buffer
                carryover_to_next_batch = buffer_size - batch_size
                batch_buffer = []
                buffer_size = 0
                assert carryover_to_next_batch >= 0
                if carryover_to_next_batch == 0:
                    # Reset the
                    batch_size = original_batch_size
                elif carryover_to_next_batch > 0:
                    # Carryover remainder to next batch so we don't prefetch too much.
                    # Example: 4 blocks with 2 rows each. Batch size of 3.
                    # Batch 1: Yield 2 blocks (4 total rows)
                    # Batch 2: Only yield 1 additional block since 1 row from the
                    # previous yield should be included in this batch.
                    batch_size = original_batch_size - carryover_to_next_batch

        # Yield any leftover batches if necessary.
        assert buffer_size < original_batch_size
        if buffer_size > 0 and not drop_last:
            yield batch_buffer


def prefetch_batches_locally(
    block_ref_iter: Iterator[List[ObjectRef[Block]]],
    prefetcher: BlockPrefetcher,
    num_batches_to_prefetch: int,
) -> Iterator[List[ObjectRef[Block]]]:
    """Given an iterator of batched block references, returns an iterator over the same
    block references while prefetching `num_batches_to_prefetch` batches in advance.

    Args:
        block_ref_iter: An iterator over batched block references.
        prefetcher: The prefetcher to use.
        num_batches_to_prefetch: The number of batches to prefetch ahead of the
            current batch during the scan.
    """

    sliding_window = collections.deque(maxlen=num_batches_to_prefetch)
    # Create and fetch the initial window.
    for _ in range(num_batches_to_prefetch):
        try:
            sliding_window.append(next(block_ref_iter))
        except StopIteration:
            break
    prefetcher.prefetch_blocks(
        [block_ref for batch in list(sliding_window) for block_ref in batch]
    )

    while sliding_window:
        batch = sliding_window.popleft()
        try:
            sliding_window.append(next(block_ref_iter))
            prefetcher.prefetch_blocks(
                [block_ref for batch in list(sliding_window) for block_ref in batch]
            )
        except StopIteration:
            pass
        yield batch


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
