import collections
from typing import Dict, Iterator, Optional, Tuple

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BlockPrefetcher,
)

"""
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
        a. Format the batches to the provided batch format.
        b. Apply the collate function.
    5. Fetch outputs from the threadpool, maintaining order of the batches.
"""


def prefetch_batches_locally(
    block_ref_iter: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    prefetcher: BlockPrefetcher,
    num_batches_to_prefetch: int,
    batch_size: Optional[int],
) -> Iterator[ObjectRef[Block]]:
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
            next_block_ref_and_metadata = next(block_ref_iter)
        except StopIteration:
            break
        sliding_window.append(next_block_ref_and_metadata)
        current_window_size += next_block_ref_and_metadata[1].num_rows

    prefetcher.prefetch_blocks([block_ref for block_ref, _ in list(sliding_window)])

    while sliding_window:
        block_ref, metadata = sliding_window.popleft()
        current_window_size -= metadata.num_rows
        if batch_size is None or current_window_size < num_rows_to_prefetch:
            try:
                sliding_window.append(next(block_ref_iter))
                prefetcher.prefetch_blocks(
                    [block_ref for block_ref, _ in list(sliding_window)]
                )
            except StopIteration:
                pass
        yield block_ref


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
