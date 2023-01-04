from contextlib import nullcontext
import queue
from typing import TYPE_CHECKING, Iterator, Optional, Union, Dict

import numpy as np

import ray
from ray.actor import ActorHandle
from ray.data._internal.batcher import Batcher, ShufflingBatcher
from ray.data._internal.stats import DatasetPipelineStats, DatasetStats
from ray.data.block import Block, BlockAccessor
from ray.data.context import DatasetContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import pandas
    import pyarrow


# An output type of iter_batches() determined by the batch_format parameter.
BatchType = Union[
    "pandas.DataFrame",
    "pyarrow.Table",
    np.ndarray,
    Dict[str, np.ndarray],
    list,
]
PREFETCHER_ACTOR_NAMESPACE = "ray.dataset"


def batch_blocks(
    blocks: Iterator[ObjectRef[Block]],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    *,
    prefetch_blocks: int = 0,
    clear_block_after_read: bool = False,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
) -> Iterator[BatchType]:
    """Create formatted batches of data from 1 or more blocks.

    This takes a block iterator and creates batch_size batches, slicing,
    unioning, shuffling, prefetching, and formatting blocks as needed.

    This is used by both Dataset.iter_batches()/DatasetPipeline.iter_batches()
    and Dataset.map_batches()/DatasetPipeline.map_batches().

    Args:
        blocks: An iterator over block object references.
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
        shuffle_buffer_min_size: If non-None, the data will be randomly shuffled using a
            local in-memory shuffle buffer, and this value will serve as the minimum
            number of rows that must be in the local in-memory shuffle buffer in order
            to yield a batch.
        shuffle_seed: The seed to use for the local random shuffle.

    Returns:
        An iterator over record batches.
    """

    block_iter = _resolve_blocks(
        _prefetch_blocks(
            block_ref_iter=blocks,
            stats=stats,
            num_blocks_to_prefetch=prefetch_blocks,
            clear_block_after_read=clear_block_after_read,
        ),
        stats=stats,
    )

    batch_iter = _format_batches(
        _blocks_to_batches(
            block_iter=block_iter,
            stats=stats,
            batch_size=batch_size,
            drop_last=drop_last,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
        ),
        batch_format=batch_format,
    )

    for formatted_batch in batch_iter:
        user_timer = stats.iter_user_s.timer() if stats else nullcontext()
        with user_timer:
            yield formatted_batch


def _resolve_blocks(
    block_ref_iter: Iterator[ObjectRef[Block]],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[Block]:
    """Given an iterator of unresolved blocks (as Ray object references), returns an
    iterator of resolved blocks.

    The length of the returned iterator may be less than the length of the original
    if any of the references in the original iterator are None.

    Args:
        block_ref_iter: An iterator over block object references.
        stats: Dataset stats object used to store block fetching time.

    Returns:
        An iterator over resolved blocks.
    """

    for block_ref in block_ref_iter:
        if block_ref is not None:
            stats_timer = stats.iter_get_s.timer() if stats else nullcontext()
            with stats_timer:
                block = ray.get(block_ref)
            yield block


def _prefetch_blocks(
    block_ref_iter: Iterator[ObjectRef[Block]],
    num_blocks_to_prefetch: int,
    clear_block_after_read: bool = False,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[ObjectRef[Block]]:
    """Given an iterable of Block Object References, returns an iterator
    over these object reference while prefetching `num_block_to_prefetch`
    blocks in advance.

    Args:
        block_ref_iter: An iterator over block object references.
        num_blocks_to_prefetch: The number of blocks to prefetch ahead of the
            current block during the scan.
        clear_block_after_read: Whether to clear the block from object store
            manually (i.e. without waiting for Python's automatic GC) after it
            is read. Doing so will reclaim memory faster and hence reduce the
            memory footprint. However, the caller has to ensure the safety, i.e.
            the block will never be accessed again.
        stats: Dataset stats object used to store block wait time.
    """

    if num_blocks_to_prefetch == 0:
        yield from block_ref_iter

    context = DatasetContext.get_current()

    if (
        num_blocks_to_prefetch > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()

    window_size = num_blocks_to_prefetch
    sliding_window = queue.Queue(maxsize=window_size)

    # Create the initial set of blocks to prefetch.
    while not sliding_window.full():
        try:
            sliding_window.put(next(block_ref_iter))
        except StopIteration:
            break
    prefetcher.prefetch_blocks(list(sliding_window.queue))

    while not sliding_window.empty():
        block_ref = sliding_window.get()
        try:
            sliding_window.put(next(block_ref_iter))
            prefetcher.prefetch_blocks(list(sliding_window.queue))
        except StopIteration:
            pass
        yield block_ref
        if clear_block_after_read:
            ray._private.internal_api.free(block_ref, local_only=False)


def _blocks_to_batches(
    block_iter: Iterator[Block],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    batch_size: Optional[int] = None,
    drop_last: bool = False,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
) -> Iterator[Block]:
    """Given an iterator over blocks, returns an iterator over blocks
    of the appropriate bacth size.

    If the shuffling configurations are specified, then the
    output blocks contain shuffled data.

    Args:
        block_iter: An iterator over blocks.
        stats: Dataset stats object used to store block batching time.
        batch_size: Record batch size, or None to let the system pick.
        drop_last: Whether to drop the last batch if it's incomplete.

    Returns:
        An iterator over blocks of the given size that are potentially shuffled.
    """
    if shuffle_buffer_min_size is not None:
        batcher = ShufflingBatcher(
            batch_size=batch_size,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
        )
    else:
        batcher = Batcher(batch_size=batch_size)

    for block in block_iter:
        batcher.add(block)
        while batcher.has_batch():
            timer = stats.iter_next_batch_s.timer() if stats else nullcontext()
            with timer:
                batch = batcher.next_batch()
            yield batch

    # Signal to the batcher that there are no more blocks to add.
    batcher.done_adding()

    # Get any remaining data.
    if batcher.has_any() and not drop_last:
        timer = stats.iter_next_batch_s.timer() if stats else nullcontext()
        with timer:
            batch = batcher.next_batch()
        yield batch


def _format_batches(
    block_iter: Iterator[Block], batch_format: str
) -> Iterator[BatchType]:
    """Given an iterator of blocks, returns an iterator of formatted batches.

    Args:
        block_iter: An iterator over blocks.
        batch_format: The batch format to use.

    Returns:
        An iterator over formatted batches.
    """
    for block in block_iter:
        batch = BlockAccessor.for_block(block).to_batch_format(batch_format)
        yield batch


class BlockPrefetcher:
    """Interface for prefetching blocks."""

    def prefetch_blocks(self, blocks: ObjectRef[Block]):
        """Prefetch the provided blocks to this node."""
        raise NotImplementedError


class WaitBlockPrefetcher(BlockPrefetcher):
    """Block prefetcher using ray.wait."""

    def prefetch_blocks(self, blocks: ObjectRef[Block]):
        ray.wait(blocks, num_returns=1, fetch_local=True)


# ray.wait doesn't work as expected, so we have an
# actor-based prefetcher as a work around. See
# https://github.com/ray-project/ray/issues/23983 for details.
class ActorBlockPrefetcher(BlockPrefetcher):
    """Block prefetcher using a local actor."""

    def __init__(self):
        self.prefetch_actor = self._get_or_create_actor_prefetcher()

    @staticmethod
    def _get_or_create_actor_prefetcher() -> "ActorHandle":
        node_id = ray.get_runtime_context().node_id
        actor_name = f"dataset-block-prefetcher-{node_id}"
        return _BlockPretcher.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
            name=actor_name,
            namespace=PREFETCHER_ACTOR_NAMESPACE,
            get_if_exists=True,
        ).remote()

    def prefetch_blocks(self, blocks: ObjectRef[Block]):
        self.prefetch_actor.prefetch.remote(*blocks)


@ray.remote(num_cpus=0)
class _BlockPretcher:
    """Helper actor that prefetches blocks asynchronously."""

    def prefetch(self, *blocks) -> None:
        pass
