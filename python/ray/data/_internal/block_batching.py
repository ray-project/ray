import collections
import itertools
from typing import Iterable, Iterator, Optional, Union

import ray
from ray.actor import ActorHandle
from ray.data._internal.batcher import (
    Batcher,
    BatchType,
    ShufflingBatcher,
    AsyncBatcher,
)
from ray.data._internal.stats import DatasetPipelineStats, DatasetStats
from ray.data.block import Block
from ray.data.context import DatasetContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

PREFETCHER_ACTOR_NAMESPACE = "ray.dataset"


def batch_blocks(
    blocks: Iterator[ObjectRef[Block]],
    stats: Union[DatasetStats, DatasetPipelineStats],
    *,
    prefetch_blocks: int = 0,
    clear_block_after_read: bool = False,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    prefetch_batches: int = 0,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
) -> Iterator[BatchType]:
    """Create batches of data from 1 or more blocks.

    This takes a block iterator and creates batch_size batches, slicing and unioning
    blocks as needed.

    This is used by both Dataset.iter_batches() and DatasetPipeline.iter_batches().

    Args:
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
        prefetch_batches: The number of batches to fetch ahead of the current batch to
            process. If set to greater than 0, a separate thread will be used to fetch
            the specified amount of formatted batches from blocks. This improves
            performance for non-CPU bound UDFs, allowing batch fetching compute and
            formatting to be overlapped with the UDF. Defaults to 0 (no prefetching
            enabled).
        shuffle_buffer_min_size: If non-None, the data will be randomly shuffled using a
            local in-memory shuffle buffer, and this value will serve as the minimum
            number of rows that must be in the local in-memory shuffle buffer in order
            to yield a batch.
        shuffle_seed: The seed to use for the local random shuffle.

    Returns:
        An iterator over record batches.
    """
    if shuffle_buffer_min_size is not None:
        batcher = ShufflingBatcher(
            batch_size=batch_size,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
        )
    else:
        batcher = Batcher(batch_size=batch_size, batch_format=batch_format)

    if prefetch_batches > 0:
        batcher = AsyncBatcher(base_batcher=batcher, buffer_max_size=prefetch_batches)

    context = DatasetContext.get_current()

    def get_batches(block: Optional[ObjectRef[Block]] = None) -> Iterator[BatchType]:
        if block is not None:
            with stats.iter_get_s.timer():
                block = ray.get(block)
            # NOTE: Since we add one block at a time and then immediately consume
            # batches, we don't need to check batcher.can_add() before adding the block;
            # it will always be True, and batcher.add() will assert this internally.
            batcher.add(block)
        else:
            batcher.done_adding()
        while batcher.has_batch():
            # While the batcher has full batches, yield batches.
            with stats.iter_next_batch_s.timer():
                batch = batcher.next_batch()
            with stats.iter_user_s.timer():
                yield batch
        # Handle remainder batches.
        if block is None and not drop_last and batcher.has_any():
            with stats.iter_next_batch_s.timer():
                batch = batcher.next_batch()
            with stats.iter_user_s.timer():
                yield batch

    block_window = []  # Handle empty sliding window gracefully.

    if (
        prefetch_blocks > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()

    # Batch blocks over the prefetch windows.
    for block_window in _sliding_window(
        blocks, prefetch_blocks + 1, clear_block_after_read
    ):
        block_window = list(block_window)
        with stats.iter_wait_s.timer():
            prefetcher.prefetch_blocks(block_window)
        yield from get_batches(block_window[0])

    # Consume remainder of final block window.
    for block in block_window[1:]:
        yield from get_batches(block)

    # Consume any remaining batches, now that we're done adding blocks to the batcher.
    yield from get_batches()


def _sliding_window(iterable: Iterable, n: int, clear_block_after_read: bool = False):
    """Creates an iterator consisting of n-width sliding windows over
    iterable. The sliding windows are constructed lazily such that an
    element on the base iterator (iterable) isn't consumed until the
    first sliding window containing that element is reached.

    If n > len(iterable), then a single len(iterable) window is
    returned.

    Args:
        iterable: The iterable on which the sliding window will be
            created.
        n: The width of the sliding window.
        clear_block_after_read: Whether to clear the leftmost block
            from object store manually (i.e. without waiting for Python's
            automatic GC) when it's out of the sliding window (i.e. been
            consumed), so as to reclaim memory faster. The caller has to
            ensure safety, i.e. the block will never be accessed again.

    Returns:
        An iterator of n-width windows over iterable.
        If n > len(iterable), then a single len(iterable) window is
        returned.
    """
    it = iter(iterable)
    window = collections.deque(itertools.islice(it, n), maxlen=n)
    if len(window) > 0:
        yield tuple(window)
    for elem in it:
        block_ref = window.popleft()
        if clear_block_after_read:
            ray._private.internal_api.free(block_ref, local_only=False)
        window.append(elem)
        yield tuple(window)


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
