import collections
import itertools
from typing import Iterator, Iterable, Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    import pandas

import numpy as np

import ray
from ray.actor import ActorHandle
from ray.types import ObjectRef
from ray.data.block import Block, BlockAccessor
from ray.data.context import DatasetContext
from ray.data.impl.batcher import Batcher
from ray.data.impl.stats import DatasetStats, DatasetPipelineStats
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# An output type of iter_batches() determined by the batch_format parameter.
BatchType = Union["pandas.DataFrame", "pyarrow.Table", np.ndarray, list]
PREFETCHER_ACTOR_NAMESPACE = "ray.dataset"


def batch_blocks(
    blocks: Iterator[Block],
    stats: Union[DatasetStats, DatasetPipelineStats],
    *,
    prefetch_blocks: int = 0,
    batch_size: Optional[int] = None,
    batch_format: str = "native",
    drop_last: bool = False,
) -> Iterator[BatchType]:
    """Create batches of data from 1 or more blocks.

    This takes a block iterator and creates batch_size batches, slicing and unioning
    blocks as needed.

    This is used by both Dataset.iter_batches() and DatasetPipeline.iter_batches().

    Args:
        prefetch_blocks: The number of blocks to prefetch ahead of the
            current block during the scan.
        batch_size: Record batch size, or None to let the system pick.
        batch_format: The format in which to return each batch.
            Specify "native" to use the current block format (promoting
            Arrow to pandas automatically), "pandas" to
            select ``pandas.DataFrame`` or "pyarrow" to select
            ``pyarrow.Table``. Default is "native".
        drop_last: Whether to drop the last batch if it's incomplete.

    Returns:
        An iterator over record batches.
    """
    batcher = Batcher(batch_size=batch_size)

    def batch_block(block: ObjectRef[Block]):
        with stats.iter_get_s.timer():
            block = ray.get(block)
        batcher.add(block)
        while batcher.has_batch():
            with stats.iter_format_batch_s.timer():
                result = _format_batch(batcher.next_batch(), batch_format)
            with stats.iter_user_s.timer():
                yield result

    block_window = []  # Handle empty sliding window gracefully.
    context = DatasetContext.get_current()
    if (
        prefetch_blocks > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()
    for block_window in _sliding_window(blocks, prefetch_blocks + 1):
        block_window = list(block_window)
        with stats.iter_wait_s.timer():
            prefetcher.prefetch_blocks(block_window)
        yield from batch_block(block_window[0])

    # Consume remainder of final block window.
    for block in block_window[1:]:
        yield from batch_block(block)

    # Yield any remainder batches.
    if batcher.has_any() and not drop_last:
        with stats.iter_format_batch_s.timer():
            result = _format_batch(batcher.next_batch(), batch_format)
        with stats.iter_user_s.timer():
            yield result


def _format_batch(batch: Block, batch_format: str) -> BatchType:
    import pyarrow as pa

    if batch_format == "native":
        # Always promote Arrow blocks to pandas for consistency, since
        # we lazily convert pandas->Arrow internally for efficiency.
        if isinstance(batch, pa.Table) or isinstance(batch, bytes):
            batch = BlockAccessor.for_block(batch)
            batch = batch.to_pandas()
        return batch
    elif batch_format == "pandas":
        batch = BlockAccessor.for_block(batch)
        return batch.to_pandas()
    elif batch_format == "pyarrow":
        batch = BlockAccessor.for_block(batch)
        return batch.to_arrow()
    else:
        raise ValueError(
            f"The given batch format: {batch_format} "
            f"is invalid. Supported batch type: {BatchType}"
        )


def _sliding_window(iterable: Iterable, n: int):
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


@ray.remote(num_cpus=0, placement_group=None)
class _BlockPretcher:
    """Helper actor that prefetches blocks asynchronously."""

    def prefetch(self, *blocks) -> None:
        pass
