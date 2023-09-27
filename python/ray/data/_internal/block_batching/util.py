import logging
import threading
from contextlib import nullcontext
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.data._internal.batcher import Batcher, ShufflingBatcher
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BlockPrefetcher,
    CollatedBatch,
)
from ray.data._internal.stats import DatasetPipelineStats, DatasetStats
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


def _calculate_ref_hits(refs: List[ObjectRef[Any]]) -> Tuple[int, int, int]:
    """Given a list of object references, returns how many are already on the local
    node, how many require fetching from another node, and how many have unknown
    locations."""
    current_node_id = ray.get_runtime_context().get_node_id()

    locs = ray.experimental.get_object_locations(refs)
    nodes: List[List[str]] = [loc["node_ids"] for loc in locs.values()]
    hits = sum(current_node_id in node_ids for node_ids in nodes)
    unknowns = sum(1 for node_ids in nodes if not node_ids)
    misses = len(nodes) - hits - unknowns
    return hits, misses, unknowns


def resolve_block_refs(
    block_ref_iter: Iterator[ObjectRef[Block]],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[Block]:
    """Resolves the block references for each logical batch.

    Args:
        block_ref_iter: An iterator over block object references.
        stats: An optional stats object to recording block hits and misses.
    """
    hits = 0
    misses = 0
    unknowns = 0

    for block_ref in block_ref_iter:
        current_hit, current_miss, current_unknown = _calculate_ref_hits([block_ref])
        hits += current_hit
        misses += current_miss
        unknowns += current_unknown

        # TODO(amogkam): Optimized further by batching multiple references in a single
        # `ray.get()` call.
        with stats.iter_get_s.timer() if stats else nullcontext():
            block = ray.get(block_ref)
        yield block

    if stats:
        stats.iter_blocks_local = hits
        stats.iter_blocks_remote = misses
        stats.iter_unknown_location = unknowns


def blocks_to_batches(
    block_iter: Iterator[Block],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    batch_size: Optional[int] = None,
    drop_last: bool = False,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
) -> Iterator[Batch]:
    """Given an iterator over blocks, returns an iterator over blocks
    of the appropriate bacth size.

    If the shuffling configurations are specified, then the
    output blocks contain shuffled data.

    Args:
        block_iter: An iterator over blocks.
        stats: Dataset stats object used to store block batching time.
        batch_size: Record batch size, or None to let the system pick.
        drop_last: Whether to drop the last batch if it's incomplete.
        shuffle_buffer_min_size: If non-None, the data will be randomly shuffled
            using a local in-memory shuffle buffer, and this value will serve as the
            minimum number of rows that must be in the local in-memory shuffle buffer in
            order to yield a batch.
        shuffle_seed: The seed to use for the local random shuffle.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).

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
        batcher = Batcher(batch_size=batch_size, ensure_copy=ensure_copy)

    def get_iter_next_batch_s_timer():
        return stats.iter_next_batch_s.timer() if stats else nullcontext()

    global_counter = 0

    for block in block_iter:
        batcher.add(block)
        while batcher.has_batch():
            with get_iter_next_batch_s_timer():
                batch = batcher.next_batch()
            yield Batch(global_counter, batch)
            global_counter += 1

    # Signal to the batcher that there are no more blocks to add.
    batcher.done_adding()

    # Get any leftover batches in ShufflingBatcher.
    while batcher.has_batch():
        with get_iter_next_batch_s_timer():
            batch = batcher.next_batch()
        yield Batch(global_counter, batch)
        global_counter += 1

    # Get any remaining data.
    if not drop_last and batcher.has_any():
        with get_iter_next_batch_s_timer():
            batch = batcher.next_batch()
        yield Batch(global_counter, batch)
        global_counter += 1


def format_batches(
    block_iter: Iterator[Batch],
    batch_format: Optional[str],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
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
        yield Batch(batch.batch_idx, formatted_batch)


def collate(
    batch_iter: Iterator[Batch],
    collate_fn: Optional[Callable[[DataBatch], Any]],
    stats: Optional[DatasetStats] = None,
) -> Iterator[CollatedBatch]:
    """Returns an iterator with the provided collate_fn applied to items of the batch
    iterator.

    Args:
        batch_iter: An iterator over formatted batches.
        collate_fn: A function to apply to each batch.
        stats: An optional stats object to record formatting times.
    """
    for batch in batch_iter:
        with stats.iter_collate_batch_s.timer() if stats else nullcontext():
            collated_batch = collate_fn(batch.data)
        yield CollatedBatch(batch.batch_idx, collated_batch)


def finalize_batches(
    batch_iter: Iterator[CollatedBatch],
    finalize_fn: Callable[[Any], Any],
    stats: Optional[DatasetStats] = None,
) -> Iterator[CollatedBatch]:
    """Returns an iterator with the provided finalize_fn applied to items of the batch
    iterator.

    This is the same as `collate` except the input batches can be of type Any.

    Args:
        batch_iter: An iterator over processed batches.
        finalize_fn: A function to apply to each batch.
        stats: An optional stats object to record formatting times.

    Returns:
        An iterator over batch index and the finalized batch.
    """
    for batch in batch_iter:
        with stats.iter_finalize_batch_s.timer() if stats else nullcontext():
            finalized_batch = finalize_fn(batch.data)
        yield CollatedBatch(batch.batch_idx, finalized_batch)


def extract_data_from_batch(batch_iter: Iterator[Batch]) -> Iterator[Any]:
    for batch in batch_iter:
        yield batch.data


PREFETCHER_ACTOR_NAMESPACE = "ray.dataset"


class WaitBlockPrefetcher(BlockPrefetcher):
    """Block prefetcher using ray.wait."""

    def __init__(self):
        self._blocks = []
        self._stopped = False
        self._condition = threading.Condition()
        self._thread = threading.Thread(
            target=self._run,
            name="Prefetcher",
            daemon=True,
        )
        self._thread.start()

    def _run(self):
        while True:
            try:
                blocks_to_wait = []
                with self._condition:
                    if len(self._blocks) > 0:
                        blocks_to_wait, self._blocks = self._blocks[:], []
                    else:
                        if self._stopped:
                            return
                        blocks_to_wait = []
                        self._condition.wait()
                if len(blocks_to_wait) > 0:
                    ray.wait(blocks_to_wait, num_returns=1, fetch_local=True)
            except Exception:
                logger.exception("Error in prefetcher thread.")

    def prefetch_blocks(self, blocks: List[ObjectRef[Block]]):
        with self._condition:
            if self._stopped:
                raise RuntimeError("Prefetcher is stopped.")
            self._blocks = blocks
            self._condition.notify()

    def stop(self):
        with self._condition:
            if self._stopped:
                return
            self._stopped = True
            self._condition.notify()

    def __del__(self):
        self.stop()


class ActorBlockPrefetcher(BlockPrefetcher):
    """Block prefetcher using a local actor."""

    def __init__(self):
        self.prefetch_actor = self._get_or_create_actor_prefetcher()

    @staticmethod
    def _get_or_create_actor_prefetcher() -> "ActorHandle":
        node_id = ray.get_runtime_context().get_node_id()
        actor_name = f"dataset-block-prefetcher-{node_id}"
        return _BlockPretcher.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
            name=actor_name,
            namespace=PREFETCHER_ACTOR_NAMESPACE,
            get_if_exists=True,
        ).remote()

    def prefetch_blocks(self, blocks: List[ObjectRef[Block]]):
        self.prefetch_actor.prefetch.remote(*blocks)


@ray.remote(num_cpus=0)
class _BlockPretcher:
    """Helper actor that prefetches blocks asynchronously."""

    def prefetch(self, *blocks) -> None:
        pass
