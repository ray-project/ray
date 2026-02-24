import dataclasses
import functools
import logging
import threading
from contextlib import nullcontext
from typing import Any, Callable, Generic, Iterator, List, Optional, Tuple, TypeVar

import ray
from ray.actor import ActorHandle
from ray.data._internal.batcher import Batcher, ShufflingBatcher
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BatchMetadata,
    BlockPrefetcher,
    CollatedBatch,
)
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

I = TypeVar("I")
O = TypeVar("O")


class _MappingIterator(Iterator[O], Generic[I, O]):
    """Iterator that applies a transform function to each element.

    Unlike a generator, local variables in __next__ go out of scope when the method
    returns, avoiding holding references to yielded values.
    """

    def __init__(self, input_iter: Iterator[I], transform_fn: Callable[[I], O]):
        self._input_iter = input_iter
        self._transform_fn = transform_fn

    def __iter__(self) -> "_MappingIterator[I, O]":
        return self

    def __next__(self) -> O:
        return self._transform_fn(next(self._input_iter))


def _calculate_ref_hits(refs: List[ObjectRef[Any]]) -> Tuple[int, int, int]:
    """Given a list of object references, returns how many are already on the local
    node, how many require fetching from another node, and how many have unknown
    locations. If `DataContext.get_current().enable_get_object_locations_for_metrics` is
    False, this will return `(0, 0, 0)` as getting object locations is disabled."""
    current_node_id = ray.get_runtime_context().get_node_id()

    ctx = ray.data.DataContext.get_current()
    if ctx.enable_get_object_locations_for_metrics:
        locs = ray.experimental.get_object_locations(refs)
        nodes: List[List[str]] = [loc["node_ids"] for loc in locs.values()]
        hits = sum(current_node_id in node_ids for node_ids in nodes)
        unknowns = sum(1 for node_ids in nodes if not node_ids)
        misses = len(nodes) - hits - unknowns
        return hits, misses, unknowns

    return 0, 0, 0


def resolve_block_refs(
    block_ref_iter: Iterator[ObjectRef[Block]],
    stats: Optional[DatasetStats] = None,
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
    stats: Optional[DatasetStats] = None,
    batch_size: Optional[int] = None,
    drop_last: bool = False,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
) -> Iterator[Batch]:
    """Given an iterator over blocks, returns an iterator over batches."""
    return _BatchingIterator(
        block_iter,
        stats=stats,
        batch_size=batch_size,
        drop_last=drop_last,
        shuffle_buffer_min_size=shuffle_buffer_min_size,
        shuffle_seed=shuffle_seed,
        ensure_copy=ensure_copy,
    )


class _BatchingIterator(Iterator[Batch]):
    """Iterator that converts blocks to batches.

    Unlike a generator, local variables in __next__ go out of scope when the method
    returns, avoiding holding references to yielded values.
    """

    def __init__(
        self,
        block_iter: Iterator[Block],
        stats: Optional[DatasetStats] = None,
        batch_size: Optional[int] = None,
        drop_last: bool = False,
        shuffle_buffer_min_size: Optional[int] = None,
        shuffle_seed: Optional[int] = None,
        ensure_copy: bool = False,
    ):
        self._block_iter = block_iter
        self._stats = stats
        self._drop_last = drop_last
        self._global_counter = 0
        self._done_adding = False

        if shuffle_buffer_min_size is not None:
            self._batcher = ShufflingBatcher(
                batch_size=batch_size,
                shuffle_buffer_min_size=shuffle_buffer_min_size,
                shuffle_seed=shuffle_seed,
            )
        else:
            self._batcher = Batcher(batch_size=batch_size, ensure_copy=ensure_copy)

    def __iter__(self) -> "_BatchingIterator":
        return self

    def __next__(self) -> Batch:
        timer = self._stats.iter_next_batch_s.timer() if self._stats else nullcontext()

        # Try to get a batch from current batcher state
        while True:
            can_yield = self._batcher.has_batch() or (
                self._batcher.has_any() and self._done_adding and not self._drop_last
            )

            if can_yield:
                with timer:
                    next_batch = self._batcher.next_batch()

                res = Batch(
                    metadata=BatchMetadata(batch_idx=self._global_counter),
                    data=next_batch,
                )

                self._global_counter += 1
                return res

            elif not self._done_adding:
                # If can't yield try adding more blocks
                try:
                    # NOTE: Block ref is released immediately
                    block = next(self._block_iter)
                    self._batcher.add(block)
                except StopIteration:
                    self._batcher.done_adding()
                    self._done_adding = True
            else:
                # In case when
                #   - We've exhausted input AND
                #   - There's nothing to yield anymore
                #
                # We stop the iteration
                raise StopIteration


def _format_batch(
    batch: Batch,
    batch_format: Optional[str],
    stats: Optional[DatasetStats],
) -> Batch:
    with stats.iter_format_batch_s.timer() if stats else nullcontext():
        formatted_data = BlockAccessor.for_block(batch.data).to_batch_format(
            batch_format
        )
    return dataclasses.replace(batch, data=formatted_data)


def format_batches(
    batch_iter: Iterator[Batch],
    batch_format: Optional[str],
    stats: Optional[DatasetStats] = None,
) -> Iterator[Batch]:
    """Given an iterator of batches, returns an iterator of formatted batches."""
    return _MappingIterator(
        batch_iter,
        functools.partial(_format_batch, batch_format=batch_format, stats=stats),
    )


def _collate_batch(
    batch: Batch,
    collate_fn: Callable[[DataBatch], Any],
    stats: Optional[DatasetStats],
) -> CollatedBatch:
    with stats.iter_collate_batch_s.timer() if stats else nullcontext():
        collated_data = collate_fn(batch.data)
    return CollatedBatch(metadata=batch.metadata, data=collated_data)


def collate(
    batch_iter: Iterator[Batch],
    collate_fn: Optional[Callable[[DataBatch], Any]],
    stats: Optional[DatasetStats] = None,
) -> Iterator[CollatedBatch]:
    """Returns an iterator with the provided collate_fn applied to batches."""
    if not isinstance(batch_iter, Iterator):
        batch_iter = iter(batch_iter)

    return _MappingIterator(
        batch_iter,
        functools.partial(_collate_batch, collate_fn=collate_fn, stats=stats),
    )


def _finalize_batch(
    batch: CollatedBatch,
    finalize_fn: Callable[[Any], Any],
    stats: Optional[DatasetStats],
) -> CollatedBatch:
    with stats.iter_finalize_batch_s.timer() if stats else nullcontext():
        finalized_data = finalize_fn(batch.data)
    return dataclasses.replace(batch, data=finalized_data)


def finalize_batches(
    batch_iter: Iterator[CollatedBatch],
    finalize_fn: Callable[[Any], Any],
    stats: Optional[DatasetStats] = None,
) -> Iterator[CollatedBatch]:
    """Returns an iterator with finalize_fn applied to batches."""
    if not isinstance(batch_iter, Iterator):
        batch_iter = iter(batch_iter)

    return _MappingIterator(
        batch_iter,
        functools.partial(_finalize_batch, finalize_fn=finalize_fn, stats=stats),
    )


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
        while not self._stopped:
            try:
                with self._condition:
                    if len(self._blocks) == 0:
                        # Park, waiting for notification that prefetching
                        # should resume
                        self._condition.wait()

                    blocks_to_fetch, self._blocks = self._blocks[:], []

                if len(blocks_to_fetch) > 0:
                    ray.wait(
                        blocks_to_fetch,
                        num_returns=1,
                        # NOTE: We deliberately setting timeout to 0 to avoid
                        #       blocking the fetching thread unnecessarily
                        timeout=0,
                        fetch_local=True,
                    )
            except Exception:
                logger.exception("Error in prefetcher thread.")

        logger.debug("Exiting prefetcher's background thread")

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
