import dataclasses
import functools
import logging
import os
import queue
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

logger = logging.getLogger(__name__)

# When set, run the local-shuffle `ShufflingBatcher` add/next_batch loop on a
# dedicated producer thread that pushes formed batches into a bounded queue,
# so that the per-batch and pre-compaction `take` calls overlap with downstream
# format/finalize/train work. Off by default; gated to ease A/B benchmarking.
_RAY_DATA_SHUFFLE_BUFFER_ASYNC_TAKE = bool(
    int(os.environ.get("RAY_DATA_SHUFFLE_BUFFER_ASYNC_TAKE", "0"))
)
_RAY_DATA_SHUFFLE_BUFFER_ASYNC_TAKE_QUEUE_DEPTH = max(
    1, int(os.environ.get("RAY_DATA_SHUFFLE_BUFFER_ASYNC_TAKE_QUEUE_DEPTH", "4"))
)

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

        self._async_take = (
            shuffle_buffer_min_size is not None and _RAY_DATA_SHUFFLE_BUFFER_ASYNC_TAKE
        )
        if self._async_take:
            self._batch_queue: "queue.Queue" = queue.Queue(
                maxsize=_RAY_DATA_SHUFFLE_BUFFER_ASYNC_TAKE_QUEUE_DEPTH
            )
            self._sentinel = object()
            self._producer_thread: Optional[threading.Thread] = None

    def __iter__(self) -> "_BatchingIterator":
        return self

    def __next__(self) -> Batch:
        if self._async_take:
            return self._next_async()
        return self._next_sync()

    def _next_sync(self) -> Batch:
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

    def _next_async(self) -> Batch:
        if self._producer_thread is None:
            self._producer_thread = threading.Thread(
                target=self._producer_loop,
                name="ShufflingBatcherAsyncTake",
                daemon=True,
            )
            self._producer_thread.start()

        timer = self._stats.iter_next_batch_s.timer() if self._stats else nullcontext()
        with timer:
            item = self._batch_queue.get()
        if isinstance(item, BaseException):
            raise item
        if item is self._sentinel:
            raise StopIteration
        return item

    def _producer_loop(self) -> None:
        # Mirrors the control flow of _next_sync, but runs on a dedicated thread
        # and pushes formed batches into a bounded queue. The expensive `take`
        # calls inside ShufflingBatcher.next_batch release the GIL, so they can
        # run in parallel with downstream format/finalize/train work on the
        # consumer thread.
        try:
            done_adding = False
            while True:
                can_yield = self._batcher.has_batch() or (
                    self._batcher.has_any() and done_adding and not self._drop_last
                )

                if can_yield:
                    next_batch = self._batcher.next_batch()
                    res = Batch(
                        metadata=BatchMetadata(batch_idx=self._global_counter),
                        data=next_batch,
                    )
                    self._global_counter += 1
                    self._batch_queue.put(res)
                elif not done_adding:
                    try:
                        block = next(self._block_iter)
                        self._batcher.add(block)
                    except StopIteration:
                        self._batcher.done_adding()
                        done_adding = True
                else:
                    self._batch_queue.put(self._sentinel)
                    return
        except BaseException as e:
            self._batch_queue.put(e)


def _format_batch(
    batch: Batch,
    batch_format: Optional[str],
    stats: Optional[DatasetStats],
    ensure_copy: bool = False,
) -> Batch:
    with stats.iter_format_batch_s.timer() if stats else nullcontext():
        formatted_data = BlockAccessor.for_block(batch.data).to_batch_format(
            batch_format
        )
        if ensure_copy:
            formatted_data = _copy_batch(formatted_data)
    return dataclasses.replace(batch, data=formatted_data)


def _copy_batch(batch: "DataBatch") -> "DataBatch":
    """Return a copy of a batch, making it writable.

    ``pa.Array.to_numpy()`` returns read-only arrays by default, so when
    a caller passes ``ensure_copy=True`` (i.e. ``zero_copy_batch=False``) and the
    block is Arrow, the numpy-format batch must be explicitly copied to give the UDF
    writable arrays.
    """
    import numpy as np

    if isinstance(batch, dict):
        # Return a dictionary with the same keys (column names) and values (column numpy arrays),
        # with the values copied
        return {
            k: v.copy() if isinstance(v, np.ndarray) else v for k, v in batch.items()
        }
    elif isinstance(batch, np.ndarray):
        return batch.copy()
    return batch


def format_batches(
    batch_iter: Iterator[Batch],
    batch_format: Optional[str],
    stats: Optional[DatasetStats] = None,
    ensure_copy: bool = False,
) -> Iterator[Batch]:
    """Given an iterator of batches, returns an iterator of formatted batches."""
    return _MappingIterator(
        batch_iter,
        functools.partial(
            _format_batch,
            batch_format=batch_format,
            stats=stats,
            ensure_copy=ensure_copy,
        ),
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
            label_selector={ray._raylet.RAY_NODE_ID_KEY: node_id},
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
