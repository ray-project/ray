import dataclasses
import functools
import logging
import queue
import threading
import time
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import ray
from ray.actor import ActorHandle
from ray.data._internal.batcher import Batcher, ShufflingBatcher
from ray.data._internal.block_batching.interfaces import (
    Batch,
    BatchMetadata,
    BatchStageTimings,
    BlockPrefetcher,
    BlockStageTimings,
    CollatedBatch,
    ResolvedBlock,
)
from ray.data._internal.stats import DatasetStats, TimeSpan, _maybe_time
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")
I = TypeVar("I")
O = TypeVar("O")

_SENTINEL = object()


def iter_threaded(
    base_iterator: Iterator[T],
    fn: Callable[[Iterator[T]], Iterator[U]],
    num_workers: int = 1,
    output_buffer_size: int = 1,
) -> Generator[U, None, None]:
    """Apply ``fn`` to ``base_iterator`` across ``num_workers`` background
    threads, yielding results through a single bounded queue.

    Workers share ``base_iterator`` under a lock (so it may be a stateful,
    non-thread-safe generator) and run ``fn`` concurrently. With
    ``num_workers > 1`` the output order is not preserved and must be restored
    downstream if needed. In-flight items are bounded to roughly
    ``num_workers + output_buffer_size``, keeping pinned resources small. When
    the consumer stops early (``break``, ``.close()``, or GC), workers are
    signaled to stop via an event so they do not leak.

    Args:
        base_iterator: Iterator consumed (under a lock) by the workers.
        fn: Transform applied by each worker to its view of ``base_iterator``.
        num_workers: Number of background worker threads.
        output_buffer_size: Max number of items buffered in the output queue.

    Yields:
        U: Items produced by ``fn``.
    """
    if num_workers < 1:
        raise ValueError("num_workers must be at least 1.")

    stopped = threading.Event()
    result_queue: queue.Queue = queue.Queue(maxsize=output_buffer_size)
    iter_lock = threading.Lock()

    def _locked_next():
        # Pull the next item under a lock so workers can safely share a
        # stateful iterator. Returns _SENTINEL once exhausted or once the
        # consumer has stopped (so workers don't pay for one more fetch
        # before the next _put-side stop check).
        with iter_lock:
            if stopped.is_set():
                return _SENTINEL
            try:
                return next(base_iterator)
            except StopIteration:
                return _SENTINEL

    def _put(item) -> bool:
        """Put with periodic stop checks. Returns False if interrupted."""
        while not stopped.is_set():
            try:
                result_queue.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    remaining_workers = num_workers
    remaining_lock = threading.Lock()

    def _worker():
        nonlocal remaining_workers
        try:
            for item in fn(iter(_locked_next, _SENTINEL)):
                if not _put(item):
                    return
        except Exception as e:
            if not stopped.is_set():
                _put(e)
        finally:
            with remaining_lock:
                remaining_workers -= 1
                is_last = remaining_workers == 0
            if is_last and not stopped.is_set():
                _put(_SENTINEL)

    worker_threads = [
        threading.Thread(target=_worker, name="iter_threaded", daemon=True)
        for _ in range(num_workers)
    ]
    for t in worker_threads:
        t.start()

    try:
        while True:
            item = result_queue.get()
            if item is _SENTINEL:
                break
            if isinstance(item, Exception):
                raise item
            yield item
    finally:
        stopped.set()


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
) -> Iterator[ResolvedBlock]:
    """Resolves the block references for each logical batch.

    Each resolved block is wrapped in a ``ResolvedBlock`` that carries
    the per-block stage timings. The data_transfer window spans the
    ``ray.get()`` call; production_wait is captured around ``next()``
    upstream. When *stats* is provided, the cumulative ray.get() time is
    also recorded in ``stats.iter_get_s``.

    ``production_wait`` is captured for attribution only and not accumulated
    into ``iter_get_ref_bundles_s`` — that Timer is driven by
    ``prefetch_batches_locally.get_next_ref_bundle`` when prefetch is enabled;
    accumulating here would double-count.

    Args:
        block_ref_iter: An iterator over block object references.
        stats: An optional stats object to record block hits, misses, and
            cumulative ray.get() time.

    Yields:
        ResolvedBlock: Each resolved block with its stage timings.
    """
    hits = 0
    misses = 0
    unknowns = 0

    while True:
        # production_wait: upstream wait (not accumulated here).
        production_wait_start = time.perf_counter() if stats else 0.0
        try:
            block_ref = next(block_ref_iter)
        except StopIteration:
            break
        production_wait_span = (
            TimeSpan(start_s=production_wait_start, end_s=time.perf_counter())
            if stats
            else None
        )

        current_hit, current_miss, current_unknown = _calculate_ref_hits([block_ref])
        hits += current_hit
        misses += current_miss
        unknowns += current_unknown

        # data_transfer: cross-node transfer via ray.get().
        # TODO(amogkam): batch multiple references in one ray.get() call.
        with _maybe_time(stats.iter_get_s if stats else None) as data_transfer_span:
            block = ray.get(block_ref)

        stage_timings = BlockStageTimings(
            production_wait=production_wait_span,
            data_transfer=data_transfer_span,
        )
        yield ResolvedBlock(block=block, stage_timings=stage_timings)

    if stats:
        stats.iter_blocks_local = hits
        stats.iter_blocks_remote = misses
        stats.iter_unknown_location = unknowns


def blocks_to_batches(
    block_iter: Iterator[ResolvedBlock],
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
        block_iter: Iterator[ResolvedBlock],
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
        # Accumulates per-block fetch timings until a batch is yielded.
        self._pending_timings = BatchStageTimings()

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
        # Try to get a batch from current batcher state
        while True:
            can_yield = self._batcher.has_batch() or (
                self._batcher.has_any() and self._done_adding and not self._drop_last
            )

            if can_yield:
                with _maybe_time(
                    self._stats.iter_next_batch_s if self._stats else None
                ) as span:
                    next_batch = self._batcher.next_batch()
                self._pending_timings.batching = span

                res = Batch(
                    metadata=BatchMetadata(
                        batch_idx=self._global_counter,
                        num_rows=BlockAccessor.for_block(next_batch).num_rows(),
                        timings=self._pending_timings,
                    ),
                    data=next_batch,
                )
                self._pending_timings = BatchStageTimings()

                self._global_counter += 1
                return res

            elif not self._done_adding:
                # If can't yield try adding more blocks
                try:
                    # NOTE: Block ref is released immediately
                    block_result = next(self._block_iter)
                    if block_result.stage_timings is not None:
                        self._pending_timings.accumulate_block_timings(
                            block_result.stage_timings
                        )
                    self._batcher.add(block_result.block)
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
    ensure_copy: bool = False,
) -> Batch:
    with _maybe_time(stats.iter_format_batch_s if stats else None) as span:
        formatted_data = BlockAccessor.for_block(batch.data).to_batch_format(
            batch_format
        )
        if ensure_copy:
            formatted_data = _copy_batch(formatted_data)
    batch.metadata.stage_timings.format = span
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
    with _maybe_time(stats.iter_collate_batch_s if stats else None) as span:
        collated_data = collate_fn(batch.data)
    batch.metadata.stage_timings.collate = span
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
    with _maybe_time(stats.iter_finalize_batch_s if stats else None) as span:
        finalized_data = finalize_fn(batch.data)
    batch.metadata.stage_timings.finalize = span
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
