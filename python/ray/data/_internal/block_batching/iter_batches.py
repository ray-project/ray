import collections
import time
from contextlib import contextmanager, nullcontext
from typing import Any, Callable, Dict, Iterator, Optional

import ray
from ray._private.ray_constants import env_integer
from ray.data._internal.block_batching.interfaces import Batch, BlockPrefetcher
from ray.data._internal.block_batching.util import (
    ActorBlockPrefetcher,
    WaitBlockPrefetcher,
    blocks_to_batches,
    collate,
    finalize_batches,
    format_batches,
    resolve_block_refs,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.stats import DatasetStats, _StatsManager
from ray.data._internal.util import make_async_gen
from ray.data.block import Block, DataBatch
from ray.data.context import DataContext
from ray.types import ObjectRef

DEFAULT_FORMAT_THREADPOOL_NUM_WORKERS = env_integer(
    "RAY_DATA_MAX_FORMAT_THREADPOOL_NUM_WORKERS", 4
)


class BatchIterator:
    """Defines an iterator pipeline to convert a stream of block object references
    into a stream of formatted batches ready to be consumed by the user.

    This takes a block iterator and creates batch_size batches, slicing,
    unioning, shuffling, prefetching, and formatting blocks as needed.

    This involves both pipeline parallelism (e.g. prefetching)
    and data parallelism (e.g. threadpool operations):

    If prefetch_batches=2, these are all the batches in flight:

    [User thread] trains on Batch 0
    - [Fetch thread] Batch 1 finalization + move to output queue
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
        5. Finalize each of the collated batches
        6. Fetch outputs from the threadpool, maintaining order of the batches.

    Args:
        ref_bundles: An iterator over RefBundles.
        stats: DatasetStats object to record timing and other statistics.
        dataset_tag: The tag of the dataset to record timing and other statistics.
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
            ``pyarrow.Table``, or None to use entire blocks
            as batches. Default is "default".
        drop_last: Whether to drop the last batch if it's incomplete.
        collate_fn: A function to apply to each data batch before returning it.
        finalize_fn: A function to apply to each data batch after it has been collated.
            This function is not run in a threadpool so it can be used for
            memory-intensive operations such as GPU preloading.
        shuffle_buffer_min_size: If non-None, the data will be randomly shuffled using a
            local in-memory shuffle buffer, and this value will serve as the minimum
            number of rows that must be in the local in-memory shuffle buffer in order
            to yield a batch.
        shuffle_seed: The seed to use for the local random shuffle.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).
        prefetch_batches: The number of batches to fetch ahead of the current batch to
            process. If set to greater than 0, a separate thread will be used to fetch
            the specified amount of formatted batches from blocks. This improves
            performance for non-CPU bound UDFs, allowing batch fetching compute and
            formatting to be overlapped with the UDF. Defaults to 1.
    """

    UPDATE_METRICS_INTERVAL_S: float = 5.0

    def __init__(
        self,
        ref_bundles: Iterator[RefBundle],
        *,
        stats: Optional[DatasetStats] = None,
        dataset_tag: Optional[str] = None,
        clear_block_after_read: bool = False,
        batch_size: Optional[int] = None,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        collate_fn: Optional[Callable[[DataBatch], Any]] = None,
        finalize_fn: Optional[Callable[[Any], Any]] = None,
        shuffle_buffer_min_size: Optional[int] = None,
        shuffle_seed: Optional[int] = None,
        ensure_copy: bool = False,
        prefetch_batches: int = 1,
    ):
        self._ref_bundles = ref_bundles
        self._stats = stats
        self._dataset_tag = dataset_tag
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._drop_last = drop_last
        self._collate_fn = collate_fn
        self._finalize_fn = finalize_fn
        self._shuffle_buffer_min_size = shuffle_buffer_min_size
        self._shuffle_seed = shuffle_seed
        self._ensure_copy = ensure_copy
        self._prefetch_batches = prefetch_batches
        # TODO: pass the dataset's context down instead of fetching the global context here.
        self._ctx = DataContext.get_current()
        self._eager_free = clear_block_after_read and self._ctx.eager_free

        actor_prefetcher_enabled = (
            prefetch_batches > 0
            and self._ctx.actor_prefetcher_enabled
            and not ray.util.client.ray.is_connected()
        )
        self._prefetcher = (
            ActorBlockPrefetcher()
            if actor_prefetcher_enabled
            else WaitBlockPrefetcher()
        )
        self._yielded_first_batch = False

        # This stores the last time we updated the metrics.
        # This allows us to update metrics on some interval,
        # by comparing it with the current timestamp.
        self._metrics_last_updated: float = 0.0

    def _prefetch_blocks(
        self, ref_bundles: Iterator[RefBundle]
    ) -> Iterator[ObjectRef[Block]]:
        return prefetch_batches_locally(
            ref_bundles=ref_bundles,
            prefetcher=self._prefetcher,
            num_batches_to_prefetch=self._prefetch_batches,
            batch_size=self._batch_size,
            eager_free=self._eager_free,
            stats=self._stats,
        )

    def _resolve_block_refs(
        self, block_refs: Iterator[ObjectRef[Block]]
    ) -> Iterator[Block]:
        return resolve_block_refs(
            block_ref_iter=block_refs,
            stats=self._stats,
            ctx=self._ctx,
            max_get_batch_size=self._max_block_get_batch_size,
        )

    def _max_block_get_batch_size(self) -> int:
        prefetched_blocks = self._prefetcher.num_prefetched_blocks()
        if prefetched_blocks <= 0:
            prefetched_blocks = (
                self._prefetch_batches if self._prefetch_batches > 0 else 0
            )
        limit = max(1, prefetched_blocks + 1)
        return min(self._ctx.iter_get_block_batch_size, limit)

    def _blocks_to_batches(self, blocks: Iterator[Block]) -> Iterator[Batch]:
        return blocks_to_batches(
            block_iter=blocks,
            stats=self._stats,
            batch_size=self._batch_size,
            drop_last=self._drop_last,
            shuffle_buffer_min_size=self._shuffle_buffer_min_size,
            shuffle_seed=self._shuffle_seed,
            ensure_copy=self._ensure_copy,
        )

    def _format_batches(self, batches: Iterator[Batch]) -> Iterator[Batch]:
        num_threadpool_workers = min(
            DEFAULT_FORMAT_THREADPOOL_NUM_WORKERS, self._prefetch_batches
        )
        return _format_in_threadpool(
            batch_iter=batches,
            stats=self._stats,
            batch_format=self._batch_format,
            collate_fn=self._collate_fn,
            num_threadpool_workers=num_threadpool_workers,
        )

    def _finalize_batches(
        self,
        batch_iter: Iterator[Batch],
    ) -> Iterator[Batch]:
        if self._finalize_fn is None:
            return batch_iter

        return finalize_batches(
            batch_iter, finalize_fn=self._finalize_fn, stats=self._stats
        )

    def _restore_original_batch_order(
        self, batches: Iterator[Batch]
    ) -> Iterator[Batch]:
        return restore_original_order(batches)

    def _pipeline(self, ref_bundles: Iterator[RefBundle]) -> Iterator[Batch]:
        # Step 1: Prefetch logical batches locally.
        block_iter = self._prefetch_blocks(ref_bundles)

        # Step 2: Resolve the blocks.
        block_iter = self._resolve_block_refs(block_iter)

        # Step 3: Batch and shuffle the resolved blocks.
        batch_iter = self._blocks_to_batches(block_iter)

        # Step 4: Format and collate the batches in a threadpool.
        batch_iter = self._format_batches(batch_iter)

        # Step 5: Finalize the batches (e.g., move to GPU).
        batch_iter = self._finalize_batches(batch_iter)

        # Step 6: Restore the original order of the batches, as the prior
        # threadpool operations may have reordered the batches non-deterministically.
        batch_iter = self._restore_original_batch_order(batch_iter)

        yield from batch_iter

    def _iter_batches(self) -> Iterator[DataBatch]:
        async_batch_iter = make_async_gen(
            self._ref_bundles,
            fn=self._pipeline,
            num_workers=1,
            preserve_ordering=False,
            buffer_size=max(self._prefetch_batches, 1),
        )

        self.before_epoch_start()

        while True:
            with self.get_next_batch_context():
                try:
                    batch = next(async_batch_iter)
                except StopIteration:
                    break
            with self.yield_batch_context(batch):
                yield batch.data

        self.after_epoch_end()

    def __iter__(self) -> Iterator[DataBatch]:
        return self._iter_batches()

    def before_epoch_start(self):
        self._yielded_first_batch = False

    def after_epoch_end(self):
        if self._stats is None:
            return

        _StatsManager.update_iteration_metrics(self._stats, self._dataset_tag)

    @contextmanager
    def get_next_batch_context(self):
        try:
            if self._stats:
                # Always track total blocked time
                total_timer = self._stats.iter_total_blocked_s.timer()
                # Also track the time until the first batch is ready
                first_batch_ready_timer = (
                    self._stats.iter_time_to_first_batch_s.timer()
                    if not self._yielded_first_batch
                    else nullcontext()
                )
                with total_timer, first_batch_ready_timer:
                    yield
            else:
                yield
        finally:
            self._yielded_first_batch = True

    @contextmanager
    def yield_batch_context(self, batch: Batch):
        with self._stats.iter_user_s.timer() if self._stats else nullcontext():
            yield

        if self._stats is None:
            return
        now = time.time()
        if (now - self._metrics_last_updated) > self.UPDATE_METRICS_INTERVAL_S:
            _StatsManager.update_iteration_metrics(self._stats, self._dataset_tag)
            self._metrics_last_updated = now


def _format_in_threadpool(
    batch_iter: Iterator[Batch],
    stats: DatasetStats,
    batch_format: Optional[str],
    collate_fn: Optional[Callable[[DataBatch], Any]],
    num_threadpool_workers: int,
) -> Iterator[Batch]:
    """Executes the batching, formatting, and collation logic in a threadpool.

    Args:
        logical_batch_iterator: An iterator over logical batches.
        stats: DatasetStats object to record timing and other statistics.
        batch_format: The format in which to return each batch.
            Specify "default" to use the current block format (promoting
            Arrow to pandas automatically), "pandas" to
            select ``pandas.DataFrame`` or "pyarrow" to select
            ``pyarrow.Table``, or None to use entire blocks
            as batches.
        collate_fn: A function to apply to each data batch before returning it.
        num_threadpool_workers: The number of threads to use in the threadpool.
    """

    def threadpool_computations_format_collate(
        batch_iter: Iterator[Batch],
    ) -> Iterator[Batch]:
        # Step 4a: Format the batches.
        formatted_batch_iter = format_batches(
            batch_iter, batch_format=batch_format, stats=stats
        )

        # Step 4b: Apply the collate function if applicable.
        if collate_fn is not None:
            formatted_batch_iter = collate(
                formatted_batch_iter, collate_fn=collate_fn, stats=stats
            )
        yield from formatted_batch_iter

    if num_threadpool_workers > 0:
        collated_iter = make_async_gen(
            base_iterator=batch_iter,
            fn=threadpool_computations_format_collate,
            preserve_ordering=False,
            num_workers=num_threadpool_workers,
        )
    else:
        collated_iter = threadpool_computations_format_collate(batch_iter)
    return collated_iter


def prefetch_batches_locally(
    ref_bundles: Iterator[RefBundle],
    prefetcher: BlockPrefetcher,
    num_batches_to_prefetch: int,
    batch_size: Optional[int],
    eager_free: bool = False,
    stats: Optional[DatasetStats] = None,
) -> Iterator[ObjectRef[Block]]:
    """Given an iterator of batched RefBundles, returns an iterator over the
    corresponding block references while prefetching `num_batches_to_prefetch`
    batches in advance.

    Args:
        ref_bundles: An iterator over batched RefBundles.
        prefetcher: The prefetcher to use.
        num_batches_to_prefetch: The number of batches to prefetch ahead of the
            current batch during the scan.
        batch_size: User specified batch size, or None to let the system pick.
        eager_free: Whether to eagerly free the object reference from the object store.
        stats: Dataset stats object used to store ref bundle retrieval time.
    """

    def get_next_ref_bundle() -> RefBundle:
        with stats.iter_get_ref_bundles_s.timer() if stats else nullcontext():
            return next(ref_bundles)

    sliding_window = collections.deque()
    current_window_size = 0

    if num_batches_to_prefetch <= 0:
        if stats:
            stats.iter_prefetched_bytes = 0
        for ref_bundle in ref_bundles:
            for block_ref in ref_bundle.block_refs:
                yield block_ref
        return

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
            next_ref_bundle = get_next_ref_bundle()
            sliding_window.extend(next_ref_bundle.blocks)
            current_window_size += next_ref_bundle.num_rows()
        except StopIteration:
            break

    prefetcher.prefetch_blocks([block_ref for block_ref, _ in list(sliding_window)])
    if stats:
        stats.iter_prefetched_bytes = sum(
            metadata.size_bytes or 0 for _, metadata in sliding_window
        )

    while sliding_window:
        block_ref, metadata = sliding_window.popleft()
        current_window_size -= metadata.num_rows
        if batch_size is None or current_window_size < num_rows_to_prefetch:
            try:
                next_ref_bundle = get_next_ref_bundle()
                for block_ref_and_md in next_ref_bundle.blocks:
                    sliding_window.append(block_ref_and_md)
                    current_window_size += block_ref_and_md[1].num_rows
                prefetcher.prefetch_blocks(
                    [block_ref for block_ref, _ in list(sliding_window)]
                )
            except StopIteration:
                pass
        if stats:
            stats.iter_prefetched_bytes = sum(
                metadata.size_bytes or 0 for _, metadata in sliding_window
            )
        yield block_ref
        trace_deallocation(block_ref, loc="iter_batches", free=eager_free)
    prefetcher.stop()


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
        assert batch.metadata.batch_idx not in buffer
        buffer[batch.metadata.batch_idx] = batch
        while next_index_required in buffer:
            yield buffer.pop(next_index_required)
            next_index_required += 1

    while next_index_required in buffer:
        yield buffer.pop(next_index_required)
        next_index_required += 1
