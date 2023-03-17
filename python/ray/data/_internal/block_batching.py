from concurrent.futures import ThreadPoolExecutor
import collections
import itertools
import queue
import sys
import threading
from typing import Any, Callable, Iterator, List, Optional, Tuple, TypeVar, Union

import ray
from ray.actor import ActorHandle
from ray.data.block import BlockMetadata
from ray.data._internal.batcher import Batcher, ShufflingBatcher
from ray.data._internal.stats import DatasetPipelineStats, DatasetStats
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DatasetContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

T = TypeVar("T")

if sys.version_info >= (3, 7):
    from contextlib import nullcontext
else:
    from contextlib import contextmanager

    @contextmanager
    def nullcontext(enter_result=None):
        yield enter_result


PREFETCHER_ACTOR_NAMESPACE = "ray.dataset"


def batch_block_refs(
    block_refs: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    *,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    clear_block_after_read: bool = False,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
    prefetch_batches: int = 0,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from 1 or more block object references.

    This takes a block iterator and creates batch_size batches, slicing,
    unioning, shuffling, prefetching, and formatting blocks as needed.

    This is used by both Dataset.iter_batches()/DatasetPipeline.iter_batches()
    and Dataset.map_batches()/DatasetPipeline.map_batches().

    Args:
        block_refs: An iterator over block object references.
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
        collate_fn: A function to apply to each data batch before returning it.
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
            formatting to be overlapped with the UDF. Defaults to 0 (no prefetching
            enabled).

    Returns:
        An iterator over record batches.
    """

    context = DatasetContext.get_current()

    if (
        prefetch_batches > 0
        and context.actor_prefetcher_enabled
        and not ray.util.client.ray.is_connected()
    ):
        prefetcher = ActorBlockPrefetcher()
    else:
        prefetcher = WaitBlockPrefetcher()

    if prefetch_batches > 0:
        def computations(base_iterator):
             yield from batch_blocks(
                _resolve_blocks(
                    _prefetch_batch_locally(
                        base_iterator,
                        prefetcher=prefetcher,
                        stats=stats,
                        batch_size=batch_size
                    ),
                    clear_block_after_read=clear_block_after_read,
                    stats=stats,
                ),
                batch_size=batch_size,
                batch_format=batch_format,
                drop_last=drop_last,
                collate_fn=collate_fn,
                shuffle_buffer_min_size=shuffle_buffer_min_size,
                shuffle_seed=shuffle_seed,
                ensure_copy=ensure_copy,
                prefetch_batches=0,
                stats=stats,
             )

        yield from _make_async_gen_threadpool(block_refs, max_workers=prefetch_batches, computations=computations, stats=stats)

    else:
        with stats.thread_total_time_s.timer():
            if prefetch_batches > 0:
                block_ref_iter = _prefetch_batch_locally(block_ref_iter=block_refs, prefetcher=prefetcher, stats=stats, batch_size=batch_size)
            else:
                def block_ref_gen() -> Iterator[ObjectRef[Block]]:
                    for block_ref in block_refs:
                        yield block_ref[0]   

                block_ref_iter = block_ref_gen()     

            block_iter = _resolve_blocks(block_ref_iter,
                clear_block_after_read=clear_block_after_read,
                stats=stats,
            )

            yield from batch_blocks(
                block_iter,
                stats=stats,
                batch_size=batch_size,
                batch_format=batch_format,
                drop_last=drop_last,
                collate_fn=collate_fn,
                shuffle_buffer_min_size=shuffle_buffer_min_size,
                shuffle_seed=shuffle_seed,
                ensure_copy=ensure_copy,
                prefetch_batches=prefetch_batches,
            )


def batch_blocks(
    blocks: Iterator[Block],
    *,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    drop_last: bool = False,
    collate_fn: Optional[Callable[[DataBatch], DataBatch]] = None,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
    prefetch_batches: int = 0,
) -> Iterator[DataBatch]:
    """Create formatted batches of data from 1 or more blocks.

    This is equivalent to batch_block_refs, except
    it takes in an iterator consisting of already fetched blocks.
    This means that this function does not support block prefetching.
    """

    batch_iter = _format_batches(
        _blocks_to_batches(
            block_iter=blocks,
            stats=stats,
            batch_size=batch_size,
            drop_last=drop_last,
            shuffle_buffer_min_size=shuffle_buffer_min_size,
            shuffle_seed=shuffle_seed,
            ensure_copy=ensure_copy,
        ),
        batch_format=batch_format,
        stats=stats,
    )

    if collate_fn is not None:

        def batch_fn_iter(iterator: Iterator[DataBatch]) -> Iterator[DataBatch]:
            for batch in iterator:
                with stats.iter_collate_batch_s.timer():
                    output = collate_fn(batch)
                yield output

        batch_iter = batch_fn_iter(batch_iter)

    if prefetch_batches > 0:
        batch_iter = _make_async_gen_threadpool(batch_iter, max_workers=prefetch_batches)
        #batch_iter = _make_async_gen(batch_iter, prefetch_batches)

    for formatted_batch in batch_iter:
        user_timer = stats.iter_user_s.timer() if stats else nullcontext()
        with user_timer:
            yield formatted_batch


def _make_async_gen(
    base_iterator: Iterator[T], prefetch_buffer_size: int = 1
) -> Iterator[T]:
    """Returns a new iterator with elements fetched from the base_iterator
    in an async fashion using a background thread.

    Args:
        base_iterator: The iterator to asynchronously fetch from.
        prefetch_buffer_size: The maximum number of items to prefetch. Increasing the
            size allows for more computation overlap for very expensive downstream UDFs.
            However it comes at the cost of additional memory overhead. Defaults to 1.

    Returns:
        An iterator with the same elements as the base_iterator.
    """

    fetch_queue = queue.Queue(maxsize=prefetch_buffer_size)

    sentinel = object()

    def _async_fetch():
        for item in base_iterator:
            fetch_queue.put(item, block=True)

        # Indicate done adding items.
        fetch_queue.put(sentinel, block=True)

    # Start a background thread which iterates through the base iterator,
    # triggering execution and adding results to the queue until it is full.
    # Iterating through the iterator returned by this function pulls
    # ready items from the queue, allowing the background thread to continue execution.

    fetch_thread = threading.Thread(target=_async_fetch)
    fetch_thread.start()

    while True:
        next_item = fetch_queue.get(block=True)
        if next_item is not sentinel:
            yield next_item
        fetch_queue.task_done()
        if next_item is sentinel:
            break

    fetch_queue.join()
    fetch_thread.join()


def _make_async_gen_threadpool(base_iterator: Iterator[T], max_workers: int = 1, computations=None, stats=None):
    """Returns a new iterator with elements fetched from the base_iterator
    in an async fashion using a threadpool.

    All the threads in the threadpool will fetch data from the base_iterator, triggering the base iterator's execution.

    Args:
        base_iterator: The iterator to asynchronously fetch from.
        max_workers: The maximum number of threads to use in the threadpool.

    Returns:
        An iterator with the same elements as the base_iterator.
    """

    def convert_to_threadsafe_iterator(base_iterator):
        class ThreadSafeIterator:
            def __init__(self, it):
                self.lock = threading.Lock()
                self.it = it
            
            def __next__(self):
                with self.lock:
                    return next(self.it)
        
        return ThreadSafeIterator(base_iterator)
    
    thread_safe_generator = convert_to_threadsafe_iterator(base_iterator)

    # class Sentinel:
    #     def __init__()
    sentinel = object()

    fetch_queue = queue.Queue()
    semaphore = threading.Semaphore(0)
    
    def execute():
        with stats.thread_total_time_s.timer():
            for item in computations(thread_safe_generator):
                fetch_queue.put(item, block=True)
            #semaphore.release()
            fetch_queue.put(sentinel, block=True)
            
        # try:
        #     return next(thread_safe_generator)
        # except StopIteration:
        #     return sentinel
    
    executor = ThreadPoolExecutor(max_workers=max_workers)

    ordered_futures = collections.deque()
    for i in range(max_workers):
        ordered_futures.append(executor.submit(execute))

    # finished = False
    # while not finished:
    #     next_result = ordered_futures.popleft().result()
    #     if next_result is sentinel:
    #         finished = True
    #     else:
    #         ordered_futures.append(executor.submit(execute))
    #         yield next_result
    
    # for future in ordered_futures:
    #     output = future.result()
    #     if output is not sentinel:
    #         yield output

    # while True:
    #     num_threads_finished = 0
    #     try:
    #         next_item = fetch_queue.get(timeout=0.01)
    #         yield next_item
    #         fetch_queue.task_done()
    #     except queue.Empty:
    #         pass
    #     if semaphore.acquire(blocking=False):
    #         num_threads_finished += 1
    #     if num_threads_finished >= max_workers:
    #         break

    # while True:
    #     try:
    #         next_item = fetch_queue.get(block=False)
    #         yield next_item
    #     except queue.Empty:
    #         break

    num_threads_finished = 0
    while True:
        next_item = fetch_queue.get(block=True)
        if next_item is not sentinel:
            yield next_item
        else:
            print("Thread finished")
            num_threads_finished += 1
        if num_threads_finished >= max_workers:
            break


def _resolve_blocks(
    block_ref_iter: Iterator[ObjectRef[Block]],
    clear_block_after_read: bool = False, 
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[Block]:
    """Given an iterator of unresolved blocks (as Ray object references), returns an
    iterator of resolved blocks.

    The length of the returned iterator may be less than the length of the original
    if any of the references in the original iterator are None.

    Args:
        block_ref_iter: An iterator over block object references.
        clear_block_after_read: Whether to clear the block from object store
            manually (i.e. without waiting for Python's automatic GC) after it
            is read. Doing so will reclaim memory faster and hence reduce the
            memory footprint. However, the caller has to ensure the safety, i.e.
            the block will never be accessed again.
        stats: Dataset stats object used to store block fetching time.

    Returns:
        An iterator over resolved blocks.
    """
    eager_free = clear_block_after_read and DatasetContext.get_current().eager_free

    hit = 0
    miss = 0
    unknown = 0
    for block_ref in block_ref_iter:
        if block_ref is not None:
            stats_timer = stats.iter_get_s.timer() if stats else nullcontext()
            # Count the number of blocks that we hit locally or miss (so have to
            # fetch from remote node). This is to measure the effectiveness of
            # prefetch.
            loc = ray.experimental.get_object_locations([block_ref])
            nodes = loc[block_ref]["node_ids"]
            if nodes:
                current = ray.get_runtime_context().get_node_id()
                if current in nodes:
                    hit += 1
                else:
                    miss += 1
            else:
                unknown += 1
            with stats_timer:
                block = ray.get(block_ref)
                trace_deallocation(
                    block_ref, "block_batching._prefetch_blocks", free=eager_free
                )
            yield block

    if stats:
        stats.iter_blocks_local += hit
        stats.iter_blocks_remote += miss
        stats.iter_unknown_location += unknown


def _prefetch_batches_locally(
    block_ref_iter: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
    prefetcher: "BlockPrefetcher",
    batch_size: int,
    num_batches_to_prefetch: int,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[ObjectRef[Block]]:
    """Given an iterable of Block Object References and their corresponding metadata, returns an iterator over these object reference while prefetching a batch in advance.

    The batch is determined by the provided `batch_size`, which may differ from the block sizes.

    Args:
        block_ref_iter: An iterator over block object references.
        num_blocks_to_prefetch: The number of blocks to prefetch ahead of the
            current block during the scan.
        stats: Dataset stats object used to store block wait time.
    """

    def bundle_batch() -> List[ObjectRef[Block]]:
        """Returns the block refs comprising the next batch of the dataset."""
        blocks_for_batch = collections.deque()
        num_rows_counter = 0
        if batch_size is None:
            try:
                return [next(block_ref_iter)]
            except StopIteration:
                return []
        else:
            while num_rows_counter < batch_size:
                try:
                    next_block_ref, next_metadata = next(block_ref_iter)
                    blocks_for_batch.append(next_block_ref)
                    if next_metadata.num_rows is None:
                        # If the number of rows for this block is not known, then
                        # assume 1 block ==  1 batch.
                        break
                    else:
                        num_rows_counter += next_metadata.num_rows
                except StopIteration:
                    break
            return blocks_for_batch

    # Create the initial set of blocks to prefetch.
    blocks_for_batch = bundle_batch()
    with stats.iter_wait_s.timer() if stats else nullcontext():
        prefetcher.prefetch_blocks(list(blocks_for_batch))

    while len(blocks_for_batch) > 0:
        current_block = blocks_for_batch.popleft()
        if len(blocks_for_batch) == 0:
            # Prefetch the next batch while batch formatting and collation is happening
            # on the current batch.
            blocks_for_batch = bundle_batch()
            with stats.iter_wait_s.timer() if stats else nullcontext():
                prefetcher.prefetch_blocks(list(blocks_for_batch))
            
        yield current_block

def _blocks_to_batches(
    block_iter: Iterator[Block],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    batch_size: Optional[int] = None,
    drop_last: bool = False,
    shuffle_buffer_min_size: Optional[int] = None,
    shuffle_seed: Optional[int] = None,
    ensure_copy: bool = False,
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

    for block in block_iter:
        batcher.add(block)
        while batcher.has_batch():
            with get_iter_next_batch_s_timer():
                batch = batcher.next_batch()
            yield batch

    # Signal to the batcher that there are no more blocks to add.
    batcher.done_adding()

    # Get any leftover batches in ShufflingBatcher.
    while batcher.has_batch():
        with get_iter_next_batch_s_timer():
            batch = batcher.next_batch()
        yield batch

    # Get any remaining data.
    if not drop_last and batcher.has_any():
        with get_iter_next_batch_s_timer():
            batch = batcher.next_batch()
        yield batch


def _format_batches(
    block_iter: Iterator[Block],
    batch_format: str,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[DataBatch]:
    """Given an iterator of blocks, returns an iterator of formatted batches.

    Args:
        block_iter: An iterator over blocks.
        batch_format: The batch format to use.

    Returns:
        An iterator over formatted batches.
    """
    for block in block_iter:
        with stats.iter_format_batch_s.timer() if stats else nullcontext():
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
