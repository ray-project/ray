from concurrent.futures import ThreadPoolExecutor
import collections
import itertools
import queue
import random
import sys
import threading
from typing import Any, Callable, Iterator, List, NamedTuple, Optional, Tuple, TypeVar, Union

import ray
from ray.actor import ActorHandle
from ray.data.block import BlockMetadata
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
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

class LogicalBatch(NamedTuple):
    """A logical "batch" of data.
    
    This is not a fully created batch, but rather a conceptual batch
    consisting of unresolved Block Object references.

    Attributes:
        bundle_idx: The global index of this bundle so that downstream operations can
            maintain ordering.
        block_refs: The list of block object references for this batch.
        starting_block_idx: The index of the first block where this batch starts.
        ending_block_idx: The index of the last block where this batch ends.
        num_rows: The number of rows in this batch. This should be equivalent to the
            provided batch size, except for the final batch.
    """
    batch_idx: int
    block_refs: List[ObjectRef[Block]]
    starting_block_idx: Optional[int]
    ending_block_idx: Optional[int]
    num_rows: int


class ResolvedLogicalBatch(NamedTuple):
    """Same as LogicalBatch except contains resolved blocks instead of block refs."""

    batch_idx: int
    blocks: List[Block]
    starting_block_idx: Optional[int]
    ending_block_idx: Optional[int]
    num_rows: int

def _bundle_block_refs_to_logical_batches(
        block_ref_iterator: Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
        batch_size: Optional[int],
        drop_last: bool = False
) -> Iterator[LogicalBatch]:
    """Given an iterator of block object references, and their corresponding metadata,
    bundles the block object references into groups of the provided `batch_size`.

    The output iterator returns an iterator over LogicalBatch objects.

    This function does not do any slicing or creation of actual batch objects.
    """
    batch_buffer: List[ObjectRef[Block]] = []
    buffer_size = 0
    starting_index = 0

    global_index = 0
    for block_ref, metadata in block_ref_iterator:
        if block_ref is not None:
            next_block_num_rows = metadata.num_rows
            if batch_size is None:
                yield LogicalBatch(global_index, [block_ref], None, None, next_block_num_rows)
                global_index += 1
            else:
                if buffer_size + next_block_num_rows <= batch_size:
                    batch_buffer.append(block_ref)
                    buffer_size += next_block_num_rows

                    # Yield if exactly equal to batch size.
                    if buffer_size == batch_size:
                        yield LogicalBatch(global_index, batch_buffer, starting_index, None, buffer_size)
                        batch_buffer = []
                        buffer_size = 0
                        starting_index = 0
                        global_index += 1
                else:
                    # Add leftover for this batch.
                    need = (buffer_size + next_block_num_rows) - batch_size
                    assert need > 0
                    batch_buffer.append(block_ref)
                    buffer_size += need
                    assert buffer_size == batch_size
                    yield LogicalBatch(global_index, batch_buffer, starting_index, need, buffer_size)
                    # Carry over the remainder to the next logical batch
                    batch_buffer = [block_ref]
                    buffer_size = next_block_num_rows - need
                    starting_index = need
                    global_index += 1


    # Yield any leftover batches if necessary.
    if buffer_size > 0 and not drop_last:
        yield LogicalBatch(global_index, batch_buffer, starting_index, None, buffer_size)
        global_index += 1

def _local_shuffle_logical_batches(
        logical_batch_iterator: Iterator[LogicalBatch],
        shuffle_buffer_min_size: int,
        shuffle_seed: Optional[int] = None,
) -> Iterator[LogicalBatch]:
    """Shuffles the provided logical batch iterator using a buffer of the provided size.
    """

    if shuffle_seed is not None:
        random.seed(shuffle_seed)
    
    shuffle_buffer: List[LogicalBatch] = []
    shuffle_buffer_size = 0

    for logical_batch in logical_batch_iterator:
        shuffle_buffer.append(logical_batch)
        shuffle_buffer_size += logical_batch.num_rows

        while shuffle_buffer_size > shuffle_buffer_min_size:
            output_batch = shuffle_buffer.pop(random.randint(0, len(shuffle_buffer)))
            yield output_batch
            shuffle_buffer_size -= output_batch.num_rows

    # Yield any leftover.
    while len(shuffle_buffer) > 0:
        yield shuffle_buffer[random.randint(0, len(shuffle_buffer))]

def _prefetch_batches_locally(
    logical_batch_iter: Iterator[LogicalBatch],
    prefetcher: "BlockPrefetcher",
    num_batches_to_prefetch: int,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[LogicalBatch]:
    """Given an iterator of logical batches, returns an iterator over the same logical batches, while prefetching `num_batches_to_prefetch` batches in advance.

    Args:
        logical_batch_iter: An iterator over logical batches.
        prefetcher: The prefetcher to use.
        num_batches_to_prefetch: The number of batches to prefetch ahead of the
            current batch during the scan.
        stats: Dataset stats object used to store block wait time.
    """
    def get_next_batches() -> Iterator[List[LogicalBatch]]:
        """Return lists of logical batches corresponding to the provided"""
        next_batches = []
        while True:
            try:
                next_batches.append(next(logical_batch_iter))
                if len(next_batches) == num_batches_to_prefetch:
                    yield next_batches
            except StopIteration:
                break
        
        if len(next_batches) > 0:
            yield next_batches
    
    # Fetch the initial set of batches.
    batch_iterator = get_next_batches()
    try:
        batches = next(batch_iterator)
    except StopIteration:
        return

    with stats.iter_wait_s.timer() if stats else nullcontext():
        block_refs = [block_ref for batch in batches for block_ref in batch.blocks_refs]
        prefetcher.prefetch_blocks(block_refs)
    
    for next_batches in batch_iterator:
        # Prefetch the next batches.
        with stats.iter_wait_s.timer() if stats else nullcontext():
            block_refs = [block_ref for batch in next_batches for block_ref in batch.blocks_refs]
            prefetcher.prefetch_blocks(block_refs)
        
        for batch in batches:
            yield batch
        
        batches = next_batches

    # Yield the final set of batches.
    for batch in batches:
        yield batch


def _resolve_blocks(
    logical_batch_iterator: Iterator[LogicalBatch],
    clear_block_after_read: bool = False, 
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[ResolvedLogicalBatch]:
    """Given an iterator of unresolved logical batches (as Ray object references), returns an iterator of resolved logical batches.

    Args:
        logical_batch_iterator: An iterator over logical batches.
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
    current_node_id = ray.get_runtime_context().get_node_id()
    for logical_batch in logical_batch_iterator:
        block_refs = logical_batch.block_refs

        stats_timer = stats.iter_get_s.timer() if stats else nullcontext()
        # Count the number of blocks that we hit locally or miss (so have to
        # fetch from remote node). This is to measure the effectiveness of
        # prefetch.
        locs = ray.experimental.get_object_locations(block_refs)
        nodes: List[List[str]] = [loc["node_ids"] for loc in locs.values()]
        known_nodes = [node_ids for node_ids in nodes if node_ids]
        unknown += len(nodes) - known_nodes
        hits = sum(current_node_id in node_ids for node_ids in nodes)
        hit += hits
        miss += (len(nodes) - hits)
        
        with stats_timer:
            blocks = ray.get(block_refs)

            # Trace the deallocations after resolving the blocks.
            for i in range(len(block_refs)):
                block_ref = block_refs[i]
                # Don't eagerly free the first block if stating_block_idx is set, since
                # this block may be in another logical batch.
                if i == 0 and logical_batch.starting_block_idx:
                    free = False
                # Don't eagerly free the last block if ending_block_idx is set, since
                # this block may be in another logical batch.
                elif i == len(block_refs) - 1 and logical_batch.ending_block_idx:
                    free = False
                else:
                    free = eager_free
                trace_deallocation(
                    block_ref, "block_batching._resolve_blocks", free=free
                )
            
        yield ResolvedLogicalBatch(
            batch_idx=logical_batch.batch_idx,
            blocks=blocks,
            starting_block_idx=logical_batch.starting_block_idx,
            ending_block_idx=logical_batch.ending_block_idx,
            num_rows=logical_batch.num_rows
        )

    if stats:
        stats.iter_blocks_local += hit
        stats.iter_blocks_remote += miss
        stats.iter_unknown_location += unknown

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

def _blocks_to_batches(
    resolved_logical_batch_iter: Iterator[ResolvedLogicalBatch],
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
    ensure_copy: bool = False,
) -> Iterator[Tuple[int, Block]]:
    """Given an iterator over logical batches, returns an iterator over actual constructed batches.

    Args:
        resolved_logical_batch_iter: An iterator over resolved logical batches.
        stats: Dataset stats object used to store block batching time.
        ensure_copy: Whether batches are always copied from the underlying base
            blocks (not zero-copy views).

    Returns:
        An iterator over batch index and batches of the given size.
    """
    def get_iter_next_batch_s_timer():
        return stats.iter_next_batch_s.timer() if stats else nullcontext()

    for logical_batch in resolved_logical_batch_iter:
        with get_iter_next_batch_s_timer():
            blocks = [BlockAccessor.for_block(block) for block in logical_batch.blocks]
            output = DelegatingBlockBuilder()

            # Perform slicing if necessary to get exact batches from the list of blocks.
            if logical_batch.starting_block_idx:
                blocks[0] = blocks[0].slice(logical_batch.starting_block_idx, blocks[0].num_rows(), copy=False)
            if logical_batch.ending_block_idx:
                blocks[-1] = blocks[-1].slice(0, logical_batch.ending_block_idx, copy=False)
            
            for block in blocks:
                output.add_block(block)
            
            batch = output.build()
            if ensure_copy:
                # Need to ensure that the batch is a fresh copy.
                batch = BlockAccessor.for_block(batch)
                batch = batch.slice(0, batch.num_rows(), copy=True)
        
        yield logical_batch.batch_idx, batch


def _format_batches(
    block_iter: Iterator[Tuple[int, Block]],
    batch_format: str,
    stats: Optional[Union[DatasetStats, DatasetPipelineStats]] = None,
) -> Iterator[Tuple[int, DataBatch]]:
    """Given an iterator of blocks, returns an iterator of formatted batches.

    Args:
        block_iter: An iterator over blocks.
        batch_format: The batch format to use.
        stats: An optional stats object to record formatting times.

    Returns:
        An iterator over batch index and the formatted batch.
    """
    for idx, block in block_iter:
        with stats.iter_format_batch_s.timer() if stats else nullcontext():
            batch = BlockAccessor.for_block(block).to_batch_format(batch_format)
        yield idx, batch

def _collate(batch_iter: Iterator[Tuple[int, DataBatch]], collate_fn: Optional[Callable[[DataBatch], Any]], stats: Optional[Union[DatasetStats, DatasetPipelineStats]]) -> Iterator[Tuple[int, Any]]:
    """Returns an iterator with the provided collate_fn applied to items of the batch iterator.

    Args:
        batch_iter: An iterator over formatted batches.
        stats: An optional stats object to record collation time.
    """
    for idx, batch in batch_iter:
        with stats.iter_collate_batch_s.timer() if stats else nullcontext():
            output = collate_fn(batch)
        yield idx, output

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
