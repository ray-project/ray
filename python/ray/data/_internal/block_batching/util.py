import logging
import queue
import threading
import sys
from typing import Any, Callable, Iterator, List, Optional, Tuple, TypeVar, Union

import ray
from ray.types import ObjectRef
from ray.actor import ActorHandle
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data._internal.batcher import Batcher, ShufflingBatcher
from ray.data._internal.block_batching.interfaces import (
    Batch,
    CollatedBatch,
    BlockPrefetcher,
)
from ray.data._internal.stats import DatasetPipelineStats, DatasetStats
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

T = TypeVar("T")
U = TypeVar("U")

logger = logging.getLogger(__name__)

if sys.version_info >= (3, 7):
    from contextlib import nullcontext
else:
    from contextlib import contextmanager

    @contextmanager
    def nullcontext(enter_result=None):
        yield enter_result


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
    """
    for batch in batch_iter:
        with stats.iter_collate_batch_s.timer() if stats else nullcontext():
            collated_batch = collate_fn(batch.data)
        yield CollatedBatch(batch.batch_idx, collated_batch)


def extract_data_from_batch(batch_iter: Iterator[Batch]) -> Iterator[Any]:
    for batch in batch_iter:
        yield batch.data


def make_async_gen(
    base_iterator: Iterator[T],
    fn: Callable[[Iterator[T]], Iterator[U]],
    num_workers: int = 1,
) -> Iterator[U]:
    """Returns a new iterator with elements fetched from the base_iterator
    in an async fashion using a threadpool.

    Each thread in the threadpool will fetch data from the base_iterator in a
    thread-safe fashion, and apply the provided `fn` computation concurrently.

    Args:
        base_iterator: The iterator to asynchronously fetch from.
        fn: The function to run on the input iterator.
        num_workers: The number of threads to use in the threadpool. Defaults to 1.

    Returns:
        An iterator with the same elements as outputted from `fn`.
    """

    if num_workers < 1:
        raise ValueError("Size of threadpool must be at least 1.")

    # Use a lock to fetch from the base_iterator in a thread-safe fashion.
    def convert_to_threadsafe_iterator(base_iterator: Iterator[T]) -> Iterator[T]:
        class ThreadSafeIterator:
            def __init__(self, it):
                self.lock = threading.Lock()
                self.it = it

            def __next__(self):
                with self.lock:
                    return next(self.it)

            def __iter__(self):
                return self

        return ThreadSafeIterator(base_iterator)

    thread_safe_generator = convert_to_threadsafe_iterator(base_iterator)

    class Sentinel:
        def __init__(self, thread_index: int):
            self.thread_index = thread_index

    output_queue = queue.Queue(1)

    # Because pulling from the base iterator cannot happen concurrently,
    # we must execute the expensive computation in a separate step which
    # can be parallelized via a threadpool.
    def execute_computation(thread_index: int):
        try:
            for item in fn(thread_safe_generator):
                output_queue.put(item, block=True)
            output_queue.put(Sentinel(thread_index), block=True)
        except Exception as e:
            output_queue.put(e, block=True)

    threads = [
        threading.Thread(target=execute_computation, args=(i,), daemon=True)
        for i in range(num_workers)
    ]

    for thread in threads:
        thread.start()

    num_threads_finished = 0
    while True:
        next_item = output_queue.get(block=True)
        if isinstance(next_item, Exception):
            output_queue.task_done()
            raise next_item
        if isinstance(next_item, Sentinel):
            output_queue.task_done()
            logger.debug(f"Thread {next_item.thread_index} finished.")
            num_threads_finished += 1
            threads[next_item.thread_index].join()
        else:
            yield next_item
            output_queue.task_done()
        if num_threads_finished >= num_workers:
            break


PREFETCHER_ACTOR_NAMESPACE = "ray.dataset"


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
        node_id = ray.get_runtime_context().get_node_id()
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
