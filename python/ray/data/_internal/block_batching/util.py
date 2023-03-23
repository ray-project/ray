import logging
import queue
import threading
from typing import Any, Callable, Iterator, List, Tuple, TypeVar

import ray
from ray.types import ObjectRef
from ray.actor import ActorHandle
from ray.data.block import Block
from ray.data._internal.block_batching.interfaces import BlockPrefetcher
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

T = TypeVar("T")
U = TypeVar("U")

logger = logging.getLogger(__name__)


def _make_async_gen(
    base_iterator: Iterator[T],
    fn: Callable[[Iterator[T]], Iterator[U]],
    num_workers: int = 1,
) -> Iterator[U]:
    """Returns a new iterator with elements fetched from the base_iterator
    in an async fashion using a threadpool.
    Each thread in the threadpool will fetch data from the base_iterator in a
    thread-safe fashion, and apply the provided computation.  triggering the base
    iterator's execution.

    Args:
        base_iterator: The iterator to asynchronously fetch from.
        fn: The function to run on the input iterator.
        num_workers: The number of threads to use in the threadpool. Defaults to 1.

    Returns:
        An iterator with the same elements as the base_iterator.
    """

    # If no threadpool workers are specified, then don't use a threadpool.
    if num_workers <= 0:
        yield from fn(base_iterator)
        return

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
