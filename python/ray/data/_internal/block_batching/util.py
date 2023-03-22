import logging
import queue
import threading
from typing import Callable, Iterator, TypeVar

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
    thread-safe fashion, and apply the provided `fn` computation concurrently.

    Args:
        base_iterator: The iterator to asynchronously fetch from.
        fn: The function to run on the input iterator.
        num_workers: The number of threads to use in the threadpool.

    Returns:
        An iterator with the same elements as outputted from `fn`.
    """

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
            output_queue.join()
            break
