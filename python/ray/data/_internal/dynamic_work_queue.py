import dataclasses
import itertools
import logging
import threading
from collections.abc import Generator
from typing import Any, Callable, Iterable, Optional, TypeVar

from ray.data._internal.util import _InterruptibleQueue

logger = logging.getLogger(__name__)

SENTINEL = object()
WorkItemT = TypeVar("WorkItemT")
ResultT = TypeVar("ResultT")


@dataclasses.dataclass(slots=True)
class _WorkerError:
    """Wraps an exception captured in a worker thread so that errors on the
    output queue are unambiguously distinguishable from legitimate result
    values."""

    exception: Optional[BaseException]


def _raise_if_error(item: Any) -> Any:
    """Re-raise *item* if it is a :class:`_WorkerError`, otherwise return it
    unchanged."""
    if isinstance(item, _WorkerError):
        exception = item.exception
        assert exception is not None
        item.exception = None
        raise exception
    return item


class _WorkerPool:
    """Lightweight helper that manages daemon worker threads sharing a common
    interrupt signal.

    Provides:

    - A shared ``threading.Event`` (``interrupted``) wired into the work and
      output queues.
    - Convenience methods to start and register daemon threads.
    - A single :meth:`shutdown` method that sets the interrupt flag,
      optionally drains queues with sentinels, and optionally joins threads.
    """

    _pool_counter = itertools.count()

    def __init__(self):
        self._prefix = f"pool-{next(self._pool_counter)}"
        self.interrupted = threading.Event()
        self._threads: list[threading.Thread] = []
        self.work_queue = _InterruptibleQueue(-1, self.interrupted)
        self.output_queue = _InterruptibleQueue(-1, self.interrupted)

    def start_thread(
        self,
        target: Callable,
        *,
        name: str = "worker",
        args: tuple = (),
    ) -> threading.Thread:
        """Start a daemon thread and register it for cleanup."""
        t = threading.Thread(
            target=target, args=args, name=f"{self._prefix}/{name}", daemon=True
        )
        self._threads.append(t)
        t.start()
        return t

    def shutdown(self, *, join_timeout: Optional[float] = None):
        """Signal all workers to stop.

        1. Sets the ``interrupted`` event so that blocking queue operations
           raise ``InterruptedError``.
        2. If *join_timeout* is not ``None``, joins each thread with the given
           timeout.
        """
        self.interrupted.set()
        if join_timeout is not None:
            for t in self._threads:
                t.join(timeout=join_timeout)


def _interruptible_join(
    q: _InterruptibleQueue,
    interrupted: threading.Event,
    poll_interval: float = 0.5,
) -> bool:
    """Like ``q.join()`` but periodically checks *interrupted* so the
    thread can exit promptly during shutdown instead of blocking
    forever on the non-interruptible ``Queue.join()``.

    Args:
        q: The queue to join.
        interrupted: Event that when set signals shutdown.
        poll_interval: Seconds to wait between checks.

    Returns:
        True if all tasks finished.
        False if shutdown was requested before finishing.
    """
    with q.all_tasks_done:
        while q.unfinished_tasks:
            if interrupted.is_set():
                return False
            q.all_tasks_done.wait(timeout=poll_interval)
    return True


def _worker(
    pool: _WorkerPool,
    process_fn: Callable[
        [WorkItemT, Callable[[WorkItemT], None], Callable[[ResultT], None]], None
    ],
) -> None:
    try:
        while True:
            item = pool.work_queue.get()
            if item is SENTINEL:
                pool.work_queue.task_done()
                break
            try:
                # Thread-safe callbacks passed to process_fn.  Workers
                # enqueue work themselves (rather than returning it to a
                # central coordinator) so that newly discovered items can
                # be picked up immediately by any idle worker for better
                # load distribution.
                process_fn(
                    item,
                    pool.work_queue.put,
                    pool.output_queue.put,
                )
            except InterruptedError:
                break
            except BaseException as e:
                pool.output_queue.put(_WorkerError(e))
            finally:
                pool.work_queue.task_done()
    except InterruptedError:
        # Expected during shutdown: pool.interrupted is set, so blocking
        # queue operations raise.  Exit quietly; the finally block still
        # runs for logging.
        pass
    finally:
        logger.debug(
            "Worker %s exited",
            threading.current_thread().name,
        )


def _signal_completion(pool: _WorkerPool, num_workers: int) -> None:
    # Join = detect when all work is done.  SENTINEL = tell workers to stop.
    #
    #   join(work_queue)     -->  block until unfinished_tasks == 0
    #   blocks here              (all results already in output_queue)
    #         |
    #         v
    #   output_queue.put(S)  -->  tell main thread: no more results
    #         |
    #         v
    #   work_queue.put(S)*N -->  tell each worker: exit
    #
    # Why it works: the main thread uses iter(output_queue.get, SENTINEL);
    # workers use iter(work_queue.get, SENTINEL).  We send 1 output
    # SENTINEL (for main) and num_workers work SENTINELs (one per worker).
    # Order: join first (determines when all work is done), then output
    # SENTINEL (tells main to stop consuming), then work SENTINELs
    # (tell each worker to stop producing).
    #
    if not _interruptible_join(pool.work_queue, pool.interrupted):
        return
    # Stop consumers
    pool.output_queue.put(SENTINEL)
    for _ in range(num_workers):
        # Stop producers
        pool.work_queue.put(SENTINEL)


def parallel_process_work_stealing(
    seed_items: Iterable[WorkItemT],
    process_fn: Callable[
        [WorkItemT, Callable[[WorkItemT], None], Callable[[ResultT], None]], None
    ],
    num_workers: int = 1,
    preserve_order: bool = False,
    order_key: Optional[Callable[[ResultT], Any]] = None,
) -> Generator[ResultT, None, None]:
    """Returns a generator that processes work items in parallel using a shared
    work queue with dynamic load balancing (work stealing).

    Unlike ``make_async_gen``, which uses simple round-robin distribution with
    goals of a) maintaining order and b) limiting memory use, this utility uses
    work-stealing for better load distribution (faster processing).  It supports
    *dynamic* work generation: workers can enqueue new work items that any
    available worker may pick up, with no limit on memory use.  Suitable for
    graph/tree traversal patterns (e.g., recursive directory listing) where the
    total work set is discovered at runtime.

    Data flow::

        seed_items -----> [ work_queue ] <--+
                               |            |
                    +----------+----------+ |
                    |          |          | |
                worker_0  worker_1  ... worker_N
                    |          |          |
                    |   process_fn(item, add_work, add_result)
                    |     /          \
                    |  add_work()  add_result()
                    |     |            |
                    |     +---> work_queue (new work, feeds back)
                    |                  |
                    +--------> [ output_queue ]
                                       |
                            completion signaler
                           (join work_queue, then
                            send SENTINEL)
                                       |
                                       v
                              main thread drains
                             output_queue & yields

    NOTE: There are some important constraints that need to be carefully
          understood before using this method:

        1. If ``preserve_order`` is True
            a. **All** results are buffered in memory before yielding so that
               they can be sorted by ``order_key(result)``.
            b. ``order_key`` is required in this mode.

        2. If ``preserve_order`` is False
            a. Results are yielded as soon as they are produced (no buffering).
            b. Resulting ordering is unspecified and non-deterministic.

    Args:
        seed_items: Initial work items to enqueue.
        process_fn: Called for each work item with the signature
            ``process_fn(item, add_work, add_result)`` where:

            - ``item`` is the work item to process.
            - ``add_work(new_item)`` enqueues a new work item.
            - ``add_result(result)`` produces an output item.

            Exceptions raised by ``process_fn`` are propagated to the
            consuming thread with their original tracebacks.
        num_workers: Number of worker threads (must be >= 1).
        preserve_order: Whether to buffer and sort results before yielding.
        order_key: Sort key function applied to each result for ordering.
            Required when ``preserve_order`` is True.

    Yields:
        ResultT: Result items produced by ``process_fn`` via ``add_result``.
    """
    if num_workers < 1:
        raise ValueError("num_workers must be at least 1.")
    if preserve_order and order_key is None:
        raise ValueError("order_key is required when preserve_order is True.")

    pool = _WorkerPool()

    for item in seed_items:
        pool.work_queue.put(item)

    # Drain output queue on the calling thread.
    #
    # Thread creation is inside the try/finally so that pool.shutdown() is
    # always reached even when starting the Nth thread raises (e.g. OSError
    # from hitting the OS thread limit).
    try:
        for _ in range(num_workers):
            pool.start_thread(target=_worker, args=(pool, process_fn))

        # Separate thread needed: _signal_completion blocks on work_queue.join()
        # until all dynamic work is done.  The main thread must drain
        # output_queue and yield; it cannot block waiting for work completion.
        pool.start_thread(
            target=_signal_completion,
            args=(pool, num_workers),
            name="completion-signaler",
        )

        if preserve_order:
            assert order_key is not None
            results: list[ResultT] = []
            for item in iter(pool.output_queue.get, SENTINEL):
                _raise_if_error(item)
                results.append(item)
            results.sort(key=order_key)
            yield from results
        else:
            for item in iter(pool.output_queue.get, SENTINEL):
                _raise_if_error(item)
                yield item
    finally:
        pool.shutdown(join_timeout=2)
