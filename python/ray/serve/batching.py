import asyncio
import io
import logging
import time
from collections import deque
from dataclasses import dataclass
from functools import wraps
from inspect import isasyncgenfunction, iscoroutinefunction
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
    overload,
)

from ray import serve
from ray._common.signature import extract_signature, flatten_args, recover_args
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.constants import (
    BATCH_EXECUTION_TIME_BUCKETS_MS,
    BATCH_SIZE_BUCKETS,
    BATCH_UTILIZATION_BUCKETS_PERCENT,
    BATCH_WAIT_TIME_BUCKETS_MS,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.utils import extract_self_if_method_call
from ray.serve.exceptions import RayServeException
from ray.serve.metrics import Counter, Gauge, Histogram
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


# The user can return these values in their streaming batch handler function to
# indicate that a request is finished, so Serve can terminate the request.
USER_CODE_STREAMING_SENTINELS = [StopIteration, StopAsyncIteration]


@dataclass
class _SingleRequest:
    self_arg: Any
    flattened_args: List[Any]
    future: asyncio.Future
    request_context: serve.context._RequestContext


@dataclass
class _GeneratorResult:
    result: Any
    next_future: asyncio.Future


@dataclass
class _RuntimeSummaryStatistics:
    start_times: List[float]

    @property
    def min_start_time(self) -> Optional[float]:
        return min(self.start_times) if self.start_times else None

    @property
    def mean_start_time(self) -> Optional[float]:
        return (
            sum(self.start_times) / len(self.start_times) if self.start_times else None
        )

    @property
    def max_start_time(self) -> Optional[float]:
        return max(self.start_times) if self.start_times else None

    @property
    def num_requests(self) -> int:
        return len(self.start_times)


def _batch_args_kwargs(
    list_of_flattened_args: List[List[Any]],
) -> Tuple[Tuple[Any], Dict[Any, Any]]:
    """Batch a list of flatten args and returns regular args and kwargs"""
    # Ray's flatten arg format is a list with alternating key and values
    # e.g. args=(1, 2), kwargs={"key": "val"} got turned into
    #      [None, 1, None, 2, "key", "val"]
    arg_lengths = {len(args) for args in list_of_flattened_args}
    assert (
        len(arg_lengths) == 1
    ), "All batch requests should have the same number of parameters."
    arg_length = arg_lengths.pop()

    batched_flattened_args = []
    for idx in range(arg_length):
        if idx % 2 == 0:
            batched_flattened_args.append(list_of_flattened_args[0][idx])
        else:
            batched_flattened_args.append(
                [item[idx] for item in list_of_flattened_args]
            )

    return recover_args(batched_flattened_args)


class _BatchQueue:
    def __init__(
        self,
        max_batch_size: int,
        batch_wait_timeout_s: float,
        max_concurrent_batches: int,
        handle_batch_func: Optional[Callable] = None,
        batch_size_fn: Optional[Callable[[List], int]] = None,
    ) -> None:
        """Async queue that accepts individual items and returns batches.

        Respects max_batch_size and batch_wait_timeout_s; a batch will be returned when
        max_batch_size elements are available or the timeout has passed since
        the previous get.

        If handle_batch_func is passed in, a background coroutine will run to
        poll from the queue and call handle_batch_func on the results.

        Cannot be pickled.

        Arguments:
            max_batch_size: max number of elements to return in a batch.
            batch_wait_timeout_s: time to wait before returning an incomplete
                batch.
            max_concurrent_batches: max number of batches to run concurrently.
            handle_batch_func(Optional[Callable]): callback to run in the
                background to handle batches if provided.
            batch_size_fn(Optional[Callable[[List], int]]): optional function to
                compute the effective batch size. If None, uses len(batch).
                The function takes a list of requests and returns an integer
                representing the batch size. This is useful for batching based
                on custom metrics such as total nodes in graphs, total tokens
                in sequences, etc.
        """
        self.queue: asyncio.Queue[_SingleRequest] = asyncio.Queue()
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.max_concurrent_batches = max_concurrent_batches
        self.batch_size_fn = batch_size_fn
        self.semaphore = asyncio.Semaphore(max_concurrent_batches)
        self.requests_available_event = asyncio.Event()
        self.tasks: Set[asyncio.Task] = set()

        # Used for observability.
        self.curr_iteration_start_times: Dict[asyncio.Task, float] = {}

        # Initialize batching metrics.
        self._batch_wait_time_histogram = Histogram(
            "serve_batch_wait_time_ms",
            description="Time requests waited for batch to fill (in milliseconds).",
            boundaries=BATCH_WAIT_TIME_BUCKETS_MS,
            tag_keys=("function_name",),
        )
        self._batch_execution_time_histogram = Histogram(
            "serve_batch_execution_time_ms",
            description="Time to execute the batch function (in milliseconds).",
            boundaries=BATCH_EXECUTION_TIME_BUCKETS_MS,
            tag_keys=("function_name",),
        )
        self._batch_queue_length_gauge = Gauge(
            "serve_batch_queue_length",
            description="Number of requests waiting in the batch queue.",
            tag_keys=("function_name",),
        )
        self._batch_utilization_histogram = Histogram(
            "serve_batch_utilization_percent",
            description="Batch utilization as percentage (actual_batch_size / max_batch_size * 100).",
            boundaries=BATCH_UTILIZATION_BUCKETS_PERCENT,
            tag_keys=("function_name",),
        )
        self._batch_size_histogram = Histogram(
            "serve_actual_batch_size",
            description="The actual number of requests in each batch.",
            boundaries=BATCH_SIZE_BUCKETS,
            tag_keys=("function_name",),
        )
        self._batches_processed_counter = Counter(
            "serve_batches_processed",
            description="Counter of batches executed.",
            tag_keys=("function_name",),
        )

        self._function_name = (
            handle_batch_func.__name__ if handle_batch_func is not None else "unknown"
        )

        self._handle_batch_task = None
        self._loop = get_or_create_event_loop()
        if handle_batch_func is not None:
            self._handle_batch_task = self._loop.create_task(
                self._process_batches(handle_batch_func)
            )
        self._warn_if_max_batch_size_exceeds_max_ongoing_requests()

    def _warn_if_max_batch_size_exceeds_max_ongoing_requests(self):
        """Helper to check whether the max_batch_size is bounded.

        Log a warning to configure `max_ongoing_requests` if it's bounded.
        """
        max_ongoing_requests = (
            serve.get_replica_context()._deployment_config.max_ongoing_requests
        )
        if max_ongoing_requests < self.max_batch_size * self.max_concurrent_batches:
            logger.warning(
                f"`max_batch_size` ({self.max_batch_size}) * `max_concurrent_batches` "
                f"({self.max_concurrent_batches}) is larger than `max_ongoing_requests` "
                f"({max_ongoing_requests}). This means the replica will never achieve "
                "the configured `max_batch_size` concurrently. Please update "
                "`max_ongoing_requests` to be >= `max_batch_size` * `max_concurrent_batches`."
            )

    def set_max_batch_size(self, new_max_batch_size: int) -> None:
        """Updates queue's max_batch_size."""
        self.max_batch_size = new_max_batch_size
        self._warn_if_max_batch_size_exceeds_max_ongoing_requests()

    def put(self, request: Tuple[_SingleRequest, asyncio.Future]) -> None:
        self.queue.put_nowait(request)
        self.requests_available_event.set()

    def _compute_batch_size(self, batch: List[_SingleRequest]) -> int:
        """Compute the effective batch size using batch_size_fn or len()."""
        if self.batch_size_fn is None:
            return len(batch)

        # Extract the actual data items from requests to pass to batch_size_fn.
        # We need to reconstruct the original arguments from flattened_args.
        items = []
        for request in batch:
            # Recover the original arguments from flattened format
            args, kwargs = recover_args(request.flattened_args)
            # The batch function expects a single positional argument (the item)
            # after 'self' has been extracted (if it was a method)
            items.append(args[0])

        return self.batch_size_fn(items)

    async def wait_for_batch(self) -> Tuple[List[_SingleRequest], int]:
        """Wait for batch respecting self.max_batch_size and self.timeout_s.

        Returns a tuple of (batch, computed_batch_size) where batch contains
        up to self.max_batch_size items. Waits for up to self.timeout_s after
        receiving the first request that will be in the next batch. After the
        timeout, returns as many items as are ready.

        Always returns a batch with at least one item - will block
        indefinitely until an item comes in.
        """

        batch = []
        first_item = await self.queue.get()  # Block until first item arrives

        # Cache current max_batch_size and batch_wait_timeout_s for this batch.
        max_batch_size = self.max_batch_size
        batch_wait_timeout_s = self.batch_wait_timeout_s

        # Check if first item alone exceeds max_batch_size (only with batch_size_fn)
        if self.batch_size_fn is not None:
            first_item_size = self._compute_batch_size([first_item])
            if first_item_size > max_batch_size:
                exc = RuntimeError(
                    "Size of item is greater than max_batch_size. "
                    "Please increase the max_batch_size or check the "
                    "implementation of the batch_size_fn."
                )
                # Set exception on the future so the caller receives it
                first_item.future.set_exception(exc)
                return [], 0

        batch.append(first_item)

        # Wait self.timeout_s seconds for new queue arrivals.
        batch_start_time = time.time()
        while True:
            # Record queue length metric.
            self._batch_queue_length_gauge.set(
                self.queue.qsize(), tags={"function_name": self._function_name}
            )

            remaining_batch_time_s = max(
                batch_wait_timeout_s - (time.time() - batch_start_time), 0
            )
            try:
                # Wait for new arrivals.
                await asyncio.wait_for(
                    self.requests_available_event.wait(), remaining_batch_time_s
                )
            except asyncio.TimeoutError:
                pass

            # Custom batch size function logic
            if self.batch_size_fn is not None:
                # Add all new arrivals to the batch.
                # Track items we need to put back if they don't fit
                deferred_item = None
                while not self.queue.empty():
                    next_item = self.queue.get_nowait()
                    # Temporarily add to check size
                    batch.append(next_item)
                    new_size = self._compute_batch_size(batch)

                    if new_size > max_batch_size:
                        # Would exceed limit, remove it and save for later
                        batch.pop()
                        deferred_item = next_item
                        break
                    # Size is OK, keep it in the batch (already added above)

                # Put deferred item back in queue for next batch
                if deferred_item is not None:
                    # NOTE: The deferred item goes to the back of the queue (FIFO),
                    # so newer requests may be processed before it. Consider using
                    # asyncio.PriorityQueue if strict ordering is required.
                    self.queue.put_nowait(deferred_item)
                    # Compute final batch size before breaking (batch is now valid
                    # after popping the deferred item).
                    current_batch_size = self._compute_batch_size(batch)
                    # break the loop early because the deferred item is too large to fit in the batch
                    break
            else:
                # Default behavior: use original len() check logic
                while len(batch) < max_batch_size and not self.queue.empty():
                    batch.append(self.queue.get_nowait())

            # Only clear the put event if the queue is empty. If it's not empty
            # we can start constructing a new batch immediately in the next loop.
            # The code that puts items into the queue runs on the same event loop
            # as this code, so there's no race condition between the time we
            # get objects in the queue (and clear the event) and when objects
            # get added to the queue.
            if self.queue.empty():
                self.requests_available_event.clear()

            current_batch_size = self._compute_batch_size(batch)
            if (
                time.time() - batch_start_time >= batch_wait_timeout_s
                or current_batch_size >= max_batch_size
            ):
                break

        # Record batch wait time metric (time spent waiting for batch to fill).
        batch_wait_time_ms = (time.time() - batch_start_time) * 1000
        self._batch_wait_time_histogram.observe(
            batch_wait_time_ms, tags={"function_name": self._function_name}
        )

        return batch, current_batch_size

    def _validate_results(
        self, results: Iterable[Any], input_batch_length: int
    ) -> None:
        if len(results) != input_batch_length:
            raise RayServeException(
                "Batched function doesn't preserve batch size. "
                f"The input list has length {input_batch_length} but the "
                f"returned list has length {len(results)}."
            )

    async def _consume_func_generator(
        self,
        func_generator: AsyncGenerator,
        initial_futures: List[asyncio.Future],
        input_batch_length: int,
    ) -> None:
        """Consumes batch function generator.

        This function only runs if the function decorated with @serve.batch
        is a generator.
        """

        FINISHED_TOKEN = None

        try:
            futures = deque(initial_futures)
            assert len(futures) == input_batch_length

            async for results in func_generator:
                self._validate_results(results, input_batch_length)
                for idx in range(input_batch_length):
                    result, future = results[idx], futures[0]

                    if future is FINISHED_TOKEN:
                        # This caller has already terminated.
                        futures.append(FINISHED_TOKEN)
                    elif result in USER_CODE_STREAMING_SENTINELS:
                        # User's code returned sentinel. No values left
                        # for caller. Terminate iteration for caller.
                        _set_exception_if_not_done(future, StopAsyncIteration)
                        futures.append(FINISHED_TOKEN)
                    else:
                        next_future = get_or_create_event_loop().create_future()
                        _set_result_if_not_done(
                            future, _GeneratorResult(result, next_future)
                        )
                        futures.append(next_future)

                    # Remove processed future. We remove the future at the very
                    # end of the loop to ensure that if an exception occurs,
                    # all pending futures will get set in the `except` block.
                    futures.popleft()

            for future in futures:
                if future is not FINISHED_TOKEN:
                    _set_exception_if_not_done(future, StopAsyncIteration)
        except Exception as e:
            for future in futures:
                if future is not FINISHED_TOKEN:
                    _set_exception_if_not_done(future, e)

    async def _assign_func_results(
        self,
        func_future: asyncio.Future,
        futures: List[asyncio.Future],
        input_batch_length: int,
    ):
        """Assigns func's results to the list of futures."""

        try:
            results = await func_future
            self._validate_results(results, input_batch_length)
            for result, future in zip(results, futures):
                _set_result_if_not_done(future, result)
        except Exception as e:
            for future in futures:
                _set_exception_if_not_done(future, e)

    def _split_batch_by_model_id(
        self, batch: List[_SingleRequest]
    ) -> List[List[_SingleRequest]]:
        """Split a batch into sub-batches based on multiplexed_model_id.

        When using model multiplexing with batching, requests for different models
        may end up in the same batch. This method ensures that each sub-batch only
        contains requests for the same model, preventing issues where a single batch
        contains requests for different models.

        If no requests have a multiplexed_model_id set, returns the original batch
        as a single sub-batch.

        Args:
            batch: The batch of requests to split.

        Returns:
            A list of sub-batches, where each sub-batch contains requests for the
            same multiplexed_model_id.
        """
        # Group requests by their multiplexed_model_id
        model_id_to_requests: Dict[str, List[_SingleRequest]] = {}
        for request in batch:
            model_id = request.request_context.multiplexed_model_id
            if model_id not in model_id_to_requests:
                model_id_to_requests[model_id] = []
            model_id_to_requests[model_id].append(request)

        # Return sub-batches for each model_id
        return list(model_id_to_requests.values())

    async def _process_batches(self, func: Callable) -> None:
        """Loops infinitely and processes queued request batches."""
        # When asyncio task is created, the task will inherit the request context from the current context.
        # So we unset the request context so the current context is not inherited by the task, _process_batch.
        serve.context._unset_request_context()
        while not self._loop.is_closed():
            batch, _ = await self.wait_for_batch()

            # Split batch by multiplexed_model_id to ensure requests for different
            # models are processed in separate batches. This is necessary when using
            # model multiplexing with batching, as a single batch containing requests
            # for different models would not work correctly.
            sub_batches = self._split_batch_by_model_id(batch)

            # Process all sub-batches together under a single semaphore permit.
            # This ensures sub-batches from the same original batch run concurrently
            # rather than being serialized by the semaphore.
            promise = self._process_sub_batches(func, sub_batches)
            task = asyncio.create_task(promise)
            self.tasks.add(task)
            self.curr_iteration_start_times[task] = time.time()
            task.add_done_callback(self._handle_completed_task)

    async def _process_sub_batches(
        self, func: Callable, sub_batches: List[List[_SingleRequest]]
    ) -> None:
        """Processes multiple sub-batches concurrently under a single semaphore permit.

        This method acquires the semaphore once and then processes all sub-batches
        in parallel, ensuring that sub-batches from the same original batch don't
        compete for semaphore permits.
        """
        # NOTE: this semaphore caps the number of concurrent batches specified by `max_concurrent_batches`
        async with self.semaphore:
            # Create tasks for each sub-batch. We use asyncio.create_task() instead
            # of passing coroutines directly to asyncio.gather() because create_task
            # copies the current context, giving each sub-batch its own isolated
            # contextvars. This prevents concurrent sub-batches from overwriting
            # each other's _serve_batch_request_context, which would cause
            # get_multiplexed_model_id() to return wrong values.
            tasks = [
                asyncio.create_task(self._process_batch_inner(func, sub_batch))
                for sub_batch in sub_batches
            ]
            await asyncio.gather(*tasks)

    async def _process_batch_inner(
        self, func: Callable, batch: List[_SingleRequest]
    ) -> None:
        """Processes a single batch without acquiring the semaphore.

        This is the inner implementation called by _process_sub_batches after
        the semaphore has already been acquired.
        """
        # Remove requests that have been cancelled from the batch. If
        # all requests have been cancelled, simply return and wait for
        # the next batch.
        batch = [req for req in batch if not req.future.cancelled()]
        if len(batch) == 0:
            return

        # Compute batch size for this sub-batch. Each sub-batch may have a different
        # size, especially when splitting by model_id, so we compute it here.
        computed_batch_size = self._compute_batch_size(batch)

        # Calculate and record batch utilization percentage.
        batch_utilization_percent = (computed_batch_size / self.max_batch_size) * 100
        self._batch_utilization_histogram.observe(
            batch_utilization_percent, tags={"function_name": self._function_name}
        )

        # Record actual batch size (number of requests in the batch computed by the batch_size_fn).
        self._batch_size_histogram.observe(
            computed_batch_size, tags={"function_name": self._function_name}
        )

        # Increment batches processed counter.
        self._batches_processed_counter.inc(tags={"function_name": self._function_name})

        futures = [item.future for item in batch]

        # Most of the logic in the function should be wrapped in this try-
        # except block, so the futures' exceptions can be set if an exception
        # occurs. Otherwise, the futures' requests may hang indefinitely.
        batch_execution_start_time = time.time()
        try:
            self_arg = batch[0].self_arg
            args, kwargs = _batch_args_kwargs([item.flattened_args for item in batch])

            # Method call.
            if self_arg is not None:
                func_future_or_generator = func(self_arg, *args, **kwargs)
            # Normal function call.
            else:
                func_future_or_generator = func(*args, **kwargs)

            # Add individual request context to the batch request context
            serve.context._set_batch_request_context(
                [req.request_context for req in batch]
            )

            if isasyncgenfunction(func):
                func_generator = func_future_or_generator
                await self._consume_func_generator(func_generator, futures, len(batch))
            else:
                func_future = func_future_or_generator
                await self._assign_func_results(func_future, futures, len(batch))

            # Reset the batch request context after the batch is processed
            serve.context._set_batch_request_context([])
        except Exception as e:
            logger.exception("_process_batch ran into an unexpected exception.")

            for future in futures:
                _set_exception_if_not_done(future, e)
        finally:
            # Record batch execution time.
            batch_execution_time_ms = (time.time() - batch_execution_start_time) * 1000
            self._batch_execution_time_histogram.observe(
                batch_execution_time_ms, tags={"function_name": self._function_name}
            )

    def _handle_completed_task(self, task: asyncio.Task) -> None:
        self.tasks.remove(task)
        del self.curr_iteration_start_times[task]
        self._log_if_exception(task.exception())

    @staticmethod
    def _log_if_exception(exception_maybe: Optional[BaseException]) -> None:
        if exception_maybe is not None:
            if isinstance(exception_maybe, asyncio.CancelledError):
                logger.debug("Task was cancelled")
            else:
                logger.exception("Task failed unexpectedly")

    def __del__(self):
        if (
            self._handle_batch_task is None
            or not get_or_create_event_loop().is_running()
        ):
            return

        # TODO(edoakes): although we try to gracefully shutdown here, it still
        # causes some errors when the process exits due to the asyncio loop
        # already being destroyed.
        self._handle_batch_task.cancel()


class _LazyBatchQueueWrapper:
    """Stores a _BatchQueue and updates its settings.

    _BatchQueue cannot be pickled, you must construct it lazily
    at runtime inside a replica. This class initializes a queue only upon
    first access.
    """

    def __init__(
        self,
        max_batch_size: int = 10,
        batch_wait_timeout_s: float = 0.0,
        max_concurrent_batches: int = 1,
        handle_batch_func: Optional[Callable] = None,
        batch_size_fn: Optional[Callable[[List], int]] = None,
    ):
        self._queue: Optional[_BatchQueue] = None
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.max_concurrent_batches = max_concurrent_batches
        self.handle_batch_func = handle_batch_func
        self.batch_size_fn = batch_size_fn

    @property
    def queue(self) -> _BatchQueue:
        """Returns _BatchQueue.

        Initializes queue when called for the first time.
        """
        if self._queue is None:
            self._queue = _BatchQueue(
                self.max_batch_size,
                self.batch_wait_timeout_s,
                self.max_concurrent_batches,
                self.handle_batch_func,
                self.batch_size_fn,
            )
        return self._queue

    def set_max_batch_size(self, new_max_batch_size: int) -> None:
        """Updates queue's max_batch_size."""

        self.max_batch_size = new_max_batch_size

        if self._queue is not None:
            self._queue.set_max_batch_size(new_max_batch_size)

    def set_batch_wait_timeout_s(self, new_batch_wait_timeout_s: float) -> None:
        self.batch_wait_timeout_s = new_batch_wait_timeout_s

        if self._queue is not None:
            self._queue.batch_wait_timeout_s = new_batch_wait_timeout_s

    def get_max_batch_size(self) -> int:
        return self.max_batch_size

    def get_batch_wait_timeout_s(self) -> float:
        return self.batch_wait_timeout_s

    def _get_curr_iteration_start_times(self) -> _RuntimeSummaryStatistics:
        """Gets summary statistics of current iteration's start times."""
        return _RuntimeSummaryStatistics(
            list(self.queue.curr_iteration_start_times.values())
        )

    async def _is_batching_task_alive(self) -> bool:
        """Gets whether default _BatchQueue's background task is alive.

        Returns False if the batch handler doesn't use a default _BatchQueue.
        """

        if hasattr(self.queue, "_handle_batch_task"):
            return not self.queue._handle_batch_task.done()
        else:
            return False

    async def _get_handling_task_stack(self) -> Optional[str]:
        """Gets the stack for the default _BatchQueue's background task.

        Returns empty string if the batch handler doesn't use a default _BatchQueue.
        """

        if hasattr(self.queue, "_handle_batch_task"):
            str_buffer = io.StringIO()
            self.queue._handle_batch_task.print_stack(file=str_buffer)
            return str_buffer.getvalue()
        else:
            return None


def _validate_max_batch_size(max_batch_size):
    if not isinstance(max_batch_size, int):
        if isinstance(max_batch_size, float) and max_batch_size.is_integer():
            max_batch_size = int(max_batch_size)
        else:
            raise TypeError(
                f"max_batch_size must be integer >= 1, got {max_batch_size}"
            )

    if max_batch_size < 1:
        raise ValueError(
            f"max_batch_size must be an integer >= 1, got {max_batch_size}"
        )


def _validate_batch_wait_timeout_s(batch_wait_timeout_s):
    if not isinstance(batch_wait_timeout_s, (float, int)):
        raise TypeError(
            f"batch_wait_timeout_s must be a float >= 0, got {batch_wait_timeout_s}"
        )

    if batch_wait_timeout_s < 0:
        raise ValueError(
            f"batch_wait_timeout_s must be a float >= 0, got {batch_wait_timeout_s}"
        )


def _validate_max_concurrent_batches(max_concurrent_batches: int) -> None:
    if not isinstance(max_concurrent_batches, int) or max_concurrent_batches < 1:
        raise TypeError(
            f"max_concurrent_batches must be an integer >= 1, got {max_concurrent_batches}"
        )


def _validate_batch_size_fn(batch_size_fn: Optional[Callable[[List], int]]) -> None:
    if batch_size_fn is not None and not callable(batch_size_fn):
        raise TypeError(
            f"batch_size_fn must be a callable or None, got {type(batch_size_fn)}"
        )


SelfType = TypeVar("SelfType", contravariant=True)
T = TypeVar("T")
R = TypeVar("R")


class _SyncBatchingMethod(Protocol, Generic[SelfType, T, R]):
    def __call__(self, self_: SelfType, __batch: List[T], /) -> List[R]:
        ...


class _AsyncBatchingMethod(Protocol, Generic[SelfType, T, R]):
    async def __call__(self, self_: SelfType, __batch: List[T], /) -> List[R]:
        ...


@overload  # Sync function for `batch` called WITHOUT arguments
def batch(_sync_func: Callable[[List[T]], List[R]], /) -> Callable[[T], R]:
    ...


@overload  # Async function for `batch` called WITHOUT arguments
def batch(
    _async_func: Callable[[List[T]], Coroutine[Any, Any, List[R]]], /
) -> Callable[[T], Coroutine[Any, Any, R]]:
    ...


@overload  # Sync method for `batch` called WITHOUT arguments
def batch(
    _sync_meth: _SyncBatchingMethod[SelfType, T, R], /
) -> Callable[[SelfType, T], R]:
    ...


@overload  # Async method for `batch` called WITHOUT arguments
def batch(
    _async_meth: _AsyncBatchingMethod[SelfType, T, R], /
) -> Callable[[SelfType, T], Coroutine[Any, Any, R]]:
    ...


@overload  # `batch` called WITH arguments
def batch(
    _: Literal[None] = None,
    /,
    max_batch_size: int = 10,
    batch_wait_timeout_s: float = 0.01,
    max_concurrent_batches: int = 1,
    batch_size_fn: Optional[Callable[[List], int]] = None,
) -> "_BatchDecorator":
    ...


class _BatchDecorator(Protocol):
    """Descibes behaviour of decorator produced by calling `batch` with arguments"""

    @overload  # Sync function
    def __call__(self, _sync_func: Callable[[List[T]], List[R]], /) -> Callable[[T], R]:
        ...

    @overload  # Async function
    def __call__(
        self, _async_func: Callable[[List[T]], Coroutine[Any, Any, List[R]]], /
    ) -> Callable[[T], Coroutine[Any, Any, R]]:
        ...

    @overload  # Sync method
    def __call__(
        self, _sync_meth: _SyncBatchingMethod[SelfType, T, R], /
    ) -> Callable[[SelfType, T], R]:
        ...

    @overload  # Async method
    def __call__(
        self, _async_meth: _AsyncBatchingMethod[SelfType, T, R], /
    ) -> Callable[[SelfType, T], Coroutine[Any, Any, R]]:
        ...


@PublicAPI(stability="stable")
def batch(
    _func: Optional[Callable] = None,
    /,
    max_batch_size: int = 10,
    batch_wait_timeout_s: float = 0.01,
    max_concurrent_batches: int = 1,
    batch_size_fn: Optional[Callable[[List], int]] = None,
) -> Callable:
    """Converts a function to asynchronously handle batches.

    The function can be a standalone function or a class method. In both
    cases, the function must be `async def` and take a list of objects as
    its sole argument and return a list of the same length as a result.

    When invoked, the caller passes a single object. These will be batched
    and executed asynchronously once there is a batch of `max_batch_size`
    or `batch_wait_timeout_s` has elapsed, whichever occurs first.

    `max_batch_size` and `batch_wait_timeout_s` can be updated using setter
    methods from the batch_handler (`set_max_batch_size` and
    `set_batch_wait_timeout_s`).

    Example:

    .. code-block:: python

            from ray import serve
            from starlette.requests import Request

            @serve.deployment
            class BatchedDeployment:
                @serve.batch(max_batch_size=10, batch_wait_timeout_s=0.1)
                async def batch_handler(self, requests: List[Request]) -> List[str]:
                    response_batch = []
                    for r in requests:
                        name = (await requests.json())["name"]
                        response_batch.append(f"Hello {name}!")

                    return response_batch

                def update_batch_params(self, max_batch_size, batch_wait_timeout_s):
                    self.batch_handler.set_max_batch_size(max_batch_size)
                    self.batch_handler.set_batch_wait_timeout_s(batch_wait_timeout_s)

                async def __call__(self, request: Request):
                    return await self.batch_handler(request)

            app = BatchedDeployment.bind()

    Arguments:
        max_batch_size: the maximum batch size that will be executed in
            one call to the underlying function.
        batch_wait_timeout_s: the maximum duration to wait for
            `max_batch_size` elements before running the current batch.
        max_concurrent_batches: the maximum number of batches that can be
            executed concurrently. If the number of concurrent batches exceeds
            this limit, the batch handler will wait for a batch to complete
            before sending the next batch to the underlying function.
        batch_size_fn: optional function to compute the effective batch size.
            If provided, this function takes a list of items and returns an
            integer representing the batch size. This is useful for batching
            based on custom metrics such as total nodes in graphs, total tokens
            in sequences, or other domain-specific measures. If None, the batch
            size is computed as len(batch).
    """
    # `_func` will be None in the case when the decorator is parametrized.
    # See the comment at the end of this function for a detailed explanation.
    if _func is not None:
        if not callable(_func):
            raise TypeError(
                "@serve.batch can only be used to decorate functions or methods."
            )

        if not iscoroutinefunction(_func):
            raise TypeError("Functions decorated with @serve.batch must be 'async def'")

    _validate_max_batch_size(max_batch_size)
    _validate_batch_wait_timeout_s(batch_wait_timeout_s)
    _validate_max_concurrent_batches(max_concurrent_batches)
    _validate_batch_size_fn(batch_size_fn)

    def _batch_decorator(_func):
        lazy_batch_queue_wrapper = _LazyBatchQueueWrapper(
            max_batch_size,
            batch_wait_timeout_s,
            max_concurrent_batches,
            _func,
            batch_size_fn,
        )

        async def batch_handler_generator(
            first_future: asyncio.Future,
        ) -> AsyncGenerator:
            """Generator that handles generator batch functions."""

            future = first_future
            while True:
                try:
                    async_response: _GeneratorResult = await future
                    future = async_response.next_future
                    yield async_response.result
                except StopAsyncIteration:
                    break

        def enqueue_request(args, kwargs) -> asyncio.Future:
            flattened_args: List = flatten_args(extract_signature(_func), args, kwargs)

            # If the function is a method, remove self as an argument.
            self = extract_self_if_method_call(args, _func)
            if self is not None:
                flattened_args = flattened_args[2:]

            batch_queue = lazy_batch_queue_wrapper.queue

            future = get_or_create_event_loop().create_future()
            request_context = serve.context._get_serve_request_context()
            batch_queue.put(
                _SingleRequest(self, flattened_args, future, request_context)
            )
            return future

        @wraps(_func)
        def generator_batch_wrapper(*args, **kwargs):
            first_future = enqueue_request(args, kwargs)
            return batch_handler_generator(first_future)

        @wraps(_func)
        async def batch_wrapper(*args, **kwargs):
            # This will raise if the underlying call raised an exception.
            return await enqueue_request(args, kwargs)

        if isasyncgenfunction(_func):
            wrapper = generator_batch_wrapper
        else:
            wrapper = batch_wrapper

        # We store the lazy_batch_queue_wrapper's getters and setters as
        # batch_wrapper attributes, so they can be accessed in user code.
        wrapper._get_max_batch_size = lazy_batch_queue_wrapper.get_max_batch_size
        wrapper._get_batch_wait_timeout_s = (
            lazy_batch_queue_wrapper.get_batch_wait_timeout_s
        )
        wrapper.set_max_batch_size = lazy_batch_queue_wrapper.set_max_batch_size
        wrapper.set_batch_wait_timeout_s = (
            lazy_batch_queue_wrapper.set_batch_wait_timeout_s
        )

        # Store debugging methods in the lazy_batch_queue wrapper
        wrapper._get_curr_iteration_start_times = (
            lazy_batch_queue_wrapper._get_curr_iteration_start_times
        )
        wrapper._is_batching_task_alive = (
            lazy_batch_queue_wrapper._is_batching_task_alive
        )
        wrapper._get_handling_task_stack = (
            lazy_batch_queue_wrapper._get_handling_task_stack
        )

        return wrapper

    # Unfortunately, this is required to handle both non-parametrized
    # (@serve.batch) and parametrized (@serve.batch(**kwargs)) usage.
    # In the former case, `serve.batch` will be called with the underlying
    # function as the sole argument. In the latter case, it will first be
    # called with **kwargs, then the result of that call will be called
    # with the underlying function as the sole argument (i.e., it must be a
    # "decorator factory.").
    return _batch_decorator(_func) if callable(_func) else _batch_decorator


def _set_result_if_not_done(future: asyncio.Future, result: Any):
    """Sets the future's result if the future is not done."""

    if not future.done():
        future.set_result(result)


def _set_exception_if_not_done(future: asyncio.Future, exception: Any):
    """Sets the future's exception if the future is not done."""

    if not future.done():
        future.set_exception(exception)
