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
    Tuple,
    TypeVar,
    overload,
)

from ray import serve
from ray._private.signature import extract_signature, flatten_args, recover_args
from ray._private.utils import get_or_create_event_loop
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import extract_self_if_method_call
from ray.serve.exceptions import RayServeException
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


@dataclass
class _GeneratorResult:
    result: Any
    next_future: asyncio.Future


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
        handle_batch_func: Optional[Callable] = None,
    ) -> None:
        """Async queue that accepts individual items and returns batches.

        Respects max_batch_size and timeout_s; a batch will be returned when
        max_batch_size elements are available or the timeout has passed since
        the previous get.

        If handle_batch_func is passed in, a background coroutine will run to
        poll from the queue and call handle_batch_func on the results.

        Cannot be pickled.

        Arguments:
            max_batch_size: max number of elements to return in a batch.
            timeout_s: time to wait before returning an incomplete
                batch.
            handle_batch_func(Optional[Callable]): callback to run in the
                background to handle batches if provided.
        """
        self.queue: asyncio.Queue[_SingleRequest] = asyncio.Queue()
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.requests_available_event = asyncio.Event()

        # Used for observability.
        self.curr_iteration_start_time = time.time()

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
        if max_ongoing_requests < self.max_batch_size:
            logger.warning(
                f"`max_batch_size` ({self.max_batch_size}) is larger than "
                f"`max_ongoing_requests` ({max_ongoing_requests}). This means "
                "the replica will never receive a full batch. Please update "
                "`max_ongoing_requests` to be >= `max_batch_size`."
            )

    def set_max_batch_size(self, new_max_batch_size: int) -> None:
        """Updates queue's max_batch_size."""
        self.max_batch_size = new_max_batch_size
        self._warn_if_max_batch_size_exceeds_max_ongoing_requests()

    def put(self, request: Tuple[_SingleRequest, asyncio.Future]) -> None:
        self.queue.put_nowait(request)
        self.requests_available_event.set()

    async def wait_for_batch(self) -> List[Any]:
        """Wait for batch respecting self.max_batch_size and self.timeout_s.

        Returns a batch of up to self.max_batch_size items. Waits for up to
        to self.timeout_s after receiving the first request that will be in
        the next batch. After the timeout, returns as many items as are ready.

        Always returns a batch with at least one item - will block
        indefinitely until an item comes in.
        """

        batch = []
        batch.append(await self.queue.get())

        # Cache current max_batch_size and batch_wait_timeout_s for this batch.
        max_batch_size = self.max_batch_size
        batch_wait_timeout_s = self.batch_wait_timeout_s

        # Wait self.timeout_s seconds for new queue arrivals.
        batch_start_time = time.time()
        while True:
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

            # Add all new arrivals to the batch.
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

            if (
                time.time() - batch_start_time >= batch_wait_timeout_s
                or len(batch) >= max_batch_size
            ):
                break

        return batch

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

    async def _process_batches(self, func: Callable) -> None:
        """Loops infinitely and processes queued request batches."""

        while not self._loop.is_closed():
            try:
                self.curr_iteration_start_time = time.time()
                await self._process_batch(func)
            except Exception:
                logger.exception(
                    "_process_batches asyncio task ran into an unexpected exception."
                )

    async def _process_batch(self, func: Callable) -> None:
        """Processes queued request batch."""

        batch: List[_SingleRequest] = await self.wait_for_batch()
        assert len(batch) > 0
        futures = [item.future for item in batch]

        # Most of the logic in the function should be wrapped in this try-
        # except block, so the futures' exceptions can be set if an exception
        # occurs. Otherwise, the futures' requests may hang indefinitely.
        try:
            self_arg = batch[0].self_arg
            args, kwargs = _batch_args_kwargs([item.flattened_args for item in batch])

            # Method call.
            if self_arg is not None:
                func_future_or_generator = func(self_arg, *args, **kwargs)
            # Normal function call.
            else:
                func_future_or_generator = func(*args, **kwargs)

            if isasyncgenfunction(func):
                func_generator = func_future_or_generator
                await self._consume_func_generator(func_generator, futures, len(batch))
            else:
                func_future = func_future_or_generator
                await self._assign_func_results(func_future, futures, len(batch))

        except Exception as e:
            logger.exception("_process_batch ran into an unexpected exception.")

            for future in futures:
                _set_exception_if_not_done(future, e)

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
        handle_batch_func: Optional[Callable] = None,
    ):
        self._queue: Optional[_BatchQueue] = None
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.handle_batch_func = handle_batch_func

    @property
    def queue(self) -> _BatchQueue:
        """Returns _BatchQueue.

        Initializes queue when called for the first time.
        """
        if self._queue is None:
            self._queue = _BatchQueue(
                self.max_batch_size,
                self.batch_wait_timeout_s,
                self.handle_batch_func,
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

    def _get_curr_iteration_start_time(self) -> Optional[float]:
        """Gets current iteration's start time on default _BatchQueue implementation.

        Returns None if the batch handler doesn't use a default _BatchQueue.
        """

        if hasattr(self.queue, "curr_iteration_start_time"):
            return self.queue.curr_iteration_start_time
        else:
            return None

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
            "batch_wait_timeout_s must be a float >= 0, " f"got {batch_wait_timeout_s}"
        )

    if batch_wait_timeout_s < 0:
        raise ValueError(
            "batch_wait_timeout_s must be a float >= 0, " f"got {batch_wait_timeout_s}"
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
    batch_wait_timeout_s: float = 0.0,
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
    batch_wait_timeout_s: float = 0.0,
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

    def _batch_decorator(_func):
        lazy_batch_queue_wrapper = _LazyBatchQueueWrapper(
            max_batch_size,
            batch_wait_timeout_s,
            _func,
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
            batch_queue.put(_SingleRequest(self, flattened_args, future))
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
        wrapper._get_curr_iteration_start_time = (
            lazy_batch_queue_wrapper._get_curr_iteration_start_time
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
