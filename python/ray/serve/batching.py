import asyncio
from functools import wraps
from inspect import iscoroutinefunction
import time
from typing import Any, Callable, List, Optional

from ray.serve.exceptions import RayServeException


class _BatchQueue:
    def __init__(self,
                 max_batch_size: int,
                 timeout_s: float,
                 handle_batch_func: Optional[Callable] = None) -> None:
        self.queue = asyncio.Queue()
        self.full_batch_event = asyncio.Event()
        self.max_batch_size = max_batch_size
        self.timeout_s = timeout_s

        self._handle_batch_task = None
        if handle_batch_func is not None:
            self._handle_batch_task = asyncio.get_event_loop().create_task(
                self._handle_batches(handle_batch_func))

    def set_config(self, max_batch_size: int, timeout_s: float) -> None:
        self.max_batch_size = max_batch_size
        self.timeout_s = timeout_s

    def put(self, request: Any) -> None:
        self.queue.put_nowait(request)
        # Signal when the full batch is ready. The event will be reset
        # in wait_for_batch.
        if self.queue.qsize() == self.max_batch_size:
            self.full_batch_event.set()

    def qsize(self) -> int:
        return self.queue.qsize()

    async def wait_for_batch(self) -> List[Any]:
        """Wait for batch respecting self.max_batch_size and self.timeout_s.

        Returns a batch of up to self.max_batch_size items, waiting for up
        to self.timeout_s for a full batch. After the timeout, returns as many
        items as are ready.

        Always returns a batch with at least one item - will block
        indefinitely until an item comes in.
        """
        curr_timeout = self.timeout_s
        batch = []
        while len(batch) == 0:
            loop_start = time.time()

            # If the timeout is 0, wait for any item to be available on the
            # queue.
            if curr_timeout == 0:
                batch.append(await self.queue.get())
            # If the timeout is nonzero, wait for either the timeout to occur
            # or the max batch size to be ready.
            else:
                try:
                    await asyncio.wait_for(self.full_batch_event.wait(),
                                           curr_timeout)
                except asyncio.TimeoutError:
                    pass

            # Pull up to the max_batch_size requests off the queue.
            while len(batch) < self.max_batch_size and not self.queue.empty():
                batch.append(self.queue.get_nowait())

            # Reset the event if there are fewer than max_batch_size requests
            # in the queue.
            if (self.queue.qsize() < self.max_batch_size
                    and self.full_batch_event.is_set()):
                self.full_batch_event.clear()

            # Adjust the timeout based on the time spent in this iteration.
            curr_timeout = max(0, curr_timeout - (time.time() - loop_start))

        return batch

    async def _handle_batches(self, func):
        while True:
            batch = await self.wait_for_batch()
            assert len(batch) > 0
            self_arg = batch[0][0]
            args = [item[1] for item in batch]
            futures = [item[2] for item in batch]

            try:
                # Method call.
                if self_arg is not None:
                    results = await func(self_arg, args)
                # Normal function call.
                else:
                    results = await func(args)

                print("RESULTS", results)
                if len(results) != len(batch):
                    raise RayServeException(
                        "Batched function doesn't preserve batch size. "
                        f"The input list has length {len(batch)} but the "
                        f"returned list has length {len(results)}.")
            except Exception as e:
                print("GOT EXCEPTION :(")
                results = [_ExceptionWrapper(e)] * len(batch)
                print("RESULTS", results)

            for i, result in enumerate(results):
                futures[i].set_result(result)

    def __del__(self):
        if self._handle_batch_task is None:
            return

        async def await_task():
            try:
                await self._handle_batch_task
            except asyncio.CancelledError:
                pass

        # TODO(edoakes): although we try to gracefully shutdown here, it still
        # causes some errors when the process exits due to the asyncio loop
        # already being destroyed.
        self._handle_batch_task.cancel()
        asyncio.get_event_loop.run_until_complete(await_task())


class _ExceptionWrapper:
    def __init__(self, underlying):
        assert isinstance(underlying, Exception)
        self._underlying = underlying

    def raise_underlying(self):
        raise self._underlying


def batch(func=None, max_batch_size=10, batch_wait_timeout_s=0.1):
    """Converts a function to asynchronously handle batches.

    The function can be a standalone function or a class method, and must
    take a list of objects as its sole argument and return a list of the
    same length.

    When invoked, the caller passes a single object. These will be batched
    and executed asynchronously once there is a batch of `max_batch_size`
    or `batch_wait_timeout_s` has elapsed, whichever occurs first.

    Example:
        @serve.batch(max_batch_size=50, batch_wait_timeout_s=0.5)
        async def handle_batch(self, batch: List[str]):
            return [s.lower() for s in batch]

        async def handle_single(self, s: str):
            # Will return s.lower().
            return await handle_batch(s)
    """
    if func is not None:
        if not callable(func):
            raise TypeError("@serve.batch can only be used to "
                            "decorate functions or methods.")

        if not iscoroutinefunction(func):
            raise TypeError(
                "Functions decorated with @serve.batch must be 'async def'")

    if not isinstance(max_batch_size, int):
        if isinstance(max_batch_size, float) and max_batch_size.is_integer():
            max_batch_size = int(max_batch_size)
        else:
            raise TypeError("max_batch_size must be integer >= 1")

    if max_batch_size < 1:
        raise ValueError("max_batch_size must be an integer >= 1")

    if not isinstance(batch_wait_timeout_s, (float, int)):
        raise TypeError("batch_wait_timeout_s must be a float >= 0")

    if batch_wait_timeout_s < 0:
        raise ValueError("batch_wait_timeout_s must be a float >= 0")

    def _batch_decorator(func):
        @wraps(func)
        async def batch_wrapper(*args, **kwargs):
            nonlocal func
            is_method = False
            if len(args) > 0:
                method = getattr(args[0], func.__name__, False)
                if method:
                    wrapped = getattr(method, "__wrapped__", False)
                    if wrapped and wrapped == func:
                        is_method = True

            self = None
            args = list(args)
            if is_method:
                self = args.pop(0)

            if len(args) != 1:
                raise ValueError("@serve.batch functions can only take a "
                                 "single argument as input")

            if len(kwargs) > 0:
                raise ValueError(
                    "kwargs not supported for @serve.batch functions")

            batch_queue_attr = f"__serve_batch_queue_{func.__name__}"
            if is_method:
                batch_queue_object = self
            else:
                batch_queue_object = func

            if not hasattr(batch_queue_object, batch_queue_attr):
                batch_queue = _BatchQueue(max_batch_size, batch_wait_timeout_s,
                                          func)
                setattr(batch_queue_object, batch_queue_attr, batch_queue)
            else:
                batch_queue = getattr(batch_queue_object, batch_queue_attr)

            future = asyncio.get_event_loop().create_future()
            batch_queue.put((self, args[0], future))
            result = await future
            if isinstance(result, _ExceptionWrapper):
                result.raise_underlying()

            return result

        return batch_wrapper

    return _batch_decorator(func) if callable(func) else _batch_decorator
