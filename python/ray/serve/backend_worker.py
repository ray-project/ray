import asyncio
import traceback
import inspect
from collections.abc import Iterable
from collections import defaultdict
from itertools import groupby
from typing import Union, List, Any, Callable, Type
import time

import ray
from ray.async_compat import sync_to_async

from ray.serve import context as serve_context
from ray.serve.context import FakeFlaskRequest
from ray.serve.utils import (parse_request_item, _get_logger, chain_future,
                             unpack_future)
from ray.serve.exceptions import RayServeException
from ray.experimental import metrics
from ray.serve.config import BackendConfig
from ray.serve.router import Query
from ray.serve.constants import DEFAULT_LATENCY_BUCKET_MS
from ray.exceptions import RayTaskError

logger = _get_logger()


class BatchQueue:
    def __init__(self, max_batch_size: int, timeout_s: float) -> None:
        self.queue = asyncio.Queue()
        self.full_batch_event = asyncio.Event()
        self.max_batch_size = max_batch_size
        self.timeout_s = timeout_s

    def set_config(self, max_batch_size: int, timeout_s: float) -> None:
        self.max_batch_size = max_batch_size
        self.timeout_s = timeout_s

    def put(self, request: Query) -> None:
        self.queue.put_nowait(request)
        # Signal when the full batch is ready. The event will be reset
        # in wait_for_batch.
        if self.queue.qsize() == self.max_batch_size:
            self.full_batch_event.set()

    def qsize(self) -> int:
        return self.queue.qsize()

    async def wait_for_batch(self) -> List[Query]:
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


def create_backend_worker(func_or_class: Union[Callable, Type[Callable]]):
    """Creates a worker class wrapping the provided function or class."""

    if inspect.isfunction(func_or_class):
        is_function = True
    elif inspect.isclass(func_or_class):
        is_function = False
    else:
        assert False, "func_or_class must be function or class."

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedWorker(object):
        def __init__(self, backend_tag, replica_tag, init_args,
                     backend_config: BackendConfig, controller_name: str):
            # Set the controller name so that serve.connect() will connect to
            # the instance that this backend is running in.
            ray.serve.api._set_internal_controller_name(controller_name)
            if is_function:
                _callable = func_or_class
            else:
                _callable = func_or_class(*init_args)

            self.backend = RayServeWorker(backend_tag, replica_tag, _callable,
                                          backend_config, is_function)

        async def handle_request(self, request):
            return await self.backend.handle_request(request)

        def update_config(self, new_config: BackendConfig):
            return self.backend.update_config(new_config)

        def ready(self):
            pass

    RayServeWrappedWorker.__name__ = "RayServeWorker_" + func_or_class.__name__
    return RayServeWrappedWorker


def wrap_to_ray_error(exception: Exception) -> RayTaskError:
    """Utility method to wrap exceptions in user code."""

    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(str(e), traceback_str, e.__class__)


def ensure_async(func: Callable) -> Callable:
    if inspect.iscoroutinefunction(func):
        return func
    else:
        return sync_to_async(func)


class RayServeWorker:
    """Handles requests with the provided callable."""

    def __init__(self, backend_tag: str, replica_tag: str, _callable: Callable,
                 backend_config: BackendConfig, is_function: bool) -> None:
        self.backend_tag = backend_tag
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function

        self.config = backend_config
        self.batch_queue = BatchQueue(self.config.max_batch_size or 1,
                                      self.config.batch_wait_timeout)

        self.num_ongoing_requests = 0

        self.request_counter = metrics.Count(
            "backend_request_counter", ("Number of queries that have been "
                                        "processed in this replica"),
            "requests", ["backend"])
        self.error_counter = metrics.Count("backend_error_counter",
                                           ("Number of exceptions that have "
                                            "occurred in the backend"),
                                           "errors", ["backend"])
        self.restart_counter = metrics.Count(
            "backend_worker_starts",
            ("The number of time this replica workers "
             "has been restarted due to failure."), "restarts",
            ["backend", "replica_tag"])

        self.queuing_latency_tracker = metrics.Histogram(
            "backend_queuing_latency_ms",
            ("The latency for queries waiting in the replica's queue "
             "waiting to be processed or batched."), "ms",
            DEFAULT_LATENCY_BUCKET_MS, ["backend", "replica_tag"])
        self.processing_latency_tracker = metrics.Histogram(
            "backend_processing_latency_ms",
            "The latency for queries to be processed", "ms",
            DEFAULT_LATENCY_BUCKET_MS,
            ["backend", "replica_tag", "batch_size"])
        self.num_queued_items = metrics.Gauge(
            "replica_queued_queries",
            "Current number of queries queued in the the backend replicas",
            "requests", ["backend", "replica_tag"])
        self.num_processing_items = metrics.Gauge(
            "replica_processing_queries",
            "Current number of queries being processed", "requests",
            ["backend", "replica_tag"])

        self.restart_counter.record(1, {
            "backend": self.backend_tag,
            "replica_tag": self.replica_tag
        })

        asyncio.get_event_loop().create_task(self.main_loop())

    def get_runner_method(self, request_item: Query) -> Callable:
        method_name = request_item.metadata.call_method
        if not hasattr(self.callable, method_name):
            raise RayServeException("Backend doesn't have method {} "
                                    "which is specified in the request. "
                                    "The available methods are {}".format(
                                        method_name, dir(self.callable)))
        if self.is_function:
            return self.callable
        return getattr(self.callable, method_name)

    def has_positional_args(self, f: Callable) -> bool:
        # NOTE:
        # In the case of simple functions, not actors, the f will be
        # function.__call__, but we need to inspect the function itself.
        if self.is_function:
            f = self.callable

        signature = inspect.signature(f)
        for param in signature.parameters.values():
            if (param.kind == param.POSITIONAL_OR_KEYWORD
                    and param.default is param.empty):
                return True
        return False

    def _reset_context(self) -> None:
        # NOTE(simon): context management won't work in async mode because
        # many concurrent queries might be running at the same time.
        serve_context.web = None
        serve_context.batch_size = None

    async def invoke_single(self, request_item: Query) -> Any:
        args, kwargs, is_web_context = parse_request_item(request_item)
        serve_context.web = is_web_context

        method_to_call = self.get_runner_method(request_item)
        args = args if self.has_positional_args(method_to_call) else []
        method_to_call = ensure_async(method_to_call)

        start = time.time()
        try:
            result = await method_to_call(*args, **kwargs)
            self.request_counter.record(1, {"backend": self.backend_tag})
        except Exception as e:
            result = wrap_to_ray_error(e)
            self.error_counter.record(1, {"backend": self.backend_tag})
        finally:
            self._reset_context()
        self.processing_latency_tracker.record(
            (time.time() - start) * 1000, {
                "backend": self.backend_tag,
                "replica": self.replica_tag,
                "batch_size": "1"
            })

        return result

    async def invoke_batch(self, request_item_list: List[Query]) -> List[Any]:
        arg_list = []
        kwargs_list = defaultdict(list)
        context_flags = set()
        batch_size = len(request_item_list)
        call_methods = set()

        for item in request_item_list:
            args, kwargs, is_web_context = parse_request_item(item)
            context_flags.add(is_web_context)

            call_method = self.get_runner_method(item)
            call_methods.add(call_method)

            if is_web_context:
                # Python context only have kwargs
                flask_request = args[0]
                arg_list.append(flask_request)
            else:
                # Web context only have one positional argument
                for k, v in kwargs.items():
                    kwargs_list[k].append(v)

                # Set the flask request as a list to conform
                # with batching semantics: when in batching
                # mode, each argument is turned into list.
                if self.has_positional_args(call_method):
                    arg_list.append(FakeFlaskRequest())

        timing_start = time.time()
        try:
            # Check mixing of query context (unified context needed).
            if len(context_flags) != 1:
                raise RayServeException(
                    "Batched queries contain mixed context. Please only send "
                    "the same type of requests in batching mode.")
            serve_context.web = context_flags.pop()

            if len(call_methods) != 1:
                raise RayServeException(
                    "Queries contain mixed calling methods. Please only send "
                    "the same type of requests in batching mode.")
            call_method = ensure_async(call_methods.pop())

            serve_context.batch_size = batch_size
            # Flask requests are passed to __call__ as a list
            arg_list = [arg_list]

            self.request_counter.record(batch_size,
                                        {"backend": self.backend_tag})
            result_list = await call_method(*arg_list, **kwargs_list)

            if not isinstance(result_list, Iterable) or isinstance(
                    result_list, (dict, set)):
                error_message = ("RayServe expects an ordered iterable object "
                                 "but the worker returned a {}".format(
                                     type(result_list)))
                raise RayServeException(error_message)

            # Normalize the result into a list type. This operation is fast
            # in Python because it doesn't copy anything.
            result_list = list(result_list)

            if (len(result_list) != batch_size):
                error_message = ("Worker doesn't preserve batch size. The "
                                 "input has length {} but the returned list "
                                 "has length {}. Please return a list of "
                                 "results with length equal to the batch size"
                                 ".".format(batch_size, len(result_list)))
                raise RayServeException(error_message)
            self._reset_context()
        except Exception as e:
            wrapped_exception = wrap_to_ray_error(e)
            self.error_counter.record(1, {"backend": self.backend_tag})
            self._reset_context()
            result_list = [wrapped_exception for _ in range(batch_size)]

        self.processing_latency_tracker.record(
            (time.time() - timing_start) * 1000, {
                "backend": self.backend_tag,
                "replica_tag": self.replica_tag,
                "batch_size": str(batch_size)
            })

        return result_list

    async def main_loop(self) -> None:
        while True:
            # NOTE(simon): There's an issue when user updated batch size and
            # batch wait timeout during the execution, these values will not be
            # updated until after the current iteration.
            batch = await self.batch_queue.wait_for_batch()

            # Record metrics
            self.num_queued_items.record(self.batch_queue.qsize(), {
                "backend": self.backend_tag,
                "replica_tag": self.replica_tag
            })
            self.num_processing_items.record(
                self.num_ongoing_requests - self.batch_queue.qsize(), {
                    "backend": self.backend_tag,
                    "replica_tag": self.replica_tag
                })
            for query in batch:
                queuing_time = (time.time() - query.tick_enter_replica) * 1000
                self.queuing_latency_tracker.record(queuing_time, {
                    "backend": self.backend_tag,
                    "replica_tag": self.replica_tag
                })

            all_evaluated_futures = []

            if not self.config.internal_metadata.accepts_batches:
                query = batch[0]
                evaluated = asyncio.ensure_future(self.invoke_single(query))
                all_evaluated_futures = [evaluated]
                chain_future(evaluated, query.async_future)
            else:
                get_call_method = (
                    lambda query: query.metadata.call_method  # noqa: E731
                )
                sorted_batch = sorted(batch, key=get_call_method)
                for _, group in groupby(sorted_batch, key=get_call_method):
                    group = list(group)
                    evaluated = asyncio.ensure_future(self.invoke_batch(group))
                    all_evaluated_futures.append(evaluated)
                    result_futures = [q.async_future for q in group]
                    chain_future(
                        unpack_future(evaluated, len(group)), result_futures)

            if self.config.internal_metadata.is_blocking:
                # We use asyncio.wait here so if the result is exception,
                # it will not be raised.
                await asyncio.wait(all_evaluated_futures)

    def update_config(self, new_config: BackendConfig) -> None:
        self.config = new_config
        self.batch_queue.set_config(self.config.max_batch_size or 1,
                                    self.config.batch_wait_timeout)

    async def handle_request(self,
                             request: Union[Query, bytes]) -> asyncio.Future:
        if isinstance(request, bytes):
            request = Query.ray_deserialize(request)

        request.tick_enter_replica = time.time()
        logger.debug("Worker {} got request {}".format(self.replica_tag,
                                                       request))
        request.async_future = asyncio.get_event_loop().create_future()
        self.num_ongoing_requests += 1

        self.batch_queue.put(request)
        result = await request.async_future

        self.num_ongoing_requests -= 1
        return result
