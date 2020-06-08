import asyncio
import traceback
import inspect
from collections.abc import Iterable
from collections import defaultdict
from itertools import groupby
from operator import attrgetter
import time

import ray
from ray.async_compat import sync_to_async

from ray import serve
from ray.serve import context as serve_context
from ray.serve.context import FakeFlaskRequest
from ray.serve.utils import (parse_request_item, _get_logger, chain_future,
                             unpack_future)
from ray.serve.exceptions import RayServeException
from ray.serve.metric import MetricClient
from ray.serve.config import BackendConfig
from ray.serve.router import Query

logger = _get_logger()


class WaitableQueue(asyncio.Queue):
    async def wait_for_batch(self, num_items: int, timeout_s: float):
        """Wait up to num_items in the queue given timeout_s.

        This method will block indefinitely for the first item. Therefore, it
        guarantees to return at least one item.
        """

        assert num_items >= 1
        # Wait for the first value without timeout. We will return at least
        # one item. Additionally this help the caller context switch on empty
        # queue.
        start_waiting = time.time()
        batch = [
            await self.get(),
        ]

        # Adjust the timeout to account for the time waiting for first item.
        time_remaining = timeout_s - (time.time() - start_waiting)
        time_remaining = max(0, time_remaining)

        # Wait for the remaining batch with the timeout
        if num_items > 1:
            done_set, not_done_set = await asyncio.wait(
                [self.get() for _ in range(num_items - 1)],
                timeout=time_remaining)
            for task in done_set:
                batch.append(task.result())
            for task in not_done_set:
                task.cancel()
        return batch


def create_backend_worker(func_or_class):
    """Creates a worker class wrapping the provided function or class."""

    if inspect.isfunction(func_or_class):
        is_function = True
    elif inspect.isclass(func_or_class):
        is_function = False
    else:
        assert False, "func_or_class must be function or class."

    class RayServeWrappedWorker(object):
        def __init__(self,
                     backend_tag,
                     replica_tag,
                     init_args,
                     backend_config: BackendConfig,
                     instance_name=None):
            serve.init(name=instance_name)

            if is_function:
                _callable = func_or_class
            else:
                _callable = func_or_class(*init_args)

            master = serve.api._get_master_actor()
            [metric_exporter] = ray.get(master.get_metric_exporter.remote())
            metric_client = MetricClient(
                metric_exporter, default_labels={"backend": backend_tag})
            self.backend = RayServeWorker(backend_tag, replica_tag, _callable,
                                          backend_config, is_function,
                                          metric_client)

        async def handle_request(self, request):
            return await self.backend.handle_request(request)

        def update_config(self, new_config: BackendConfig):
            return self.backend.update_config(new_config)

        def ready(self):
            pass

    RayServeWrappedWorker.__name__ = "RayServeWorker_" + func_or_class.__name__
    return RayServeWrappedWorker


def wrap_to_ray_error(exception):
    """Utility method to wrap exceptions in user code."""

    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(str(e), traceback_str, e.__class__)


def ensure_async(func):
    if inspect.iscoroutinefunction(func):
        return func
    else:
        return sync_to_async(func)


class RayServeWorker:
    """Handles requests with the provided callable."""

    def __init__(self, name, replica_tag, _callable,
                 backend_config: BackendConfig, is_function, metric_client):
        self.name = name
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function

        self.config = backend_config
        self.query_queue = WaitableQueue()

        self.metric_client = metric_client
        self.request_counter = self.metric_client.new_counter(
            "backend_request_counter",
            description=("Number of queries that have been "
                         "processed in this replica"),
        )
        self.error_counter = self.metric_client.new_counter(
            "backend_error_counter",
            description=("Number of exceptions that have "
                         "occurred in the backend"),
        )
        self.restart_counter = self.metric_client.new_counter(
            "backend_worker_starts",
            description=("The number of time this replica workers "
                         "has been restarted due to failure."),
            label_names=("replica_tag", ))

        self.restart_counter.labels(replica_tag=self.replica_tag).add()

        self.loop_task = asyncio.get_event_loop().create_task(self.main_loop())
        self.config_updated = asyncio.Event()

    def get_runner_method(self, request_item):
        method_name = request_item.call_method
        if not hasattr(self.callable, method_name):
            raise RayServeException("Backend doesn't have method {} "
                                    "which is specified in the request. "
                                    "The available methods are {}".format(
                                        method_name, dir(self.callable)))
        if self.is_function:
            return self.callable
        return getattr(self.callable, method_name)

    def has_positional_args(self, f):
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

    def _reset_context(self):
        # NOTE(simon): context management won't work in async mode because
        # many concurrent queries might be running at the same time.
        serve_context.web = None
        serve_context.batch_size = None

    async def invoke_single(self, request_item):
        args, kwargs, is_web_context = parse_request_item(request_item)
        serve_context.web = is_web_context

        method_to_call = self.get_runner_method(request_item)
        args = args if self.has_positional_args(method_to_call) else []
        method_to_call = ensure_async(method_to_call)
        try:
            result = await method_to_call(*args, **kwargs)
            self.request_counter.add()
        except Exception as e:
            result = wrap_to_ray_error(e)
            self.error_counter.add()
        finally:
            self._reset_context()

        return result

    async def invoke_batch(self, request_item_list):
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

            self.request_counter.add(batch_size)
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
            return result_list
        except Exception as e:
            wrapped_exception = wrap_to_ray_error(e)
            self.error_counter.add()
            self._reset_context()
            return [wrapped_exception for _ in range(batch_size)]

    async def main_loop(self):
        while True:
            # NOTE(simon): There's an issue when user updated batch size and
            # batch wait timeout during the execution, these values will not be
            # updated until after the current iteration.
            batch = await self.query_queue.wait_for_batch(
                num_items=self.config.max_batch_size or 1,
                timeout_s=self.config.batch_wait_timeout)

            all_evaluated_futures = []

            if not self.config.accepts_batches:
                query = batch[0]
                evaluated = asyncio.ensure_future(self.invoke_single(query))
                all_evaluated_futures = [evaluated]
                chain_future(evaluated, query.async_future)
            else:
                get_call_method = attrgetter("call_method")
                sorted_batch = sorted(batch, key=get_call_method)
                for _, group in groupby(sorted_batch, key=get_call_method):
                    group = sorted(group)
                    evaluated = asyncio.ensure_future(self.invoke_batch(group))
                    all_evaluated_futures.append(evaluated)
                    result_futures = [q.async_future for q in group]
                    chain_future(
                        unpack_future(evaluated, len(group)), result_futures)

            if self.config.is_blocking:
                # We use asyncio.wait here so if the result is exception,
                # it will not be raised.
                await asyncio.wait(all_evaluated_futures)

    def update_config(self, new_config: BackendConfig):
        self.config = new_config
        self.config_updated.set()

    async def handle_request(self, request: Query):
        assert not isinstance(request, list)
        logger.debug("Worker {} got request {}".format(self.name, request))
        request.async_future = asyncio.get_event_loop().create_future()
        self.query_queue.put_nowait(request)
        return await request.async_future
