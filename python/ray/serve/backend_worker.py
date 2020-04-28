import time
import traceback
import inspect

import ray
from ray import serve
from ray.serve import context as serve_context
from ray.serve.context import FakeFlaskRequest
from collections import defaultdict
from ray.serve.utils import parse_request_item
from ray.serve.exceptions import RayServeException
from ray.async_compat import sync_to_async


def create_backend_worker(func_or_class):
    """Creates a worker class wrapping the provided function or class."""

    if inspect.isfunction(func_or_class):
        is_function = True
    elif inspect.isclass(func_or_class):
        is_function = False
    else:
        assert False, "func_or_class must be function or class."

    class RayServeWrappedWorker(object):
        def __init__(self, backend_tag, replica_tag, init_args):
            serve.init()
            if is_function:
                _callable = func_or_class
            else:
                _callable = func_or_class(*init_args)

            self.backend = RayServeWorker(backend_tag, _callable, is_function)

        def get_metrics(self):
            return self.backend.get_metrics()

        async def handle_request(self, request):
            return await self.backend.handle_request(request)

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

    def __init__(self, name, _callable, is_function):
        self.name = name
        self.callable = _callable
        self.is_function = is_function

        self.error_counter = 0
        self.latency_list = []

    def get_metrics(self):
        # Make a copy of the latency list and clear current list
        latency_list = self.latency_list[:]
        self.latency_list = []

        return {
            "{}_error_counter".format(self.name): {
                "value": self.error_counter,
                "type": "counter",
            },
            "{}_latency_s".format(self.name): {
                "value": latency_list,
                "type": "list",
            },
        }

    def get_runner_method(self, request_item):
        method_name = request_item.call_method
        if not hasattr(self.callable, method_name):
            raise RayServeException("Backend doesn't have method {} "
                                    "which is specified in the request. "
                                    "The avaiable methods are {}".format(
                                        method_name, dir(self)))
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

    async def invoke_single(self, request_item):
        args, kwargs, is_web_context = parse_request_item(request_item)
        serve_context.web = is_web_context
        start_timestamp = time.time()

        method_to_call = self.get_runner_method(request_item)
        args = args if self.has_positional_args(method_to_call) else []
        method_to_call = ensure_async(method_to_call)
        try:
            result = await method_to_call(*args, **kwargs)
        except Exception as e:
            result = wrap_to_ray_error(e)
            self.error_counter += 1

        self.latency_list.append(time.time() - start_timestamp)
        return result

    async def invoke_batch(self, request_item_list):
        # TODO(alind) : create no-http services. The enqueues
        # from such services will always be TaskContext.Python.

        # Assumption : all the requests in a bacth
        # have same serve context.

        # For batching kwargs are modified as follows -
        # kwargs [Python Context] : key,val
        # kwargs_list             : key, [val1,val2, ... , valn]
        # or
        # args[Web Context]       : val
        # args_list               : [val1,val2, ...... , valn]
        # where n (current batch size) <= max_batch_size of a backend

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

            start_timestamp = time.time()
            result_list = await call_method(*arg_list, **kwargs_list)

            self.latency_list.append(time.time() - start_timestamp)
            if (not isinstance(result_list,
                               list)) or (len(result_list) != batch_size):
                raise RayServeException("__call__ function "
                                        "doesn't preserve batch-size. "
                                        "Please return a list of result "
                                        "with length equals to the batch "
                                        "size.")
            return result_list
        except Exception as e:
            wrapped_exception = wrap_to_ray_error(e)
            self.error_counter += batch_size
            return [wrapped_exception for _ in range(batch_size)]

    async def handle_request(self, request):
        # check if work_item is a list or not
        # if it is list: then batching supported
        if not isinstance(request, list):
            result = await self.invoke_single(request)
        else:
            result = await self.invoke_batch(request)

        # re-assign to default values
        serve_context.web = False
        serve_context.batch_size = None

        return result
