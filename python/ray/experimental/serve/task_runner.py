import traceback
import time

import ray
from ray.experimental.serve import context as serve_context
from ray.experimental.serve.context import TaskContext, FakeFlaskQuest
from ray.experimental.serve.http_util import build_flask_request


class TaskRunner:
    """A simple class that runs a function.

    The purpose of this class is to model what the most basic actor could be.
    That is, a ray serve actor should implement the TaskRunner interface.
    """

    def __init__(self, func_to_run):
        self.func = func_to_run

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def wrap_to_ray_error(exception):
    """Utility method that catch and seal exceptions in execution"""
    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(str(e), traceback_str, e.__class__)


class RayServeMixin:
    """This mixin class adds the functionality to fetch from router queues.

    Warning:
        It assumes the main execution method is `__call__` of the user defined
        class. This means that serve will call `your_instance.__call__` when
        each request comes in. This behavior will be fixed in the future to
        allow assigning artibrary methods.

    Example:
        >>> # Use ray.remote decorator and RayServeMixin
        >>> # to make MyClass servable.
        >>> @ray.remote
            class RayServeActor(RayServeMixin, MyClass):
                pass
    """
    _ray_serve_self_handle = None
    _ray_serve_router_handle = None
    _ray_serve_setup_completed = False
    _ray_serve_dequeue_requestr_name = None

    # Work token can be unfullfilled from last iteration.
    # This cache will be used to determine whether or not we should
    # work on the same task as previous iteration or we are ready to
    # move on.
    _ray_serve_cached_work_token = None

    _serve_metric_error_counter = 0
    _serve_metric_latency_list = []

    def _serve_metric(self):
        # Make a copy of the latency list and clear current list
        latency_lst = self._serve_metric_latency_list[:]
        self._serve_metric_latency_list = []

        my_name = self._ray_serve_dequeue_requestr_name

        return {
            "{}_error_counter".format(my_name): {
                "value": self._serve_metric_error_counter,
                "type": "counter",
            },
            "{}_latency_s".format(my_name): {
                "value": latency_lst,
                "type": "list",
            },
        }

    def _ray_serve_setup(self, my_name, _ray_serve_router_handle):
        self._ray_serve_dequeue_requestr_name = my_name
        self._ray_serve_router_handle = _ray_serve_router_handle
        self._ray_serve_setup_completed = True

    def _ray_serve_main_loop(self, my_handle):
        assert self._ray_serve_setup_completed
        self._ray_serve_self_handle = my_handle

        # Only retrieve the next task if we have completed previous task.
        if self._ray_serve_cached_work_token is None:
            work_token = ray.get(
                self._ray_serve_router_handle.dequeue_request.remote(
                    self._ray_serve_dequeue_requestr_name))
        else:
            work_token = self._ray_serve_cached_work_token

        work_token_id = ray.ObjectID(work_token)
        ready, not_ready = ray.wait(
            [work_token_id], num_returns=1, timeout=0.5)
        if len(ready) == 1:
            work_item = ray.get(work_token_id)
            self._ray_serve_cached_work_token = None
        else:
            self._ray_serve_cached_work_token = work_token
            self._ray_serve_self_handle._ray_serve_main_loop.remote(my_handle)
            return

        if work_item.request_context == TaskContext.Web:
            serve_context.web = True
            asgi_scope, body_bytes = work_item.request_args
            flask_request = build_flask_request(asgi_scope, body_bytes)
            args = (flask_request, )
            kwargs = {}
        else:
            serve_context.web = False
            args = (FakeFlaskQuest(), )
            kwargs = work_item.request_kwargs

        result_object_id = work_item.result_object_id

        start_timestamp = time.time()
        try:
            result = self.__call__(*args, **kwargs)
            ray.worker.global_worker.put_object(result_object_id, result)
        except Exception as e:
            wrapped_exception = wrap_to_ray_error(e)
            self._serve_metric_error_counter += 1
            ray.worker.global_worker.put_object(result_object_id,
                                                wrapped_exception)
        self._serve_metric_latency_list.append(time.time() - start_timestamp)

        serve_context.web = False
        # The worker finished one unit of work.
        # It will now tail recursively schedule the main_loop again.

        # TODO(simon): remove tail recursion, ask router to callback instead
        self._ray_serve_self_handle._ray_serve_main_loop.remote(my_handle)


class TaskRunnerBackend(TaskRunner, RayServeMixin):
    """A simple function serving backend

    Note that this is not yet an actor. To make it an actor:

    >>> @ray.remote
        class TaskRunnerActor(TaskRunnerBackend):
            pass

    Note:
    This class is not used in the actual ray serve system. It exists
    for documentation purpose.
    """

    pass


@ray.remote
class TaskRunnerActor(TaskRunnerBackend):
    pass
