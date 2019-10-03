import traceback

import ray


class TaskRunner:
    """A simple class that runs a function.

    The purpose of this class is to model what the most basic actor could be.
    That is, a ray serve actor should implement the TaskRunner interface.
    """

    def __init__(self, func_to_run):
        self.func = func_to_run

    def __call__(self, *args):
        return self.func(*args)


def wrap_to_ray_error(callable_obj, *args):
    """Utility method that catch and seal exceptions in execution"""
    try:
        return callable_obj(*args)
    except Exception as e:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(
            str(callable_obj), traceback_str, e.__class__)


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

    def _ray_serve_setup(self, my_name, _ray_serve_router_handle):
        self._ray_serve_dequeue_requestr_name = my_name
        self._ray_serve_router_handle = _ray_serve_router_handle
        self._ray_serve_setup_completed = True

    def _ray_serve_main_loop(self, my_handle):
        assert self._ray_serve_setup_completed
        self._ray_serve_self_handle = my_handle

        work_token = ray.get(
            self._ray_serve_router_handle.dequeue_request.remote(
                self._ray_serve_dequeue_requestr_name))
        work_item = ray.get(ray.ObjectID(work_token))

        # TODO(simon):
        # __call__ should be able to take multiple *args and **kwargs.
        result = wrap_to_ray_error(self.__call__, work_item.request_body)
        result_object_id = work_item.result_object_id
        ray.worker.global_worker.put_object(result_object_id, result)

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
