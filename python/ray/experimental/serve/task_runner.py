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
    except Exception:
        traceback_str = traceback.format_exc()
        return ray.exceptions.RayTaskError(str(callable_obj), traceback_str)


class RayServeMixin:
    """This mixin class adds the functionality to fetch from router queues.

    Warning:
        It assumes the main execution method is __call__ of the current class.

    Example:
        >>> # to make MyClass servable
        >>> @ray.remote
            class RayServeActor(MyClass, RayServeMixin):
                pass
    """

    self_handle = None
    router_handle = None
    setup_completed = False
    consumer_name = None

    def setup(self, my_name, router_handle):
        self.consumer_name = my_name
        self.router_handle = router_handle
        self.setup_completed = True

    def main_loop(self, my_handle):
        assert self.setup_completed
        self.self_handle = my_handle

        work_token = ray.get(
            self.router_handle.consume.remote(self.consumer_name))
        work_item = ray.get(ray.ObjectID(work_token))

        # TODO(simon): handle variadic arguments
        result = wrap_to_ray_error(self.__call__, work_item.request_body)
        result_oid = work_item.result_oid
        ray.worker.global_worker.put_object(result_oid, result)

        # tail recursively schedule itself
        # TODO(simon): remove tail recursion, ask router to callback instead
        self.self_handle.main_loop.remote(my_handle)


# The TaskRunnerBackend class exists for documentation purpose
class TaskRunnerBackend(TaskRunner, RayServeMixin):
    """A simple function serving backend

    Note that this is not yet an actor. To make it an actor:

    >>> @ray.remote
        class TaskRunnerActor(TaskRunnerBackend):
            pass
    """

    pass


@ray.remote
class TaskRunnerActor(TaskRunnerBackend):
    pass
