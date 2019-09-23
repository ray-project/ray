import traceback

import ray
import time


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
    """Utility method that catch and seal exceptions in execution

    Returns:
        (object, bool): A tuple (call_result, has_errored). If the
            call errored, the object will be wrapped into RayTaskError. The
            second item of the tuple is True if the call didn't erorr, else
            if the call errored.
    """
    try:
        return callable_obj(*args), False
    except Exception:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(str(callable_obj),
                                           traceback_str), True


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
    _ray_serve_replica_id = None

    #: If true, this work token will be used to instead of fetching new one
    #  from the router.
    _ray_serve_work_token_cache = None

    # Buffer for internal metric aggregation.
    _ray_serve_latency_buffer = []
    _ray_serve_error_counter = 0

    def health_check(self):
        metric_prefix = "replica.{}.{}.".format(
            self._ray_serve_dequeue_requestr_name, self._ray_serve_replica_id)
        metric = {
            metric_prefix + "healthy_timestamp_s": time.time(),
            metric_prefix + "latency_s": list(self._ray_serve_latency_buffer),
            metric_prefix + "error_counter": int(self._ray_serve_error_counter)
        }

        # reset internal buffer
        self._ray_serve_latency_buffer = []

        return metric

    def _ray_serve_setup(self, my_name, router_handle, replica_id):
        self._ray_serve_dequeue_requestr_name = my_name
        self._ray_serve_router_handle = router_handle
        self._ray_serve_replica_id = replica_id
        self._ray_serve_setup_completed = True

    def _ray_serve_main_loop(self, my_handle):
        assert self._ray_serve_setup_completed
        self._ray_serve_self_handle = my_handle

        # If true, this means we still need to work on last work_token
        # because it was un-fullfiled after 1s timeout.
        if self._ray_serve_work_token_cache:
            work_token = self._ray_serve_work_token_cache
        else:
            work_token = ray.get(
                self._ray_serve_router_handle.dequeue_request.remote(
                    self._ray_serve_dequeue_requestr_name,
                    self._ray_serve_replica_id))

        # Try to fetch the unit of work with 1s timeout.
        ready, not_ready = ray.wait([ray.ObjectID(work_token)], timeout=1.0)

        # There isn't a unit of work for us. Cache the token and re-schedule
        # the main_loop
        if len(not_ready) == 1:
            self._ray_serve_self_handle._ray_serve_main_loop.remote(my_handle)
            self._ray_serve_work_token_cache = work_token
            return

        # We found work to do.
        work_item = ray.get(ready[0])
        self._ray_serve_work_token_cache = None

        # TODO(simon):
        # __call__ should be able to take multiple *args and **kwargs.
        start = time.time()
        result, failed = wrap_to_ray_error(self.__call__,
                                           work_item.request_body)

        # insert values into metric buffer
        self._ray_serve_latency_buffer.append(time.time() - start)
        if failed:
            self._ray_serve_error_counter += 1

        # put the result into result_object_id
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
