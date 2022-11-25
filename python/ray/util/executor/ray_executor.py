import ray
import time
from concurrent.futures import Executor

def _result_or_cancel(fut, timeout=None):
    try:
        try:
            return fut.result(timeout)
        finally:
            fut.cancel()
    finally:
        # Break a reference cycle with the exception in self._exception
        del fut

class RayExecutor(Executor):

    def __init__(self, **kwargs):
        ray.init(ignore_reinit_error=True, **kwargs)

    @staticmethod
    @ray.remote
    def __remote_fn(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    @staticmethod
    def __actor_fn(fn, *args, **kwargs):
        return fn.remote(*args, **kwargs)

    def submit(self, fn, /, *args, **kwargs):
        return self.__remote_fn.remote(fn, *args, **kwargs).future()

    def submit_actor_function(self, fn, *args, **kwargs):
        return self.__actor_fn(fn, *args, **kwargs).future()

    def map_actor_function(self, fn, *iterables, timeout=None, chunksize=1):
        if timeout is not None:
            end_time = timeout + time.monotonic()

        fs = [self.submit_actor_function(fn, *args) for args in zip(*iterables)]

        # Yield must be hidden in closure so that the futures are submitted
        # before the first iterator value is required.
        def result_iterator():
            try:
                # reverse to keep finishing order
                fs.reverse()
                while fs:
                    # Careful not to keep a reference to the popped future
                    if timeout is None:
                        yield _result_or_cancel(fs.pop())
                    else:
                        yield _result_or_cancel(fs.pop(), end_time - time.monotonic())
            finally:
                for future in fs:
                    future.cancel()
        return result_iterator()

    # def map(self, func, *iterables, timeout=None, chunksize=1):
    #     self.timeout = timeout
    #     # Use map for remote jobs
    #     # https://docs.ray.io/en/releases-1.10.0/ray-design-patterns/map-reduce.html
    #     # Don't use ray.get inside loops:
    #     # https://docs.ray.io/en/releases-1.10.0/ray-design-patterns/ray-get-loop.html
    #     futures = [self.__remote_fn.remote(func, i) for i in iterables]
    #     return ray.get(futures)

    def shutdown(self, wait=True, *, cancel_futures=False):
        ray.shutdown()


