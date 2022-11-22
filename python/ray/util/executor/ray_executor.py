import ray
from concurrent.futures import Executor


class RayExecutor(Executor):

    def __init__(self, **kwargs):
        ray.init(**kwargs)

    @staticmethod
    @ray.remote
    def __remote_fn(fn, arg, **kwargs):
        return fn(arg, **kwargs)

    def submit(self, fn, /, *args, **kwargs):
        return self.__remote_fn.remote(fn, *args, **kwargs).future()

    def map(self, func, *iterables, timeout=None, chunksize=1):
        # self.timeout = timeout
        # return map(lambda i: ray.get(self.__remote_fn.remote(func, i)), iterables)
        raise NotImplemented

    def shutdown(self, wait=True, *, cancel_futures=False):
        ray.shutdown()


