import ray
from concurrent.futures import Executor


class RayExecutor(Executor):

    def __init__(self, **kwargs):
        ray.init(**kwargs)

    @staticmethod
    @ray.remote
    def __remote_fn(fn, arg, **kwargs):
        return fn(arg, **kwargs) if args else fn()

    @staticmethod
    def __actor_fn(fn, args):
        return fn.remote(args) if args else fn.remote()

    def submit(self, fn, /, *args, **kwargs):
        return self.__remote_fn.remote(fn, *args, **kwargs).future()

    def submit_actors(self, fn, *args, **kwargs):
        return self.__actor_fn(fn, *args, **kwargs).future()

    def map(self, func, *iterables, timeout=None, chunksize=1):
        self.timeout = timeout
        # Use map for remote jobs
        # https://docs.ray.io/en/releases-1.10.0/ray-design-patterns/map-reduce.html
        # Don't use ray.get inside loops:
        # https://docs.ray.io/en/releases-1.10.0/ray-design-patterns/ray-get-loop.html
        map_remote = [self.__remote_fn.remote(func, i)) for i in iterables]
        return ray.get(map_remote)

    def shutdown(self, wait=True, *, cancel_futures=False):
        ray.shutdown()


