from functools import partial
import itertools
import ray
import time
from concurrent.futures import Executor


def _result_or_cancel(fut, timeout=None):
    """
    From concurrent.futures
    """
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
        self.context = ray.init(ignore_reinit_error=True, **kwargs)

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

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        results_list = []
        for chunk in self._get_chunks(*iterables, chunksize=chunksize):
            results = self._map(self.submit, fn, chunk, timeout=None)
            results_list.append(results)
        return itertools.chain(*results_list)

    #        return self._map(self.submit, fn, *iterables, timeout=timeout)

    def map_actor_function(self, fn, *iterables, timeout=None, chunksize=1):
        results_list = []
        for chunk in self._get_chunks(*iterables, chunksize=chunksize):
            results = self._map(self.submit_actor_function, fn, chunk, timeout=None)
            results_list.append(results)
        # results = self._map(self.submit_actor_function, fn,*iterables, timeout=None)
        return itertools.chain(*results_list)

    @staticmethod
    def _get_chunks(*iterables, chunksize):
        """
        https://github.com/python/cpython/blob/main/Lib/concurrent/futures/process.py#L186
        Iterates over zip()ed iterables in chunks.
        """
        it = zip(*iterables)
        while True:
            chunk = tuple(itertools.islice(it, chunksize))
            if not chunk:
                return
            yield chunk

    @staticmethod
    def _map(submit_fn, fn, iterables, timeout=None):
        """
        This was adapted from concurrent.futures.Executor.map.
        """
        if timeout is not None:
            end_time = timeout + time.monotonic()

        fs = [submit_fn(fn, *args) for args in iterables]

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

    def shutdown(self, wait=True, *, cancel_futures=False):
        ray.shutdown()
