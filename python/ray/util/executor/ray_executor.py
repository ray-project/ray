from functools import partial
import itertools
import ray
import time
from concurrent.futures import Executor, ProcessPoolExecutor



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
    _shutdown_lock = False
    _futures = {}

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
        self._check_shutdown_lock()
        oref = self.__remote_fn.remote(fn, *args, **kwargs)
        future = oref.future()
        self._futures[oref] = future
        return future

    def submit_actor_function(self, fn, *args, **kwargs):
        self._check_shutdown_lock()
        oref = self.__actor_fn(fn, *args, **kwargs)
        future = oref.future()
        self._futures[oref] = future
        return future

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        self._check_shutdown_lock()
        results_list = []
        for chunk in self._get_chunks(*iterables, chunksize=chunksize):
            results = self._map(self.submit, fn, chunk, timeout)
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
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
            wait: If True then shutdown will not return until all running
                futures have finished executing and the resources used by the
                executor have been reclaimed.
            cancel_futures: If True then shutdown will cancel all pending
                futures. Futures that are completed or running will not be
                cancelled.
        """
        self._shutdown_lock = True

        if cancel_futures:
            for future in self._futures.values():
                _ = future.cancel()

        if wait:
            for future in self._futures.values():
                if future.running():
                    _ = future.result()

        ray.shutdown()

    def _check_shutdown_lock(self):
        if self._shutdown_lock:
            raise RuntimeError('New task submitted after shutdown() was called')
