import itertools
import time
from concurrent.futures import Executor

import ray
from ray.util.annotations import PublicAPI


@PublicAPI
class RayExecutor(Executor):
    """`RayExecutor` is a drop-in replacement for `ProcessPoolExecutor` and
    `ThreadPoolExecutor` from `concurrent.futures` but distributes and executes
    the specified tasks over a Ray cluster instead of multiple processes or
    threads.
    """

    _shutdown_lock = False
    """A dictionary of futures and associated Ray object references generated
    by the current instance of RayExecutor"""
    _futures = {}

    def __init__(self, **kwargs):
        """Initialises a new RayExecutor instance which distributes tasks over
        a Ray cluster.

        Args:
            All keyword arguments are passed to ray.init() (see
            https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init).
            For example, this will connect to a cluster at the specified address:
            .. code-block:: python

                RayExecutor(address='192.168.0.123:25001')

            Note: excluding an address will initialise a local Ray cluster.

        """

        """
        The following is necessary because `@ray.remote` is only available at runtime.
        """

        @ray.remote
        def remote_fn(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        self.__remote_fn = remote_fn
        self.context = ray.init(ignore_reinit_error=True, **kwargs)

    @staticmethod
    def __actor_fn(fn, *args, **kwargs):
        return fn.remote(*args, **kwargs)

    def submit(self, fn, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as `fn(*args, **kwargs)` and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.

        Usage example:

        .. code-block:: python

            with RayExecutor() as ex:
                future = ex.submit(lambda x: x * x, 100)
                result = future.result()
        """
        self._check_shutdown_lock()
        oref = self.__remote_fn.remote(fn, *args, **kwargs)
        future = oref.future()
        self._futures[oref] = future
        return future

    def submit_actor_function(self, fn, *args, **kwargs):
        """Submits a callable Actor function (see
        https://docs.ray.io/en/latest/ray-core/actors.html) to be executed with
        the given arguments.

        Schedules the callable to be executed as `fn(*args, **kwargs)` and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.


        Usage example:

        .. code-block:: python

            @ray.remote
            class Actor0:
                def __init__(self, name):
                    self.name = name

                def actor_function(self, arg):
                    return f"{self.name}-Actor-{arg}"

            a = Actor0.options(name="A", get_if_exists=True).remote("A")
            with RayExecutor() as ex:
                future = ex.submit_actor_function(a.actor_function, 0)
                result = future.result()

        """
        self._check_shutdown_lock()
        oref = self.__actor_fn(fn, *args, **kwargs)
        future = oref.future()
        self._futures[oref] = future
        return future

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        """Returns an iterator equivalent to `map(fn, iter)`.

        Args:
            fn: A callable that will take as many arguments as there are
                passed iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: The size of the chunks the iterable will be broken into
                before being passed to a child process.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls may
            be evaluated out-of-order.

        Raises:
            TimeoutError: If the entire result iterator could not be generated
                before the given timeout.
            Exception: If `fn(*args)` raises for any values.

        Usage example:

        .. code-block:: python

            with RayExecutor() as ex:
                futures = ex.map(lambda x: x * x, [100, 100, 100])
                results = [future.result() for future in futures()]

        """
        self._check_shutdown_lock()
        results_list = []
        for chunk in self._get_chunks(*iterables, chunksize=chunksize):
            results = self._map(self.submit, fn, chunk, timeout)
            results_list.append(results)
        return itertools.chain(*results_list)

    def map_actor_function(self, fn, *iterables, timeout=None, chunksize=1):
        """Returns an iterator equivalent to `map(fn, iter)`.

        Args:
            fn: A callable Actor function (see
                https://docs.ray.io/en/latest/ray-core/actors.html) that will take
                as many arguments as there are passed iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: The size of the chunks the iterable will be broken into
                before being passed to a child process.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls may
            be evaluated out-of-order.

        Raises:
            TimeoutError: If the entire result iterator could not be generated
                before the given timeout.
            Exception: If `fn(*args)` raises for any values.

        Usage example:

        .. code-block:: python

            @ray.remote
            class Actor0:
                def __init__(self, name):
                    self.name = name

                def actor_function(self, arg):
                    return f"{self.name}-Actor-{arg}"

            a = Actor0.options(name="A", get_if_exists=True).remote("A")
            with RayExecutor(address=call_ray_start) as ex:
                futures = ex.map_actor_function(a.actor_function, [0, 0, 0])
                results = [future.result() for future in futures]
        """
        results_list = []
        for chunk in self._get_chunks(*iterables, chunksize=chunksize):
            results = self._map(self.submit_actor_function, fn, chunk, timeout=None)
            results_list.append(results)
        return itertools.chain(*results_list)

    @staticmethod
    def _get_chunks(*iterables, chunksize):
        """
        https://github.com/python/cpython/blob/main/Lib/concurrent/futures/process.py#L186
        Iterates over zip()-ed iterables in chunks.
        """
        it = zip(*iterables)
        while True:
            chunk = tuple(itertools.islice(it, chunksize))
            if not chunk:
                return
            yield chunk

    @staticmethod
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

    def _map(self, submit_fn, fn, iterables, timeout=None):
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
                        yield self._result_or_cancel(fs.pop())
                    else:
                        yield self._result_or_cancel(
                            fs.pop(), end_time - time.monotonic()
                        )
            finally:
                for future in fs:
                    future.cancel()

        return result_iterator()

    def shutdown(self, wait=True, *, cancel_futures=False):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. No other methods can be
        called after this one.

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
            raise RuntimeError("New task submitted after shutdown() was called")
