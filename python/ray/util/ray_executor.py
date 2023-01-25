import itertools
import time
from concurrent.futures import Executor, Future
from functools import partial
from typing import (
    Callable,
    Optional,
    List,
    Any,
    TypeVar,
    ParamSpec,
    Iterable,
    Iterator,
    TYPE_CHECKING,
)

import ray
from ray.util.actor_pool import ActorPool
from ray.util.annotations import PublicAPI


# Typing -----------------------------------------------

if TYPE_CHECKING:
    from ray._private.worker import BaseContext, RemoteFunction0

T = TypeVar("T")
P = ParamSpec("P")

# ------------------------------------------------------


@PublicAPI(stability="alpha")
class RayExecutor(Executor):
    """`RayExecutor` is a drop-in replacement for `ProcessPoolExecutor` and
    `ThreadPoolExecutor` from `concurrent.futures` but distributes and executes
    the specified tasks over a Ray cluster instead of multiple processes or
    threads.

    It initialises a new RayExecutor instance which distributes tasks over
    a Ray cluster.

    Args:
        max_workers:
            If the maximum number of Ray workers is given, task is distributed
            over a ray Actor pool, otherwise it is distributed over the cpus of
            the cluster

        All additional keyword arguments are passed to ray.init()
        (see https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init).

        For example, this will connect to a cluster at the specified address:
        .. code-block:: python

            RayExecutor(address='192.168.0.123:25001')

        Note: excluding an address will initialise a local Ray cluster.
    """

    def __init__(self, max_workers: Optional[int] = None, **kwargs):
        self._shutdown_lock: bool = False
        self._futures: List[Future] = []
        self.__actor_pool: Optional[ActorPool] = None
        self.__remote_fn: "Optional[RemoteFunction0]" = None
        self.context: "Optional[BaseContext]" = None

        # The following is necessary because `@ray.remote` is only available at runtime.
        if max_workers is None:

            @ray.remote
            def remote_fn(fn: Callable[[], T]) -> T:
                return fn()

            self.__remote_fn = remote_fn
        else:
            if max_workers < 1:
                raise ValueError(
                    f"max_workers={max_workers} given, max_workers must be >= 1"
                )

            @ray.remote
            class ExecutorActor:
                def __init__(self):
                    pass

                def actor_function(self, fn: Callable):
                    return fn()

            actors = [
                ExecutorActor.options(  # type: ignore[attr-defined]
                    name=f"actor-{i}"
                ).remote()
                for i in range(max_workers)
            ]
            self.__actor_pool = ray.util.ActorPool(actors)

        self.context = ray.init(ignore_reinit_error=True, **kwargs)

    def submit(
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as `fn(*args, **kwargs)` and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.

        Usage example:

        .. code-block:: python

            with RayExecutor() as ex:
                future_0 = ex.submit(lambda x: x * x, 100)
                future_1 = ex.submit(lambda x: x + x, 100)
                result_0 = future_0.result()
                result_1 = future_1.result()
        """
        self._check_shutdown_lock()
        fn_curried = partial(fn, *args, **kwargs)
        if self.__actor_pool:
            self.__actor_pool.submit(
                lambda a, _: a.actor_function.remote(fn_curried), None
            )
            oref = self.__actor_pool._index_to_future[
                self.__actor_pool._next_task_index - 1
            ]
        else:
            if self.__remote_fn:
                oref = self.__remote_fn.options(  # type: ignore
                    name=fn.__name__
                ).remote(fn_curried)
            else:
                raise RuntimeError("Remote function is undefined")

        future = oref.future()
        self._futures.append(future)
        return future

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: Optional[float] = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
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
            if self.__actor_pool:
                results = self.__actor_pool.map(
                    lambda a, v: a.actor_function.remote(  # type: ignore
                        partial(fn, *v)
                    ),
                    list(chunk),
                )
            else:
                results = self._map(fn, chunk, timeout)
            results_list.append(results)
        return itertools.chain(*results_list)

    @staticmethod
    def _get_chunks(
        *iterables: Iterable[Any], chunksize: int
    ) -> Iterator[tuple[tuple[Any, ...], ...]]:
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
    def _result_or_cancel(fut: Future[T], timeout: Optional[float] = None) -> T:
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

    def _map(
        self,
        fn: Callable[..., T],
        iterables: Iterable[tuple[Any, ...]],
        timeout: Optional[float] = None,
    ) -> Iterator[T]:
        """
        This was adapted from concurrent.futures.Executor.map.
        """
        if timeout is not None:
            end_time = timeout + time.monotonic()

        fs = [self.submit(fn, *args) for args in iterables]

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

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
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
            for future in self._futures:
                _ = future.cancel()

        if wait:
            for future in self._futures:
                if future.running():
                    _ = future.result()

        ray.shutdown()

    def _check_shutdown_lock(self) -> None:
        if self._shutdown_lock:
            raise RuntimeError("New task submitted after shutdown() was called")
