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
from ray.util.annotations import PublicAPI
import ray.exceptions

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

    Args:
        max_workers:
            If max_workers=None, the work is distributed over the number of
            CPUs available in the cluster (num_cpus) , otherwise the number of
            CPUs is limited to the value of max_workers.

        All additional keyword arguments are passed to ray.init()
        (see https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init).

        For example, this will connect to a cluster at the specified address:
        .. code-block:: python

            RayExecutor(address='192.168.0.123:25001')

        Note: excluding an address will initialise a local Ray cluster.
    """

    def __init__(
        self, max_workers: Optional[int] = None, shutdown_ray: bool = True, **kwargs
    ):

        """
        Initialise a new RayExecutor instance which distributes tasks over
        a Ray cluster.

        self._shutdown_ray:
            If False, the Ray cluster is not destroyed when self.shutdown() is
            called. This prevents initialising a new cluster every time the
            executor is used in a `with` context. Futures will be
            destroyed regardless of the value of `_shutdown_ray`.
        self._shutdown_lock:
            This is set to True once self.shutdown() is called. Further task
            submissions are blocked.
        self._futures:
            Futures are aggregated into this list as they are returned.
        self.__remote_fn:
            Wrapper around the remote function to be executed by Ray.
        self.context:
            Context containing settings and attributes returned after
            initialising the Ray client.
        """

        self._shutdown_ray: bool = shutdown_ray
        self._shutdown_lock: bool = False
        self._futures: List[Future] = []
        self.context: "Optional[BaseContext]" = None

        @ray.remote
        def remote_fn(fn: Callable[[], T]) -> T:
            return fn()

        self.__remote_fn: "RemoteFunction0" = remote_fn

        if max_workers is not None:
            if max_workers < 1:
                raise ValueError(
                    f"`max_workers={max_workers}` is given. The argument \
                    `max_workers` must be >= 1"
                )
            self.max_workers = max_workers
            kwargs["num_cpus"] = max_workers
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

        future = (
            self.__remote_fn.options(name=fn.__name__)
            .remote(fn_curried)  # type: ignore
            .future()
        )
        self._futures.append(future)
        del fn_curried
        return future

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

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: Optional[float] = None,
    ) -> Iterator[T]:
        """Returns an iterator equivalent to `map(fn, iter)`.

        Args:
            fn: A callable that will take as many arguments as there are
                passed iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.

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

        if timeout is not None:
            end_time = timeout + time.monotonic()

        fs = [self.submit(fn, *args) for args in zip(*iterables)]

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
        if self._shutdown_ray:
            self._shutdown_lock = True

            if cancel_futures:
                for future in self._futures:
                    _ = future.cancel()

            if wait:
                for future in self._futures:
                    if future.running():
                        _ = future.result()

            ray.shutdown()
        del self._futures
        self._futures = []

    def _check_shutdown_lock(self) -> None:
        if self._shutdown_lock:
            raise RuntimeError("New task submitted after shutdown() was called")
