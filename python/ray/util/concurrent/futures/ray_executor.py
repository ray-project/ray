import time
import os
from concurrent.futures import Executor, Future, TimeoutError as ConTimeoutError
from functools import partial
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    ParamSpec,
    TYPE_CHECKING,
    TypeVar,
)

import ray
from ray.util.annotations import PublicAPI
import ray.exceptions

# Typing -----------------------------------------------

T = TypeVar("T")
P = ParamSpec("P")

if TYPE_CHECKING:
    from ray._private.worker import BaseContext, RemoteFunction0
    from ray.actor import ActorHandle
    from ray.types import ObjectRef

class RoundRobinActorPool:
    pool: List["ActorHandle"]
    index: int = 0

    def __init__(self, seq: List["ActorHandle"]) -> None:
        if seq == [] or seq is None:
            raise ValueError("Pool must contain at least one Actor")
        self.pool = seq

    def next(self) -> "ActorHandle":
        obj = self.pool[self.index]
        self.index += 1
        if self.index >= len(self.pool):
            self.index = 0
        return obj

    def submit(self, fn: Callable[[], Any]) -> Future:
        return self.next().actor_function.remote(fn).future()


@PublicAPI(stability="alpha")  # type: ignore
class RayExecutor(Executor):
    """`RayExecutor` is a drop-in replacement for `ProcessPoolExecutor` and
    `ThreadPoolExecutor` from `concurrent.futures` but distributes and executes
    the specified tasks over a Ray cluster instead of multiple processes or
    threads.

    Args:
        max_workers:
            If max_workers=None, the work is distributed over the number of
            CPUs available in the cluster (num_cpus) , otherwise the number of
            CPUs is limited to the value of max_workers (this does not
            necessarily limit the number of parallel tasks, which is determined
            by how many CPUs each task uses).

        All additional keyword arguments are passed to ray.init()
        (see https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init).

        For example, this will connect to a cluster at the specified address:
        .. code-block:: python

            RayExecutor(address='192.168.0.123:25001')

        Note: excluding an address will initialise a local Ray cluster.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        shutdown_ray: bool = True,
        **kwargs: Any,
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
        self.futures:
            Futures are aggregated into this list as they are returned.
        self.__remote_fn:
            Wrapper around the remote function to be executed by Ray.
        self._context:
            Context containing settings and attributes returned after
            initialising the Ray client.
        """

        self._shutdown_ray: bool = shutdown_ray
        self._shutdown_lock: bool = False
        self.futures: List[Future[Any]] = []
        self._context: "Optional[BaseContext]" = None
        self.actor_pool: Optional[RoundRobinActorPool] = None

        if max_workers is None:
            # max_workers = int(ray._private.state.cluster_resources()["CPU"])
            cpus = os.cpu_count()
            if cpus is None:
                raise ValueError(f"max_workers was not supplied and could not determine number of cores")
            max_workers = cpus - 1
        elif max_workers < 1:
            raise ValueError(
                f"`max_workers={max_workers}` is given. The argument \
                `max_workers` must be >= 1"
            )
        self.max_workers = max_workers

        @ray.remote
        class ExecutorActor:
            def __init__(self) -> None:
                pass

            def actor_function(self, fn: Callable[[], T]) -> T:
                return fn()

        self._context = ray.init(ignore_reinit_error=True, **kwargs)

        if self.actor_pool is not None:
            for actor in self.actor_pool.pool:
                del actor
            del self.actor_pool
        self.actor_pool = RoundRobinActorPool([
            ExecutorActor.options(  # type: ignore[attr-defined]
                name=f"actor-{i}"
            ).remote()
            for i in range(max_workers)
        ])

    def submit(
        self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[T]:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as `fn(*args, **kwargs)` and
        returns a Future instance representing the execution of the callable.
        Futures are also collected in self.futures.

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
        if self.actor_pool is None:
            raise ValueError("actor_pool is not defined")
        future = self.actor_pool.submit(partial(fn, *args, **kwargs))
        self.futures.append(future)
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
        chunksize: int = 1,
    ) -> Iterator[T]:
        """
        Map a function over a series of iterables. Multiple series of iterables
        will be zipped together, and each zipped tuple will be treated as a
        single set of arguments.

        Args:
            fn: A callable that will take as many arguments as there are
                passed iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: chunksize has no effect and is included merely to retain
                the same type as super().map()

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls may
            be evaluated out-of-order.

        Raises:
            TimeoutError: If the entire result iterator could not be generated
                before the given timeout.
            Exception: If `fn(*args)` raises for any values.

        Usage example 1:

        .. code-block:: python

            with RayExecutor() as ex:
                futures = ex.map(lambda x: x * x, [100, 100, 100])
                results = [future.result() for future in futures]

        Usage example 2:

        .. code-block:: python

            def f(x, y):
                return x * y

            with RayExecutor() as ex:
                futures_iter = ex.map(f, [100, 100, 100], [1, 2, 3])
                assert [i for i in futures_iter] == [100, 200, 300]

        """
        raise NotImplementedError
        self._check_shutdown_lock()

        if timeout is not None:
            end_time = timeout + time.monotonic()
        fs = [self.submit(fn, *args) for args in zip(*iterables)]

        # Yield must be hidden in closure so that the futures are submitted
        # before the first iterator value is required.
        if self.actor_pool is not None:

            def result_iterator() -> Iterator[T]:
                assert self.actor_pool is not None
                while self.actor_pool.has_next():
                    if timeout is None:
                        yield self.actor_pool.get_next(timeout=None)
                    else:
                        try:
                            yield self.actor_pool.get_next(
                                timeout=end_time - time.monotonic()
                            )
                        except TimeoutError:
                            raise ConTimeoutError

        else:

            def result_iterator() -> Iterator[T]:
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
                for future in self.futures:
                    _ = future.cancel()

            if wait:
                for future in self.futures:
                    if future.running():
                        _ = future.result()
            ray.shutdown()
        del self.futures
        self.futures = []

    def _check_shutdown_lock(self) -> None:
        if self._shutdown_lock:
            raise RuntimeError("New task submitted after shutdown() was called")
