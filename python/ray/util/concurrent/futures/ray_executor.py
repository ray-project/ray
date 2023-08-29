import time
from concurrent.futures import Executor, Future
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
    Generator,
    Dict,
    TypedDict
)

import ray
from ray.util.annotations import PublicAPI
import ray.exceptions

# Typing -----------------------------------------------

T = TypeVar("T")
P = ParamSpec("P")

if TYPE_CHECKING:
    from ray._private.worker import BaseContext
    from ray.actor import ActorHandle

class PoolActor(TypedDict):
    actor: "ActorHandle"
    task_count: int

@ray.remote
class ExecutorActor:
    def __init__(self,
            initializer: Optional[Callable[..., Any]] = None,
            initargs: tuple[Any, ...] = (),
            ) -> None:
        self.initializer = initializer
        self.initargs = initargs
        pass

    def actor_function(self, fn: Callable[[], T]) -> T:
        if self.initializer is not None:
            self.initializer(*self.initargs)
        return fn()

    def exit(self) -> None:
        ray.actor.exit_actor()

class RoundRobinActorPool:

    def __init__(self,
            num_actors: int = 2,
            initializer: Optional[Callable[..., Any]] = None,
            initargs: tuple[Any, ...] = (),
            max_tasks_per_actor: Optional[int] = None
            ) -> None:

        if max_tasks_per_actor is not None:
            if max_tasks_per_actor < 1:
                raise ValueError(
                    f"`max_tasks_per_child={max_tasks_per_actor}` was given. The argument \
                    `max_tasks_per_child` must be >= 1 or None"
                )
        self.max_tasks_per_actor = max_tasks_per_actor
        if num_actors < 1:
            raise ValueError("Pool must contain at least one Actor")
        self.initializer = initializer
        self.initargs = initargs
        self.pool: Dict[int, PoolActor] = {i: self._build_actor()  for i in range(num_actors)}
        self.index: int = 0

    def next(self) -> "ActorHandle":
        obj = self.pool[self.index]["actor"]
        self.index += 1
        if self.index >= len(self.pool):
            self.index = 0
        return obj

    def _build_actor(self) -> PoolActor:
        return {"actor": ExecutorActor.options().remote(self.initializer, self.initargs), "task_count": 0} # type: ignore[attr-defined]

    def _replace_actor_if_max_tasks(self) -> None:
        if self.max_tasks_per_actor is not None:
            if self.pool[self.index]["task_count"] >= self.max_tasks_per_actor:
                self._exit_actor(self.index)
                self.pool[self.index] = self._build_actor()

    def submit(self, fn: Callable[[], T]) -> Future[T]:
        self._replace_actor_if_max_tasks()
        self._increment_task_count()
        return self.next().actor_function.remote(fn).future()  # type: ignore

    def _increment_task_count(self) -> None:
        self.pool[self.index]["task_count"] += 1

    def kill(self) -> None:
        for i in self.pool:
            self._kill_actor(i)

    def _kill_actor(self, i: int) -> None:
        pool_actor = self.pool[i]
        ray.kill(pool_actor["actor"])

    def _exit_actor(self, i: int) -> None:
        """
        Gracefully exit the actor and allow running jobs to finish.
        """
        pool_actor = self.pool.pop(i)
        pool_actor["actor"].exit.remote()


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
        shutdown_ray: Optional[bool] = None,
        initializer: Optional[Callable[..., Any]] = None,
        initargs: tuple[Any, ...] = (),
        mp_context: Optional[Any] = None,
        max_tasks_per_child: Optional[int] = None,
        **kwargs: Any,
    ):

        """Initialise a new RayExecutor instance which distributes tasks over
        a Ray cluster. RayExecutor handles existing Ray instances and shutting
        down as follows:

            1. If an existing cluster is discovered in the current scope,
            RayExecutor will simply connect to this cluster. By default, this
            cluster will not be destroyed when RayExecutor.shutdown() is
            called.

            2. If no existing cluster is discovered in the current scope,
            RayExecutor will instantiate a new Ray cluster instance. By
            default, this cluster will be destroyed when RayExecutor.shutdown()
            is called.

            3. If RayExecutor.shutdown_ray is set to True or False, this will
            override the behaviour above.

        self.shutdown_ray:
            Destroy the Ray cluster when self.shutdown() is called. If this is
            `None`, RayExecutor will default to shutting down the Ray instance
            if it was initialised during instantiation of the RayExecutor,
            otherwise it will not be shutdown (see above).

        self.futures:
            Futures are aggregated into this list as they are returned.

        self.actor_pool:
            An object containing a set of Actor objects over which the tasks will be distributed.
        """
        self._shutdown_lock: bool = False
        self._initialised_ray: bool = not ray.is_initialized()
        self._context: "BaseContext" = ray.init(ignore_reinit_error=True, **kwargs)
        self.futures: List[Future[Any]] = []
        self.shutdown_ray = shutdown_ray

        # mp_context is included for API consistency only, it does nothing in this context
        self._mp_context = mp_context

        if max_tasks_per_child is not None:
            if max_tasks_per_child < 1:
                raise ValueError(
                    f"`max_tasks_per_child={max_tasks_per_child}` was given. The argument \
                    `max_tasks_per_child` must be >= 1 or None"
                )
        self.max_tasks_per_child = max_tasks_per_child

        if initializer is not None:
            runtime_env = kwargs.get("runtime_env")
            if runtime_env is None or "working_dir" not in runtime_env:
                raise ValueError(f"`working_dir` must be specified in `runtime_env` dictionary if \
                                 `initializer` function is not `None` so that \
                                 it can be accessible by remote workers")

        if max_workers is None:
            max_workers = int(ray._private.state.cluster_resources()["CPU"])
        if max_workers < 1:
            raise ValueError(
                f"`max_workers={max_workers}` as given. The argument \
                `max_workers` must be >= 1"
            )
        self.max_workers = max_workers

        self.actor_pool = RoundRobinActorPool(
                num_actors=max_workers,
                initializer=initializer,
                initargs=initargs,
                max_tasks_per_actor=self.max_tasks_per_child
                )

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

        # def wrapped_fn(inner_fn, *args, initializer=None, initargs=(), **kwargs):
        #     if initializer is not None:
        #         initializer(initargs)
        #     return partial(inner_fn, *args, **kwargs)

        # curried_wrapped_function = wrapped_fn(fn, *args, initializer=self.initializer, initargs=self.initargs, **kwargs)
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
        self._check_shutdown_lock()

        end_time = None
        if timeout is not None:
            end_time = timeout + time.monotonic()
        fs = [self.submit(fn, *args) for args in zip(*iterables)]

        def result_iterator() -> Iterator[T]:
            try:
                # reverse to keep finishing order
                fs.reverse()
                while fs:
                    # Careful not to keep a reference to the popped future
                    if end_time is None:
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

        #    ┌───────────────┬──────────────────┬───────────────┐
        #    │               │                  │               │
        #    │ shutdown_ray  │ _initialised_ray │ shutdown      │
        #    │               │                  │               │
        #    ├───────────────┼──────────────────┼───────────────┤
        #    │               │                  │               │
        #    │   None        │    True          │ Yes           │
        #    │               │                  │               │
        #    ├───────────────┼──────────────────┼───────────────┤
        #    │               │                  │               │
        #    │   None        │    False         │ No            │
        #    │               │                  │               │
        #    ├───────────────┼──────────────────┼───────────────┤
        #    │               │                  │               │
        #    │   True        │    True/False    │ Yes           │
        #    │               │                  │               │
        #    ├───────────────┼──────────────────┼───────────────┤
        #    │               │                  │               │
        #    │   False       │    True/False    │ No            │
        #    │               │                  │               │
        #    └───────────────┴──────────────────┴───────────────┘


        if self.shutdown_ray is None:
            if self._initialised_ray:
                self._shutdown_ray(wait, cancel_futures)
        else:
            if self.shutdown_ray:
                self._shutdown_ray(wait, cancel_futures)

        del self.futures
        self.futures = []

    def _shutdown_ray(self, wait: bool = True, cancel_futures: bool = False) -> None:
        self._shutdown_lock = True

        if cancel_futures:
            for future in self.futures:
                _ = future.cancel()

        if wait:
            for future in self.futures:
                if future.running():
                    _ = future.result()

        ray.shutdown()

    def _check_shutdown_lock(self) -> None:
        if self._shutdown_lock:
            raise RuntimeError("New task submitted after shutdown() was called")
