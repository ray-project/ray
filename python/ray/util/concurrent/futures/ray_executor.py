from abc import ABC, abstractmethod
import time
from functools import partial
from concurrent.futures import Executor, Future
from typing import (
    Any,
    TYPE_CHECKING,
    TypeVar,
    TypedDict,
    Optional,
)
from collections.abc import Callable, Iterable, Iterator
import random

import ray
from ray.util.annotations import PublicAPI
import ray.exceptions

try:
    from concurrent.futures._base import _result_or_cancel  # type: ignore
except ImportError:
    # Backport private Python 3.12 function to Python 3.9
    # https://github.com/python/cpython/tree/main/Lib/concurrent/futures/_base.py#L306
    def _result_or_cancel(fut, timeout=None):
        try:
            try:
                return fut.result(timeout)
            finally:
                fut.cancel()
        finally:
            del fut


# Typing -----------------------------------------------

T = TypeVar("T")

if TYPE_CHECKING:
    from ray._private.worker import BaseContext
    from ray.actor import ActorHandle


class _PoolActor(TypedDict):
    actor: "ActorHandle"
    task_count: int


# ------------------------------------------------------


class _ActorPoolBase(ABC):

    """This interface defines the actor pool class of Ray actors used by
    RayExecutor."""

    @property
    @abstractmethod
    def max_tasks_per_actor(self) -> Optional[int]:
        ...

    @max_tasks_per_actor.setter
    @abstractmethod
    def max_tasks_per_actor(self, val: Optional[int]) -> None:
        ...

    @property
    @abstractmethod
    def num_actors(self) -> int:
        ...

    @num_actors.setter
    @abstractmethod
    def num_actors(self, val: int) -> None:
        ...

    @property
    def initializer(self) -> Optional[Callable[..., Any]]:
        ...

    @initializer.setter
    @abstractmethod
    def initializer(self, val: Optional[Callable[..., Any]]) -> None:
        ...

    @property
    @abstractmethod
    def initargs(self) -> tuple[Any, ...]:
        ...

    @initargs.setter
    @abstractmethod
    def initargs(self, val: tuple[Any, ...]) -> None:
        ...

    @abstractmethod
    def submit(self, fn: Callable[[], T]) -> Future[T]:
        ...

    @abstractmethod
    def next(self) -> _PoolActor:
        ...

    @abstractmethod
    def get_actor_ids(self) -> list[str]:
        ...


class _AbstractActorPool(_ActorPoolBase, ABC):

    """Common actor pool methods and attributes. New actor pool types should
    extend this. See _RandomActorPool and _RoundRobinActorPool below."""

    def __init__(
        self,
        num_actors: int = 2,
        initializer: Optional[Callable[..., Any]] = None,
        initargs: tuple[Any, ...] = (),
        max_tasks_per_actor: Optional[int] = None,
    ) -> None:
        self.max_tasks_per_actor = max_tasks_per_actor
        self.num_actors = num_actors
        self.initializer = initializer
        self.initargs = initargs
        self.pool: list[_PoolActor] = [
            self._build_actor() for _ in range(self.num_actors)
        ]
        return

    @abstractmethod
    def next(self) -> _PoolActor:
        ...

    def get_actor_ids(self) -> list[str]:
        return [i["actor"]._ray_actor_id.hex() for i in self.pool]

    @property
    def max_tasks_per_actor(self) -> Optional[int]:
        return self._max_tasks_per_actor

    @max_tasks_per_actor.setter
    def max_tasks_per_actor(self, val: Optional[int]) -> None:
        if val is not None:
            if val < 1:
                raise ValueError(
                    f"max_tasks_per_child={val} was given. The argument \
                    max_tasks_per_child must be >= 1 or None"
                )
        self._max_tasks_per_actor = val
        return

    @property
    def num_actors(self) -> int:
        return self._num_actors

    @num_actors.setter
    def num_actors(self, val: int) -> None:
        if val < 1:
            raise ValueError("Pool must contain at least one Actor")
        self._num_actors = val
        return

    @property
    def initializer(self) -> Optional[Callable[..., Any]]:
        return self._initializer

    @initializer.setter
    def initializer(self, val: Optional[Callable[..., Any]]) -> None:
        if val is not None:
            if not callable(val):
                raise TypeError("initializer must be callable")
        self._initializer = val
        return

    @property
    def initargs(self) -> tuple[Any, ...]:
        return self._initargs

    @initargs.setter
    def initargs(self, val: tuple[Any, ...]) -> None:
        if not isinstance(val, tuple):
            raise TypeError("initargs must be tuple")
        self._initargs = val
        return

    def _build_actor(self) -> _PoolActor:
        if not ray.is_initialized():
            raise ray.exceptions.RayError("No existing ray instance")

        @ray.remote
        class ExecutorActor:
            def __init__(
                self,
                initializer: Optional[Callable[..., Any]] = None,
                initargs: tuple[Any, ...] = (),
            ) -> None:
                self.initializer = initializer
                self.initargs = initargs

            def actor_function(self, fn: Callable[[], T]) -> T:
                if self.initializer is not None:
                    self.initializer(*self.initargs)
                return fn()

            def exit(self) -> None:
                ray.actor.exit_actor()

        actor = ExecutorActor.options().remote(  # type: ignore[attr-defined]
            self.initializer, self.initargs
        )
        return {
            "actor": actor,
            "task_count": 0,
        }

    def submit(self, fn: Callable[[], T]) -> Future[T]:
        """
        Submit a task to be executed on the actor.

        Parameters
        ----------
        fn : Callable
            This 0-arity function will be executed as a task on the actor.

        Returns
        -------
        Future
            A future representing the result of the task.

        """

        pool_actor = self._replace_actor_if_max_tasks()
        fut = pool_actor["actor"].actor_function.remote(fn).future()
        self._increment_task_count(pool_actor)
        return fut  # type: ignore

    def kill(self) -> None:
        """
        Kill all of the actors in the pools without waiting for their tasks to complete
        """
        for i in self.pool:
            self._kill_actor(i)
        return

    def _replace_actor_if_max_tasks(self) -> _PoolActor:
        pool_actor = self.next()
        if self.max_tasks_per_actor is not None:
            if pool_actor["task_count"] >= self.max_tasks_per_actor:
                self._exit_actor(pool_actor)
                pool_actor = self._build_actor()
                self.pool.append(pool_actor)
        return pool_actor

    def _increment_task_count(self, pool_actor: _PoolActor) -> None:
        pool_actor["task_count"] += 1
        return

    def _kill_actor(self, pool_actor: _PoolActor) -> None:
        ray.kill(pool_actor["actor"])
        return

    def _exit_actor(self, pool_actor: _PoolActor) -> "ActorHandle":
        """
        Gracefully exit the actor and allow running jobs to finish.
        """
        self.pool = [i for i in self.pool if i != pool_actor]
        pool_actor["actor"].exit.remote()
        return pool_actor["actor"]


class _RandomActorPool(_AbstractActorPool):

    """This class manages a pool of Ray actors by distributing tasks amongst
    them in a random choice fashion. Functions are executed remotely in the
    actor pool using submit().

    ...

    Attributes
    -----------
    num_actors : int
        Specify the size of the actor pool to create.
    initializer : Callable
        A function that will be called remotely in the actor context before the
        submitted task (for compatibility with
        concurrent.futures.ThreadPoolExecutor).
    initargs : tuple
        Arguments for initializer (for compatibility with
        concurrent.futures.ThreadPoolExecutor).
    max_tasks_per_actor : int
        The maximum number of tasks to be performed by an actor before it is
        gracefully killed and replaced (for compatibility with
        concurrent.futures.ProcessPoolExecutor).
    """

    def next(self) -> _PoolActor:
        """
        Get the next priority member of the actor pool

        Returns
        -------
        _PoolActor
            The current priority actor in the pool
        """
        return random.choice(self.pool)


class _RoundRobinActorPool(_AbstractActorPool):

    """This class manages a pool of Ray actors by distributing tasks amongst
    them in a simple round-robin fashion. Functions are executed remotely in
    the actor pool using submit().

    ...

    Attributes
    -----------
    num_actors : int
        Specify the size of the actor pool to create.
    initializer : Callable
        A function that will be called remotely in the actor context before the
        submitted task (for compatibility with
        concurrent.futures.ThreadPoolExecutor).
    initargs : tuple
        Arguments for initializer (for compatibility with
        concurrent.futures.ThreadPoolExecutor).
    max_tasks_per_actor : int
        The maximum number of tasks to be performed by an actor before it is
        gracefully killed and replaced (for compatibility with
        concurrent.futures.ProcessPoolExecutor).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.index: int = 0
        return

    def next(self) -> _PoolActor:
        """
        Get the next indexed member of the actor pool

        Returns
        -------
        _PoolActor
            The current priority actor in the pool
        """
        obj = self.pool[self.index]
        self.index += 1
        if self.index >= len(self.pool):
            self.index = 0
        return obj


@PublicAPI(stability="alpha")
class RayExecutor(Executor):

    """RayExecutor is a drop-in replacement for ProcessPoolExecutor and
    ThreadPoolExecutor from concurrent.futures but distributes and executes
    the specified tasks over a pool of dedicated actors belonging to a Ray
    cluster instead of multiple processes or threads, respectively.

    Attributes
    ----------
    max_workers : int | None
        The number of actors to spawn. If max_workers=None, the work is
        distributed over a number of actors equal to the number of CPU
        resources observed by the Ray instance.
    shutdown_ray : bool | None
        Destroy the Ray cluster when self.shutdown() is called. If this is
        None, RayExecutor will default to shutting down the Ray instance only
        if it was initialised during instantiation of the current RayExecutor,
        otherwise it will not be shutdown.
    initializer : Callable[..., Any] | None
        A function that will be called remotely in the actor context before the
        submitted task (for compatibility with
        concurrent.futures.ThreadPoolExecutor).
    initargs : tuple[Any, ...] | None
        Arguments for initializer (for compatibility with
        concurrent.futures.ThreadPoolExecutor).
    max_tasks_per_child : int | None
        The maximum number of tasks to be performed by an actor before it is
        gracefully killed and replaced (for compatibility with
        concurrent.futures.ProcessPoolExecutor).
    mp_context : Any | None
        This is only included for compatibility with
        concurrent.futures.ProcessPoolExecutor but is unused.
    actor_pool_type: str | None
        The type of actor pool to use: either 'random' (which will assign tasks
        to randomly selected actors), or 'roundrobin' (which will simply assign
        tasks to actors sequentially.

    All additional keyword arguments are passed to ray.init()
    (see https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init).

    For example, this will connect to a cluster at the specified address:
    .. testcode::

        RayExecutor(address='192.168.0.123:25001')

    Note: excluding an address will initialise a local Ray cluster.
    """

    #: Aggregated Futures from initiated tasks.
    futures: list[Future[Any]]
    #: A container of Actor objects over which the tasks will be distributed.
    actor_pool: _AbstractActorPool

    def __init__(
        self,
        max_workers: Optional[int] = None,
        shutdown_ray: Optional[bool] = None,
        initializer: Optional[Callable[..., Any]] = None,
        initargs: tuple[Any, ...] = (),
        max_tasks_per_child: Optional[int] = None,
        mp_context: Optional[Any] = None,
        actor_pool_type: str = "random",
        **kwargs: Any,
    ):

        """RayExecutor handles existing Ray instances and shutting
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
        """
        self._shutdown_lock: bool = False
        self._initialised_ray: bool = (not ray.is_initialized()) and (
            "address" not in kwargs
        )
        self._context: "BaseContext" = ray.init(ignore_reinit_error=True, **kwargs)
        self.futures: list[Future[Any]] = []
        self.shutdown_ray = shutdown_ray

        # mp_context is included for API compatiblity only, it does nothing in
        # this context
        self._mp_context = mp_context

        if max_tasks_per_child is not None:
            if max_tasks_per_child < 1:
                raise ValueError(
                    f"max_tasks_per_child={max_tasks_per_child} was given. The argument \
                    max_tasks_per_child must be >= 1 or None"
                )
        self.max_tasks_per_child = max_tasks_per_child

        if initializer is not None:
            runtime_env = kwargs.get("runtime_env")
            if runtime_env is None or "working_dir" not in runtime_env:
                raise ValueError(
                    "working_dir must be specified in runtime_env dictionary if \
                                 initializer function is not None so that \
                                 it can be accessible by remote workers"
                )

        if max_workers is None:
            max_workers = int(ray._private.state.cluster_resources()["CPU"])
        if max_workers < 1:
            raise ValueError(
                f"max_workers={max_workers} as given. The argument "
                "max_workers must be >= 1",
            )
        self.max_workers = max_workers

        actor_pool_type_err = ValueError(
            "actor_pool_type must be either 'random' or 'roundrobin'",
        )
        if not isinstance(actor_pool_type, str):
            raise actor_pool_type_err
        if actor_pool_type.lower() == "random":
            self.actor_pool = _RandomActorPool(
                num_actors=max_workers,
                initializer=initializer,
                initargs=initargs,
                max_tasks_per_actor=self.max_tasks_per_child,
            )
        elif actor_pool_type.lower() == "roundrobin":
            self.actor_pool = _RoundRobinActorPool(
                num_actors=max_workers,
                initializer=initializer,
                initargs=initargs,
                max_tasks_per_actor=self.max_tasks_per_child,
            )
        else:
            raise actor_pool_type_err

        return

    def submit(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        r"""
        Submits a function to be executed in the actor pool with the given arguments.

        Parameters
        -----------
        fn : Callable[]
            A function to be executed in the actor pool as fn(\*args, \*\*kwargs)

        Returns
        -------
        Future
            A future representing the yet-to-be-resolved result of the
            submitted task. Futures are also collected in self.futures.

        Usage example:

        .. testcode::

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

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: Optional[float] = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        r"""
        Map a function over a series of iterables. Multiple series of iterables
        will be zipped together, and each zipped tuple will be treated as a
        single set of arguments.

        Parameters
        ----------
        fn : Callable[]
            A function to be executed in the actor pool which will take as many
            arguments as there are iterables.
        timeout : float | None
            The maximum number of seconds to wait for the tasks to complete. If
            None, then there is no limit on the wait time.
        chunksize : int
            This has no effect and is included merely to retain compatibility
            with concurrent.futures.Executor.

        Returns
        -------
        Iterator
            An iterator equivalent to: map(func, \*iterables) but the calls may
            be evaluated out-of-order.

        Raises
        ------
            TimeoutError: If the entire result iterator could not be generated
                before the given timeout.
            Exception: If fn(\*args) raises for any values.

        Usage example 1:

        .. testcode::

            with RayExecutor() as ex:
                futures = ex.map(lambda x: x * x, [100, 100, 100])
                results = [future.result() for future in futures]

        Usage example 2:

        .. testcode::

            def f(x, y):
                return x * y

            with RayExecutor() as ex:
                futures_iter = ex.map(f, [100, 100, 100], [1, 2, 3])
                assert [i for i in futures_iter] == [100, 200, 300]

        """
        self._check_shutdown_lock()

        # this is unused and merely for  compatibility with concurrent.futures.Executor
        self.chunksize = chunksize

        end_time = None
        if timeout is not None:
            end_time = timeout + time.monotonic()
        fs = [self.submit(fn, *args) for args in zip(*iterables)]

        # from concurrent.futures.Executor.map()
        def result_iterator() -> Iterator[T]:
            try:
                # reverse to keep finishing order
                fs.reverse()
                while fs:
                    # Careful not to keep a reference to the popped future
                    if end_time is None:
                        yield _result_or_cancel(fs.pop())
                    else:
                        yield _result_or_cancel(fs.pop(), end_time - time.monotonic())
            finally:
                for future in fs:
                    future.cancel()

        return result_iterator()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. No other methods can be
        called after this one.

        Parameters
        ----------
        wait : bool
            If True then shutdown will not return until all running futures
            have finished executing and the resources used by the executor have
            been reclaimed.
        cancel_futures : bool
            If True then shutdown will cancel all pending futures. Futures that
            are completed or running will not be cancelled.
        """

        if self.shutdown_ray is None:
            if self._initialised_ray:
                self._shutdown_ray(wait, cancel_futures)
        else:
            if self.shutdown_ray:
                self._shutdown_ray(wait, cancel_futures)

        del self.futures
        self.futures = []
        return

    def _shutdown_ray(self, wait: bool = True, cancel_futures: bool = False) -> None:
        self._shutdown_lock = True

        if cancel_futures:
            for future in self.futures:
                _ = future.cancel()

        if wait:
            for future in self.futures:
                _ = future.result()

        ray.shutdown()
        return

    def _check_shutdown_lock(self) -> None:
        if self._shutdown_lock:
            raise RuntimeError("New task submitted after shutdown() was called")
        return
