import logging
import time
from abc import ABC, abstractmethod
from concurrent.futures import Future, ProcessPoolExecutor
from typing import (
    Any,
    Generic,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

logger = logging.getLogger(__name__)

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class AsyncCallee(ABC, Generic[InputT, OutputT]):
    """A unit of work that runs asynchronously in a worker process.

    Subclasses implement ``run()`` to perform expensive computation (CPU-bound)
    or concurrent I/O.

    Set ``min_interval_s`` to throttle how often the task is submitted.
    The ``AsyncServiceHandle`` enforces this on the scheduling thread.
    """

    min_interval_s: float = 0

    @abstractmethod
    def run(self, obj: InputT) -> OutputT:
        ...


@runtime_checkable
class AsyncCaller(Protocol):
    """Protocol for components that participate in async state refresh.

    Both ``PhysicalOperator`` and ``ExecutionCallback`` can implement this
    via structural subtyping — no inheritance required.

    The executor manages the submit/poll lifecycle for all registered
    refreshables; components only implement these three methods.

    The flow each scheduling step:
        1. ``build_refresh_input(executor)`` — snapshot state (scheduling thread)
        2. ``task.run(snapshot)`` — expensive work (worker process)
        3. ``apply_refresh_result(result, executor)`` — apply output (scheduling thread)
    """

    def create_async_task(self) -> Optional["AsyncCallee"]:
        """Return the task to register with the async service, or None to skip."""
        ...

    def build_refresh_input(self) -> Any:
        """Build a serializable input snapshot for the async task.

        Called on the scheduling thread. Has access to live state of the
        implementing object.
        """
        ...

    def apply_refresh_result(self, result: Any) -> None:
        """Apply the result from the async task back to internal state.

        Called on the scheduling thread when the result is ready.
        """
        ...


def _run_task(callee: AsyncCallee, state: Any) -> Any:
    """Top-level function so pickle can serialize the work item."""
    return callee.run(state)


_pool: Optional[ProcessPoolExecutor] = None


def _get_pool() -> ProcessPoolExecutor:
    """Lazily create a module-level singleton ProcessPoolExecutor(max_workers=1).

    Shared across all datasets/executors, analogous to the singleton Ray actor
    in the actor-based implementation.
    """
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=1)
    return _pool


class AsyncServiceHandle(Generic[InputT, OutputT]):
    """Client-side handle for non-blocking interaction with an AsyncCallee.

    Used in the scheduling loop to submit snapshots and poll for
    results without blocking.
    """

    def __init__(self, task: AsyncCallee):
        self._task = task
        self._min_interval_s = task.min_interval_s
        self._pending_future: Optional[Future] = None
        self._last_submit_time: float = 0

    def should_submit(self) -> bool:
        """True if no request is in-flight and the minimum interval has elapsed."""
        if self._pending_future is not None:
            return False
        if self._min_interval_s > 0:
            return time.monotonic() - self._last_submit_time >= self._min_interval_s
        return True

    def request_refresh(self, state: InputT) -> None:
        """Submit state to the worker process for async computation.

        Only call when ``should_submit()`` returns True.
        """
        assert self._pending_future is None, "Previous refresh still in flight"
        self._last_submit_time = time.monotonic()
        self._pending_future = _get_pool().submit(_run_task, self._task, state)

    def try_get_result(self) -> Optional[OutputT]:
        """Non-blocking poll. Returns the result if ready, None otherwise."""
        assert self._pending_future is not None, "No pending future yet"
        if not self._pending_future.done():
            return None

        try:
            result = self._pending_future.result(timeout=0)
            self._pending_future = None
            return result
        except Exception as e:
            logger.debug(f"Async service task failed: {e}")
            self._pending_future = None
            raise

    def has_in_flight_request(self) -> bool:
        return self._pending_future is not None

    def shutdown(self) -> None:
        if self._pending_future is not None:
            self._pending_future.cancel()
            self._pending_future = None


def shutdown_pool() -> None:
    """Shut down the module-level ProcessPoolExecutor.

    Call during interpreter shutdown or test teardown. In production the pool
    is left alive (like the detached Ray actor) so subsequent datasets reuse it.
    """
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=False)
        _pool = None
