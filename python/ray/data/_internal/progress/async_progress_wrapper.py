import concurrent.futures
import logging
import queue
import threading
import time
import typing
from typing import Optional

from ray.data._internal.progress.base_progress import BaseExecutionProgressManager

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)


class _DaemonThreadPoolExecutor:
    """
    ThreadPoolExecutor that uses daemon threads.

    thread factory not compatible with older python versions
    + parameters of concurrent.futures.thread are not forward or backward compatible across versions.
    """

    def __init__(
        self, max_workers=1, thread_name_prefix="", initializer=None, initargs=()
    ):
        self._max_workers = max_workers
        self._thread_name_prefix = thread_name_prefix
        self._initializer = initializer
        self._initargs = initargs
        self._work_queue: queue.SimpleQueue = queue.SimpleQueue()
        self._threads: set[threading.Thread] = set()
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()

    def submit(self, fn, *args, **kwargs) -> concurrent.futures.Future:
        """Submit a callable to be executed by a daemon worker thread."""
        if self._shutdown_event.is_set():
            raise RuntimeError("cannot schedule new futures after shutdown")
        f: concurrent.futures.Future = concurrent.futures.Future()
        self._work_queue.put((f, fn, args, kwargs))
        self._adjust_thread_count()
        return f

    def _adjust_thread_count(self) -> None:
        with self._lock:
            if len(self._threads) < self._max_workers:
                idx = len(self._threads)
                t = threading.Thread(
                    name=f"{self._thread_name_prefix}_{idx}",
                    target=self._worker_loop,
                    daemon=True,
                )
                t.start()
                self._threads.add(t)

    def _worker_loop(self) -> None:
        if self._initializer is not None:
            self._initializer(*self._initargs)
        while True:
            item = self._work_queue.get()
            if item is None:
                break
            f, fn, args, kwargs = item
            if not f.cancelled():
                try:
                    f.set_result(fn(*args, **kwargs))
                except Exception as e:
                    f.set_exception(e)

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        self._shutdown_event.set()
        # One sentinel per worker thread to unblock each _work_queue.get()
        for _ in self._threads:
            self._work_queue.put(None)
        if wait:
            deadline = time.time() + timeout if timeout is not None else None
            for t in self._threads:
                if deadline is not None:
                    remaining = max(0, deadline - time.time())
                    t.join(timeout=remaining)
                else:
                    t.join()


class AsyncProgressManagerWrapper(BaseExecutionProgressManager):
    """
    Async wrapper for progress managers that prevents terminal I/O from blocking
    the streaming executor scheduling loop.

    It enables to execute all progress manager operations in a background thread.
    """

    def __init__(
        self,
        wrapped_manager: BaseExecutionProgressManager,
        max_workers: int = 1,
        stall_warning_threshold: float = 10.0,
        shutdown_timeout: float = 5.0,
    ):
        self._wrapped = wrapped_manager
        self._stall_warning_threshold = stall_warning_threshold
        self._shutdown_timeout = shutdown_timeout
        self._stall_started_at: Optional[float] = None

        # ThreadPoolExecutor for async operations
        self._executor = _DaemonThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="async_progress",
        )

        # State Tracking
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()
        self._pending_futures: dict[str, concurrent.futures.Future] = {}

        # Stall detection
        self._last_successful_update = time.time()
        self._stall_warning_shown = False

        # Start monitoring thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_for_stalls, daemon=True, name="progress_stall_monitor"
        )
        self._monitor_thread.start()

        logger.debug("AsyncProgressManagerWrapper initialized")

    def start(self) -> None:
        """Non-blocking start operation."""
        if self._shutdown_event.is_set():
            return
        self._submit(self._wrapped.start)

    def refresh(self) -> None:
        """Non-blocking refresh operation."""
        if self._shutdown_event.is_set():
            return
        self._submit(self._wrapped.refresh)

    def close_with_finishing_description(self, desc: str, success: bool) -> None:
        """Close with finishing description (synchronous with timeout)."""
        logger.debug(
            f"Closing AsyncProgressManagerWrapper " f"(desc={desc}, success={success})"
        )

        with self._lock:
            self._shutdown_event.set()

        deadline = time.time() + self._shutdown_timeout

        # Wait for pending operations
        self._wait_for_pending_operations(timeout=max(0.0, deadline - time.time()))

        self._executor.shutdown(wait=True, timeout=max(0.0, deadline - time.time()))

        final_done = threading.Event()

        def _final_closer():
            try:
                self._safe_call(
                    self._wrapped.close_with_finishing_description, desc, success
                )
            finally:
                final_done.set()

        t = threading.Thread(
            target=_final_closer, daemon=True, name="async_progress_final_closer"
        )
        t.start()
        final_done.wait(timeout=max(0.0, deadline - time.time()))

        self._pending_futures.clear()

        logger.debug("AsyncProgressManagerWrapper closed")

    def update_total_progress(self, new_rows: int, total_rows: Optional[int]) -> None:
        """Non-blocking update of total progress."""
        if self._shutdown_event.is_set():
            return
        self._submit(self._wrapped.update_total_progress, new_rows, total_rows)

    def update_total_resource_status(self, resource_status: str) -> None:
        """Non-blocking update of resource status."""
        if self._shutdown_event.is_set():
            return
        self._submit(self._wrapped.update_total_resource_status, resource_status)

    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ) -> None:
        """Non-blocking update of operator progress."""
        if self._shutdown_event.is_set():
            return
        self._submit(self._wrapped.update_operator_progress, opstate, resource_manager)

    def _submit(self, method, *args, **kwargs):
        """Submit a method call to the background thread."""

        # For update_operator_progress, include operator ID in key
        if method.__name__ == "update_operator_progress" and args:
            opstate = args[0]
            # Use ID to create unique key per operator
            op_key = f"update_operator_progress:{id(opstate.op)}"
        else:
            # For other methods, use method name only
            op_key = method.__name__

        with self._lock:
            if self._shutdown_event.is_set():
                return

            replace = False

            if op_key in self._pending_futures:
                old_controller = self._pending_futures[op_key]
                if not old_controller.done():
                    old_controller.cancel()
                    replace = True

            controller = concurrent.futures.Future()

            def _worker_wrapper(ctrl_fut, m, *a, **kw):
                if ctrl_fut.cancelled():
                    return
                try:
                    res = self._timed_call(m, *a, **kw)
                    if not ctrl_fut.cancelled():
                        ctrl_fut.set_result(res)
                except Exception as e:
                    if not ctrl_fut.cancelled():
                        ctrl_fut.set_exception(e)

            # Submit worker to perform the actual method call.
            self._executor.submit(_worker_wrapper, controller, method, *args, **kwargs)
            self._pending_futures[op_key] = controller

        if replace:
            logger.debug(f"Replaced pending {op_key} with newer update")

    def _timed_call(self, method, *args, **kwargs):
        """Call method and track completion time."""
        start_time = time.time()
        method_name = getattr(method, "__name__", str(method))

        result = method(*args, **kwargs)
        duration = time.time() - start_time

        # Record successful update
        with self._lock:
            self._last_successful_update = time.time()

        if duration > 1.0:
            logger.debug(
                f"Progress operation took {duration:.2f}s " f"(method: {method_name})"
            )

        return result

    @staticmethod
    def _safe_call(method, *args, **kwargs):
        """Safely call a method, catching exceptions."""
        try:
            return method(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Progress operation failed: {e}")

    def _wait_for_pending_operations(self, timeout: float):
        """Wait for pending operations with timeout."""
        with self._lock:
            futures = list(self._pending_futures.values())
            num_pending = len(futures)

        if num_pending > 0:
            logger.debug(
                f"Waiting for {num_pending} pending "
                f"progress operations (timeout: {timeout}s)"
            )
            try:
                concurrent.futures.wait(
                    futures,
                    timeout=timeout,
                    return_when=concurrent.futures.ALL_COMPLETED,
                )
            except Exception as e:
                logger.debug(f"Error waiting for pending operations: {e}")

    def _monitor_for_stalls(self):
        """Background thread that monitors for stalled progress updates."""
        check_interval = 5.0

        logger.debug("Progress stall monitor thread started")

        while not self._shutdown_event.wait(timeout=check_interval):
            log_warning = None
            log_info = None

            with self._lock:
                time_since_update = time.time() - self._last_successful_update

                # If stalled and we haven't warned yet
                if (
                    time_since_update > self._stall_warning_threshold
                    and not self._stall_warning_shown
                ):
                    self._stall_started_at = time.time() - time_since_update
                    log_warning = (
                        f"Progress bar updates have not completed for "
                        f"{time_since_update:.1f} seconds. This usually "
                        f"indicates slow or blocked terminal I/O (e.g., slow "
                        f"SSH connection, frozen terminal, pipe buffer full). "
                        f"Your Ray Data job is still executing normally in the "
                        f"background. You can check ray-data.log for execution "
                        f"progress, or disable progress bars with: "
                        f"DataContext.get_current().enable_progress_bars = False"
                    )
                    self._stall_warning_shown = True

                # Reset warning if progress resumes
                elif time_since_update < check_interval and self._stall_warning_shown:
                    stall_duration = time.time() - self._stall_started_at
                    log_info = (
                        f"Progress bar updates have resumed after "
                        f"{stall_duration:.1f}s stall."
                    )
                    self._stall_started_at = None
                    self._stall_warning_shown = False

            if log_warning:
                logger.warning(log_warning)
            if log_info:
                logger.info(log_info)

    def __getattr__(self, name):
        """Delegate other attributes to wrapped manager."""
        return getattr(self._wrapped, name)

    def __repr__(self):
        return (
            f"AsyncProgressManagerWrapper("
            f"wrapped={self._wrapped.__class__.__name__})"
        )
