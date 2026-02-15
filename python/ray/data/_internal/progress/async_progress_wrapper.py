import concurrent.futures
import logging
import threading
import time
import typing
import weakref
from concurrent.futures.thread import _worker
from typing import Optional

from ray.data._internal.progress.base_progress import BaseExecutionProgressManager

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)


class _DaemonThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    ThreadPoolExecutor that uses daemon threads.

    thread factory not compatible with older python versions
    """

    def _adjust_thread_count(self):
        """Override to create daemon worker threads instead of regular threads."""

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = f"{self._thread_name_prefix or ''}_{num_threads}"
            t = threading.Thread(
                name=thread_name,
                target=_worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
                daemon=True,
            )
            t.start()
            self._threads.add(t)
            concurrent.futures.ThreadPoolExecutor._adjust_thread_count(self)


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

        # ThreadPoolExecutor for async operations
        self._executor = _DaemonThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="async_progress",
        )

        # State Tracking
        self._shutdown = False
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
        if self._shutdown:
            return
        self._submit(self._wrapped.start)

    def refresh(self) -> None:
        """Non-blocking refresh operation."""
        if self._shutdown:
            return
        self._submit(self._wrapped.refresh)

    def close_with_finishing_description(self, desc: str, success: bool) -> None:
        """Close with finishing description (synchronous with timeout)."""
        logger.debug(
            f"Closing AsyncProgressManagerWrapper " f"(desc={desc}, success={success})"
        )

        with self._lock:
            self._shutdown = True

        # Wait for pending operations
        self._wait_for_pending_operations()

        # Try to show final message with timeout
        try:
            future = self._executor.submit(
                self._safe_call,
                self._wrapped.close_with_finishing_description,
                desc,
                success,
            )
            future.result(timeout=self._shutdown_timeout)
            logger.debug("Final progress message displayed successfully")
        except Exception as e:
            logger.debug(f"Error showing final progress message: {e}")

        # Shutdown executor
        self._executor.shutdown(wait=False)
        self._pending_futures.clear()

        logger.debug("AsyncProgressManagerWrapper closed")

    def update_total_progress(self, new_rows: int, total_rows: Optional[int]) -> None:
        """Non-blocking update of total progress."""
        if self._shutdown:
            return
        self._submit(self._wrapped.update_total_progress, new_rows, total_rows)

    def update_total_resource_status(self, resource_status: str) -> None:
        """Non-blocking update of resource status."""
        if self._shutdown:
            return
        self._submit(self._wrapped.update_total_resource_status, resource_status)

    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ) -> None:
        """Non-blocking update of operator progress."""
        if self._shutdown:
            return
        self._submit(self._wrapped.update_operator_progress, opstate, resource_manager)

    def _submit(self, method, *args, **kwargs):
        """Submit a method call to the background thread."""

        # For update_operator_progress, include operator ID in key
        if method.__name__ == "update_operator_progress" and args:
            opstate = args[0]
            # Use operator name/ID to create unique key per operator
            op_key = f"update_operator_progress:{opstate.op.name}"
        else:
            # For other methods, use method name only
            op_key = method.__name__

        with self._lock:
            if not self._shutdown:

                # Cancel old updates (fixes unbound issue)
                if op_key in self._pending_futures:
                    old_future = self._pending_futures[op_key]
                    if not old_future.done() and not old_future.running():
                        old_future.cancel()
                        logger.debug(f"Replaced pending {op_key} with newer update")

                future = self._executor.submit(
                    self._timed_call, method, *args, **kwargs
                )
                self._pending_futures[op_key] = future
            else:
                return

    def _timed_call(self, method, *args, **kwargs):
        """Call method and track completion time."""
        start_time = time.time()
        method_name = getattr(method, "__name__", str(method))

        try:
            result = method(*args, **kwargs)
            duration = time.time() - start_time

            # Record successful update
            with self._lock:
                self._last_successful_update = time.time()

            if duration > 1.0:
                logger.debug(
                    f"Progress operation took {duration:.2f}s "
                    f"(method: {method_name})"
                )

            return result
        except Exception as e:
            logger.debug(f"Progress operation failed (method: {method_name}): {e}")

    @staticmethod
    def _safe_call(method, *args, **kwargs):
        """Safely call a method, catching exceptions."""
        try:
            return method(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Progress operation failed: {e}")

    def _wait_for_pending_operations(self):
        """Wait for pending operations with timeout."""
        with self._lock:
            futures = list(self._pending_futures.values())
            num_pending = len(futures)

        if num_pending > 0:
            logger.debug(
                f"Waiting for {num_pending} pending "
                f"progress operations (timeout: {self._shutdown_timeout}s)"
            )
            try:
                concurrent.futures.wait(
                    futures,
                    timeout=self._shutdown_timeout,
                    return_when=concurrent.futures.ALL_COMPLETED,
                )
            except Exception as e:
                logger.debug(f"Error waiting for pending operations: {e}")

    def _monitor_for_stalls(self):
        """Background thread that monitors for stalled progress updates."""
        check_interval = 5.0

        logger.debug("Progress stall monitor thread started")

        while not self._shutdown:
            time.sleep(check_interval)

            with self._lock:
                time_since_update = time.time() - self._last_successful_update

                # If stalled and we haven't warned yet
                if (
                    time_since_update > self._stall_warning_threshold
                    and not self._stall_warning_shown
                ):

                    logger.warning(
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
                    logger.info(
                        f"Progress bar updates have resumed after "
                        f"{time_since_update:.1f}s stall."
                    )
                    self._stall_warning_shown = False

        logger.debug("Progress stall monitor thread stopped")

    def __getattr__(self, name):
        """Delegate other attributes to wrapped manager."""
        return getattr(self._wrapped, name)

    def __repr__(self):
        return (
            f"AsyncProgressManagerWrapper("
            f"wrapped={self._wrapped.__class__.__name__})"
        )
