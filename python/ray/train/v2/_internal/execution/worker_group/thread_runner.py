import logging
import queue
import threading
import time
from typing import Callable, Optional, TypeVar

from ray.train.v2._internal.exceptions import UserExceptionWithTraceback
from ray.train.v2._internal.util import (
    construct_user_exception_with_traceback,
    get_callable_name,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


class ThreadRunner:
    """Utility to run a user function as a thread and capture its return value
    or exception.
    """

    def __init__(self):
        self._ret: Optional[T] = None
        self._exc: Optional[UserExceptionWithTraceback] = None

        self._thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._exc_queue = queue.SimpleQueue()

        self._is_running = False

    def run(self, target: Callable[[], T]) -> None:
        if self._thread is not None:
            raise RuntimeError("Thread is already running.")

        def _run_target():
            with self._lock:
                self._is_running = True

            has_exception = False
            try:
                result = target()
                with self._lock:
                    self._ret = result
            except BaseException as e:
                # Exclude the first 2 frames from the traceback, which are
                # the `ThreadRunner._run_target` and `construct_train_func` calls.
                self._exc_queue.put(
                    construct_user_exception_with_traceback(e, exclude_frames=2)
                )
                has_exception = True

            if not has_exception:
                self._exc_queue.put(None)

        self._thread = threading.Thread(
            target=_run_target,
            daemon=True,
            name=f"TrainingThread({get_callable_name(target)})",
        )
        self._thread.start()

        def _monitor_target():
            exc = self._exc_queue.get()
            with self._lock:
                self._exc = exc
                self._is_running = False

        self._monitor_thread = threading.Thread(
            target=_monitor_target,
            daemon=True,
            name=f"MonitoringThread({get_callable_name(target)})",
        )
        self._monitor_thread.start()

    def is_running(self) -> bool:
        """Returns False if we have a result or exception.

        If True, both _monitor_thread and _thread are running.
        If False, _monitor_thread is not running.
        If False, _thread might be running if it kicked off a nested thread
            that raised an exception. We do this because we want is_running()
            to be False ASAP so the Ray Train controller can process the result
            or exception ASAP.
        """
        with self._lock:
            return self._is_running

    def get_error(self) -> Optional[BaseException]:
        with self._lock:
            return self._exc

    def get_return_value(self) -> Optional[T]:
        with self._lock:
            return self._ret

    def get_exception_queue(self) -> queue.SimpleQueue:
        return self._exc_queue

    def join(self, timeout: Optional[float] = None) -> T:
        """Join both the target thread and the monitor thread.

        Note that unless the target function has some special exiting logic,
        if it creates a thread that raises an exception, the target function
        will not exit, and join will hang.

        However, calling join is optional. Because the target function is run
        as a daemon thread, the controller can shut down the worker even if
        the target function is still running.
        """
        if self._monitor_thread is None or self._thread is None:
            raise RuntimeError("Must call `run` before trying to `join`.")

        start_time = time.time()
        self._monitor_thread.join(timeout=timeout)
        elapsed_time = time.time() - start_time
        remaining_timeout = timeout - elapsed_time if timeout else None
        self._thread.join(timeout=remaining_timeout)

        return self.get_return_value()
