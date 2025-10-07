import logging
import queue
import threading
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
        self._exc_queue: queue.SimpleQueue[Optional[Exception]] = queue.SimpleQueue()

    def run(self, target: Callable[[], T]) -> None:
        if self._thread is not None:
            raise RuntimeError("Thread is already running.")

        def _run_target():
            try:
                result = target()
                with self._lock:
                    self._ret = result
                self._exc_queue.put(None)
            except BaseException as e:
                # Exclude the first 3 frames from the traceback, which are
                # the `ThreadRunner._run_target`, `construct_train_func`, and
                # train_fn_with_final_checkpoint_flush calls.
                self._exc_queue.put(
                    construct_user_exception_with_traceback(e, exclude_frames=3)
                )

            # Join the monitor thread. This ensures that a queued exception
            # is processed before the target function is considered done.
            self._monitor_thread.join()

        self._monitor_thread = threading.Thread(
            target=self._monitor_target,
            daemon=True,
            name=f"MonitoringThread({get_callable_name(target)})",
        )
        self._monitor_thread.start()

        self._thread = threading.Thread(
            target=_run_target,
            daemon=True,
            name=f"TrainingThread({get_callable_name(target)})",
        )
        self._thread.start()

    def _monitor_target(self):
        """Monitor the exception queue and set the exception if an exception is found.

        This should run as a daemon thread and exit when None is put into the exception queue.
        """
        exc: Optional[UserExceptionWithTraceback] = self._exc_queue.get()
        if exc is None:
            return

        with self._lock:
            self._exc = exc

    def is_running(self) -> bool:
        """Returns whether the target function is still running."""
        return self._thread is not None and self._thread.is_alive()

    def get_error(self) -> Optional[BaseException]:
        with self._lock:
            return self._exc

    def get_return_value(self) -> Optional[T]:
        with self._lock:
            return self._ret

    def get_exception_queue(self) -> queue.SimpleQueue:
        """Returns a queue that nested threads can add exceptions to."""
        return self._exc_queue
