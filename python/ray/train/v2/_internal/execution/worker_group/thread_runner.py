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
        self._condition = threading.Condition()
        self._exc_queue = queue.SimpleQueue()
        self._monitor_thread: Optional[threading.Thread] = None

        self._is_running = False

    def run(self, target: Callable[[], T]) -> None:
        if self._thread is not None:
            raise RuntimeError("Thread is already running.")

        def _run_target():
            with self._condition:
                self._is_running = True

            try:
                result = target()
                with self._condition:
                    self._ret = result
            except BaseException as e:
                with self._condition:
                    # Exclude the first 2 frames from the traceback, which are
                    # the `ThreadRunner._run_target` and `construct_train_func` calls.
                    self._exc = construct_user_exception_with_traceback(
                        e, exclude_frames=2
                    )

            with self._condition:
                self._is_running = False
                self._condition.notify_all()

        self._thread = threading.Thread(
            target=_run_target,
            daemon=True,
            name=f"TrainingThread({get_callable_name(target)})",
        )
        self._thread.start()

        def _monitor_target():
            exc = self._exc_queue.get()
            with self._condition:
                self._exc = exc
                self._is_running = False
                self._condition.notify_all()

        self._monitor_thread = threading.Thread(
            target=_monitor_target,
            daemon=True,
            name=f"MonitoringThread({get_callable_name(target)})",
        )
        self._monitor_thread.start()

    def is_running(self) -> bool:
        with self._condition:
            return self._is_running

    def get_error(self) -> Optional[BaseException]:
        with self._condition:
            return self._exc

    def get_return_value(self) -> Optional[T]:
        with self._condition:
            return self._ret

    def get_exception_queue(self) -> queue.SimpleQueue:
        return self._exc_queue

    def join(self, timeout: Optional[float] = None) -> T:
        if self._thread is None:
            raise RuntimeError("Must call `run` before trying to `join`.")

        with self._condition:
            self._condition.wait_for(lambda: not self._is_running, timeout=timeout)

        return self.get_return_value()
