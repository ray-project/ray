import threading
import traceback
from typing import Callable, Optional, TypeVar

from ray.train.v2._internal.exceptions import UserExceptionWithTraceback

T = TypeVar("T")


class ThreadRunner:
    """Utility to run a user function as a thread and capture its return value
    or exception.
    """

    def __init__(self):
        self._ret: Optional[T] = None
        self._exc: Optional[UserExceptionWithTraceback] = None

        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self._is_running = False

    def run(self, target: Callable[[], T]) -> None:
        if self._thread is not None:
            raise RuntimeError("Thread is already running.")

        def _run_target():
            with self._lock:
                self._is_running = True

            try:
                result = target()
                with self._lock:
                    self._ret = result
            except BaseException as e:
                with self._lock:
                    # Exclude the the first 2 frames from the traceback, which are
                    # the `ThreadRunner._run_target` and `construct_train_func` calls.
                    # TODO(justinvyu): This is brittle and may break if the call stack
                    # changes. Figure out a more robust way to exclude these frames.
                    exc_traceback_str = traceback.format_exc(
                        limit=-(len(traceback.extract_tb(e.__traceback__)) - 2)
                    )
                    self._exc = UserExceptionWithTraceback(
                        e, traceback_str=exc_traceback_str
                    )

            with self._lock:
                self._is_running = False

        self._thread = threading.Thread(target=_run_target, daemon=True)
        self._thread.start()

    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def get_error(self) -> Optional[BaseException]:
        with self._lock:
            return self._exc

    def get_return_value(self) -> Optional[T]:
        with self._lock:
            return self._ret

    def join(self, timeout: Optional[float] = None) -> T:
        if self._thread is None:
            raise RuntimeError("Must call `run` before trying to `join`.")

        self._thread.join(timeout=timeout)

        return self.get_return_value()
