import os
import socket
from contextlib import closing
import logging
import queue
import threading
from typing import Optional

import numpy as np

from ray.air.constants import _ERROR_REPORT_TIMEOUT

logger = logging.getLogger(__name__)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def is_nan(value):
    return np.isnan(value)


def is_nan_or_inf(value):
    return is_nan(value) or np.isinf(value)


class StartTraceback(Exception):
    """These exceptions (and their tracebacks) can be skipped with `skip_exceptions`"""

    pass


def skip_exceptions(exc: Optional[Exception]) -> Exception:
    """Skip all contained `StartTracebacks` to reduce traceback output"""
    should_not_shorten = bool(int(os.environ.get("RAY_AIR_FULL_TRACEBACKS", "0")))

    if should_not_shorten:
        return exc

    if isinstance(exc, StartTraceback):
        # If this is a StartTraceback, skip
        return skip_exceptions(exc.__cause__)

    # Else, make sure nested exceptions are properly skipped
    cause = getattr(exc, "__cause__", None)
    if cause:
        exc.__cause__ = skip_exceptions(cause)

    return exc


class RunnerThread(threading.Thread):
    """Supervisor thread that runs your script."""

    def __init__(self, *args, error_queue, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self._error_queue = error_queue
        self._ret = None

    def run(self):
        try:
            self._ret = self._target(*self._args, **self._kwargs)
        except StopIteration:
            logger.debug(
                (
                    "Thread runner raised StopIteration. Interpreting it as a "
                    "signal to terminate the thread without error."
                )
            )
        except BaseException as e:
            try:
                # report the error but avoid indefinite blocking which would
                # prevent the exception from being propagated in the unlikely
                # case that something went terribly wrong
                self._error_queue.put(e, block=True, timeout=_ERROR_REPORT_TIMEOUT)
            except queue.Full:
                logger.critical(
                    (
                        "Runner Thread was unable to report error to main "
                        "function runner thread. This means a previous error "
                        "was not processed. This should never happen."
                    )
                )

    def join(self, timeout=None):
        super(RunnerThread, self).join(timeout)
        return self._ret
