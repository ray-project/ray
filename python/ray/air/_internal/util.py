import os
import socket
from contextlib import closing
import logging
import queue
import shutil
import threading
from typing import Optional

import numpy as np
from pathlib import Path

import ray
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


def exception_cause(exc: Optional[Exception]) -> Optional[Exception]:
    if not exc:
        return None

    return getattr(exc, "__cause__", None)


class RunnerThread(threading.Thread):
    """Supervisor thread that runs your script."""

    def __init__(self, *args, error_queue, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self._error_queue = error_queue
        self._ret = None

    def _propagate_exception(self, e: BaseException):
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
        except SystemExit as e:
            # Do not propagate up for graceful termination.
            if e.code == 0:
                logger.debug(
                    (
                        "Thread runner raised SystemExit with error code 0. "
                        "Interpreting it as a signal to terminate the thread "
                        "without error."
                    )
                )
            else:
                # If non-zero exit code, then raise exception to main thread.
                self._propagate_exception(e)
        except BaseException as e:
            # Propagate all other exceptions to the main thread.
            self._propagate_exception(e)

    def join(self, timeout=None):
        super(RunnerThread, self).join(timeout)
        return self._ret


def _estimate_avail_object_store_memory() -> int:
    """Estimates total object store memory available in the cluster."""
    return ray.available_resources()["object_store_memory"]


def _copy_dir_ignore_conflicts(src_dir: Path, dst_dir: Path):
    """This is a workaround for python < 3.8 where shutil.copytree does not
    support dirs_exist_ok=True.

    We will go through the content of the folder and manually copy ites,
    while ignoring files that conflict.

    TODO(jungong): remove this workaround when we drop support for python < 3.8.
    """
    for inner in src_dir.iterdir():
        dest = dst_dir / inner.name
        if inner.is_dir():
            if not dest.exists():
                dest.mkdir(parents=True)
            _copy_dir_ignore_conflicts(inner, dest)
        else:
            if not dest.exists():
                shutil.copy2(str(inner.absolute()), str(dest.absolute()))
            else:
                # Ignore and don't overwrite the existing file.
                pass
