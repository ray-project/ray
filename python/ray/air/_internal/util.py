import os
import socket
from contextlib import closing
from typing import Optional

import numpy as np


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

    @property
    def __traceback__(self):
        return self.__cause__.__traceback__


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
