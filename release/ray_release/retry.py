"""Utils on retry."""

import time
from functools import wraps
from typing import Tuple

# Default configuration for retry.
_DEFAULT_MAX_RETRY_COUNT: int = 10
_DEFAULT_INIT_DELAY_SEC: int = 1
_DEFAULT_MAX_DELAY_SEC: int = 30
_DEFAULT_BACKOFF: int = 2
_DEFAULT_JITTER_SEC: int = 1
_DEFAULT_EXCEPTIONS: Tuple[Exception] = (Exception,)


def retry(
    max_retry_count: int = _DEFAULT_MAX_RETRY_COUNT,
    init_delay_sec: int = _DEFAULT_INIT_DELAY_SEC,
    max_delay_sec: int = _DEFAULT_MAX_DELAY_SEC,
    backoff: int = _DEFAULT_BACKOFF,
    jitter_sec: int = _DEFAULT_JITTER_SEC,
    exceptions: Tuple[Exception] = _DEFAULT_EXCEPTIONS,
):
    def wrapper(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            for cur_retry_count in range(max_retry_count):
                try:
                    return fn(*args, **kwargs)
                except exceptions:
                    if cur_retry_count + 1 == max_retry_count:
                        raise

                    sleep_sec = min(
                        init_delay_sec * (backoff**cur_retry_count) + jitter_sec,
                        max_delay_sec,
                    )
                    time.sleep(sleep_sec)

        return wrapped

    return wrapper
