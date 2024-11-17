"""Utils on retry."""

import time
from functools import wraps

# Default configuration for retry.
_DEFAULT_MAX_RETRY_COUNT = 10
_DEFAULT_INIT_DELAY_SEC = 1
_DEFAULT_MAX_DELAY_SEC = 30
_DEFAULT_BACKOFF = 2
_DEFAULT_JITTER_SEC = 1


def retry(
    max_retry_count=_DEFAULT_MAX_RETRY_COUNT,
    init_delay_sec=_DEFAULT_INIT_DELAY_SEC,
    max_delay_sec=_DEFAULT_MAX_DELAY_SEC,
    backoff=_DEFAULT_BACKOFF,
    jitter_sec=_DEFAULT_JITTER_SEC,
):
    def wrapper(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            for cur_retry_count in range(max_retry_count):
                try:
                    return fn(*args, **kwargs)
                except Exception:
                    if cur_retry_count + 1 == max_retry_count:
                        raise

                    sleep_sec = min(
                        init_delay_sec * (backoff**cur_retry_count) + jitter_sec,
                        max_delay_sec,
                    )
                    time.sleep(sleep_sec)

        return wrapped

    return wrapper
