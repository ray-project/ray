import os
from functools import wraps

import pytest

from ray.serve.pipeline import ExecutionMode

LOCAL_EXECUTION_ONLY = os.environ.get("LOCAL_EXECUTION_ONLY") not in [None, "0"]


def enable_local_execution_mode_only(f):
    """Convenience decorator to enable local execution-only testing.

    This should be used to wrap pytest functions that take an "execution_mode"
    parameter as their *first* argument. If local-only testing is enabled,
    other cases will be skipped.
    """

    @wraps(f)
    def wrapper(execution_mode: ExecutionMode, *args, **kwargs):
        if execution_mode != ExecutionMode.LOCAL and LOCAL_EXECUTION_ONLY:
            pytest.skip("local execution-only testing enabled")
        else:
            return f(execution_mode, *args, **kwargs)

    return wrapper
