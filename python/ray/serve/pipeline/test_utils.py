import os
from functools import wraps

import pytest


def test_inline_only():
    return os.environ.get("INLINE_ONLY") not in [None, "0"]


def enable_inline_only(f):
    """Convenience decorator to enable inline-only unit testing.

    This should be used to wrap pytest functions that take an "inline"
    parameter as their *first* argument. If inline-only testing is enabled,
    the inline=False case will be skipped.
    """

    @wraps(f)
    def wrapper(inline: bool, *args, **kwargs):
        if not inline and test_inline_only():
            pytest.skip("inline-only testing enabled")
        else:
            return f(inline, *args, **kwargs)

    return wrapper
