"""Deprecated: use `ray.tune.Result` or `ray.train.Result` (removed in Ray 3.0).

`Result` now lives in Ray Tune. This module keeps `ray.air.result.Result` working,
with a warning, for user code and for old pickles that reference this path.
"""

from ray._common.deprecation import make_deprecated_getattr as _make_deprecated_getattr
from ray.air._deprecation import (
    REMOVAL_VERSION as _REMOVAL_VERSION,
    alias as _alias,
    tune_or_train as _tune_or_train,
)

# Kept out of the module globals so that `__getattr__` can warn on access.
_DEPRECATED_ALIASES = {
    "Result": _alias("ray.tune._result:Result", *_tune_or_train("Result")),
}

__getattr__ = _make_deprecated_getattr(__name__, _DEPRECATED_ALIASES, _REMOVAL_VERSION)


def __dir__():
    return sorted(set(globals()) | set(_DEPRECATED_ALIASES))
