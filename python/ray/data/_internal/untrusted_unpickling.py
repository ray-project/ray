"""Ray Data's use of the untrusted-unpickling guard.

Datasource code turns external bytes into blocks, so it runs with the forbid
flag set: on the driver (constructor, ``get_read_tasks``,
``estimate_inmemory_data_size``, V2 schema sampling) and in read tasks around
each ``next()`` of a read function. While the flag is set, any unpickle that
names a class or function raises, and ``ArrowPythonObjectType`` refuses to be
rebuilt from a file or stream. Readers that must unpickle wrap only that call
in :func:`allow_unsafe_unpickling` behind an explicit opt-in.

``RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1`` turns the guard off for the job.
"""

import functools
from contextlib import contextmanager
from typing import Callable, Iterable, Iterator, TypeVar

from ray._common.utils import env_bool
from ray.util import pickle_guard as _guard
from ray.util.pickle_guard import (  # noqa: F401  (re-exported for datasources)
    UntrustedUnpicklingError,
    allow_unsafe_unpickling,
    is_unpickling_forbidden,
)

T = TypeVar("T")

AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR = "RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR"

OPT_IN_HINT = (
    "If you trust the source of this data, use the reader's opt-in (for example "
    "`read_numpy(..., allow_pickle=True)`), or set the environment variable "
    f"{AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR}=1 on all nodes (e.g. via runtime_env)."
)


def _enabled() -> bool:
    return not env_bool(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, False)


@contextmanager
def forbid_untrusted_unpickling():
    """Set the forbid flag on this thread until the block exits."""
    if not _enabled():
        yield
        return
    with _guard.forbid_untrusted_unpickling(OPT_IN_HINT):
        yield


def guard_iterator(get_iterable: Callable[[], Iterable[T]]) -> Iterator[T]:
    """Run ``get_iterable()`` and each ``next()`` on it with the forbid flag set."""
    if not _enabled():
        return iter(get_iterable())
    return _guard.guard_iterator(get_iterable, OPT_IN_HINT)


def guard_datasource_call(fn: Callable) -> Callable:
    """Wrap a datasource method so it runs with the forbid flag set. Idempotent."""
    if getattr(fn, "_ray_unpickling_guarded", False):
        return fn

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with forbid_untrusted_unpickling():
            return fn(*args, **kwargs)

    wrapper._ray_unpickling_guarded = True
    return wrapper
