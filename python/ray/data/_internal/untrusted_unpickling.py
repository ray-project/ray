"""Refuse to unpickle untrusted bytes while Ray Data datasource code runs.

Datasource code turns bytes Ray doesn't control into blocks. It runs with a
per-thread "forbid" flag set: on the driver (datasource construction, and
``get_read_tasks`` and ``estimate_inmemory_data_size`` called through
``Datasource``'s guarded entry points, and V2 schema sampling) and in read tasks
around each ``next()`` of a read function.

How it works: every unpickler (C ``pickle.loads``, ``pickle.Unpickler`` and its
subclasses, ``ray.cloudpickle``) raises the ``pickle.find_class`` audit event
before it resolves a class or function. A process-wide audit hook refuses that
event while the flag is set, so a pickle from untrusted bytes fails before any
object is built. Pickles of plain primitives never resolve a global and load.
While the flag is set, ``ArrowPythonObjectType`` also refuses to be rebuilt from
a file or stream.

The guard does not tell Ray's own bytes from external ones. Datasource code that
fetches Ray-produced objects (``ray.get`` of shared read state, for example)
wraps that ``ray.get`` in :func:`allow_unsafe_unpickling`. A reader that must
unpickle external bytes wraps only that call in it too, behind an explicit
opt-in.

Threat model: the attacker controls bytes, not code. The flag is a context
variable: it covers the thread that set it, not helper threads a library starts.

``RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1`` turns the guard off for the job.
"""

import contextvars
import os
import sys
import threading
from contextlib import contextmanager
from typing import Callable, Generator, Iterable, Iterator, TypeVar

from ray._common.utils import env_bool

T = TypeVar("T")

AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR = "RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR"

OPT_IN_HINT = (
    "If you trust the source of this data, use the reader's opt-in (for example "
    "`read_numpy(..., allow_pickle=True)`), or set the environment variable "
    f"{AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR}=1 on all nodes (e.g. via runtime_env)."
)

# Empty: unpickling allowed. Non-empty: forbidden; the text tells the user how to
# opt in and is appended to the error.
_forbid_flag: "contextvars.ContextVar[str]" = contextvars.ContextVar(
    "ray_data_forbid_unpickling", default=""
)
_hook_installed = False
_hook_lock = threading.Lock()


class UntrustedUnpicklingError(ValueError):
    """Bytes from an untrusted source would have been unpickled."""


def is_unpickling_forbidden() -> bool:
    """Whether the current thread is inside ``forbid_untrusted_unpickling()``."""
    return bool(_forbid_flag.get())


def _audit_hook(event: str, args: tuple) -> None:
    if event != "pickle.find_class":
        return
    hint = _forbid_flag.get()
    if not hint:
        return
    module, name = args
    msg = (
        f"Refusing to unpickle `{module}.{name}`: this data comes from an untrusted "
        f"source, and unpickling it can execute arbitrary code."
    )
    if hint.strip():
        msg = f"{msg} {hint.strip()}"
    raise UntrustedUnpicklingError(msg)


def _install_hook() -> None:
    global _hook_installed
    with _hook_lock:
        if not _hook_installed:
            # Process-wide and permanent: audit hooks cannot be removed.
            sys.addaudithook(_audit_hook)
            _hook_installed = True


if hasattr(os, "register_at_fork"):
    # A forked child is a new process; drop the parent's flag.
    os.register_at_fork(after_in_child=lambda: _forbid_flag.set(""))


def _enabled() -> bool:
    return not env_bool(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, False)


@contextmanager
def forbid_untrusted_unpickling(hint: str = OPT_IN_HINT):
    """Refuse unpickling on this thread until the block exits.

    ``hint`` is appended to the error to tell the user how to opt in. No-op when
    ``RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1``.
    """
    if not _enabled():
        yield
        return
    _install_hook()
    token = _forbid_flag.set(hint or " ")
    try:
        yield
    finally:
        _forbid_flag.reset(token)


@contextmanager
def allow_unsafe_unpickling():
    """Lift ``forbid_untrusted_unpickling()`` for a call the caller vouches for."""
    token = _forbid_flag.set("")
    try:
        yield
    finally:
        _forbid_flag.reset(token)


def guard_iterator(
    get_iterable: Callable[[], Iterable[T]], hint: str = OPT_IN_HINT
) -> Iterator[T]:
    """Run ``get_iterable()`` and each ``next()`` on it with the forbid flag set.

    A context variable set inside a generator stays set for its consumer between
    yields, so the flag is set around each ``next()`` instead. Whatever consumes
    the items (a fused downstream transform, for example) runs unguarded.
    """
    if not _enabled():
        return iter(get_iterable())
    return _guarded(get_iterable, hint)


def _guarded(
    get_iterable: Callable[[], Iterable[T]], hint: str
) -> Generator[T, None, None]:
    with forbid_untrusted_unpickling(hint):
        it = iter(get_iterable())
    try:
        while True:
            with forbid_untrusted_unpickling(hint):
                try:
                    item = next(it)
                except StopIteration:
                    return
            yield item
    finally:
        # On early exit, close the producer now (and guarded) rather than at GC.
        close = getattr(it, "close", None)
        if close is not None:
            with forbid_untrusted_unpickling(hint):
                close()
