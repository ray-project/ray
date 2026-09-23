"""Refuse to unpickle untrusted bytes while a block of code runs.

Every unpickler (C ``pickle.loads``, ``pickle.Unpickler`` and its subclasses)
raises the ``pickle.find_class`` audit event before it resolves a class or
function. A process-wide audit hook refuses that event while the current thread
has the "forbid" flag set, so a pickle from untrusted bytes fails before any
object is built. Pickles of plain primitives never resolve a global and load.

Threat model: the attacker controls bytes, not code. This is not a sandbox for
hostile code already running in the process.

Ray's own transport (``ray.get``, task arguments, function loading) goes through
``ray.cloudpickle.loads``, which runs under :func:`allow_unsafe_unpickling`. The
flag is a context variable: it covers the thread that set it, not helper threads
a library starts.
"""

import contextvars
import os
import sys
import threading
from contextlib import contextmanager
from typing import Callable, Iterable, Iterator, TypeVar

from ray.util.annotations import DeveloperAPI

# Empty: unpickling allowed. Non-empty: forbidden; the text tells the user how to
# opt in and is appended to the error.
_forbid_flag: "contextvars.ContextVar[str]" = contextvars.ContextVar(
    "ray_forbid_unpickling", default=""
)
_hook_installed = False
_hook_lock = threading.Lock()

T = TypeVar("T")


@DeveloperAPI(stability="alpha")
class UntrustedUnpicklingError(ValueError):
    """Bytes from an untrusted source would have been unpickled."""


@DeveloperAPI(stability="alpha")
def is_unpickling_forbidden() -> bool:
    """Whether the current thread is inside :func:`forbid_untrusted_unpickling`."""
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


@DeveloperAPI(stability="alpha")
@contextmanager
def forbid_untrusted_unpickling(hint: str = ""):
    """Refuse unpickling on this thread until the block exits.

    ``hint`` is appended to the error to tell the user how to opt in.
    """
    _install_hook()
    token = _forbid_flag.set(hint or " ")
    try:
        yield
    finally:
        _forbid_flag.reset(token)


@DeveloperAPI(stability="alpha")
@contextmanager
def allow_unsafe_unpickling():
    """Lift :func:`forbid_untrusted_unpickling` for a call the caller vouches for."""
    token = _forbid_flag.set("")
    try:
        yield
    finally:
        _forbid_flag.reset(token)


@DeveloperAPI(stability="alpha")
def guard_iterator(
    get_iterable: Callable[[], Iterable[T]], hint: str = ""
) -> Iterator[T]:
    """Run ``get_iterable()`` and each ``next()`` on it under the forbid flag.

    A context variable set inside a generator stays set for its consumer between
    yields, so the flag is set around each ``next()`` instead. Whatever consumes
    the items (a fused downstream transform, for example) runs unguarded.
    """
    with forbid_untrusted_unpickling(hint):
        it = iter(get_iterable())
    while True:
        with forbid_untrusted_unpickling(hint):
            try:
                item = next(it)
            except StopIteration:
                return
        yield item
