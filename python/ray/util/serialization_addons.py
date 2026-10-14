"""
This module is intended for implementing internal serializers for some
site packages.
"""

import sys
from typing import Any

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def register_starlette_serializer(serialization_context):
    try:
        import starlette.datastructures
    except ImportError:
        return

    # Starlette's app.state object is not serializable
    # because it overrides __getattr__
    serialization_context._register_cloudpickle_serializer(
        starlette.datastructures.State,
        custom_serializer=lambda s: s._state,
        custom_deserializer=lambda s: starlette.datastructures.State(s),
    )


def _register_pydantic_serializers(serialization_context):
    from ray._common.pydantic_compat import register_pydantic_serializers

    register_pydantic_serializers(serialization_context)


def _register_arrow_serializers(serialization_context):
    from ray._private.arrow_serialization import (
        _register_custom_datasets_serializers,
    )

    _register_custom_datasets_serializers(serialization_context)


# Each library's serializers, by the module whose import brings them. They're
# registered once the process has imported the library (`apply_pending`), not
# at startup: a value of one of its types can't exist before, and importing a
# library just to register its serializers costs every process its import time
# and memory, and, for pyarrow, threads (its jemalloc's background thread, and
# the math threads of the numpy it imports).
_ADDONS = [
    ("pydantic", _register_pydantic_serializers),
    ("starlette", register_starlette_serializer),
]
if sys.platform != "win32":
    _ADDONS.append(("pyarrow", _register_arrow_serializers))


# The libraries whose serializers wait for the process to import them, and the
# context to register them on (`apply(..., lazily=True)`). Process-wide, like
# the registrations: whatever pickles a value (`SerializationContext`, or
# `pickle_dumps` for functions and classes) checks them first.
_pending = []
_pending_context = None


@DeveloperAPI
def apply(serialization_context: Any, lazily: bool = False) -> None:
    """Registers the libraries' serializers: of every one that's installed, or,
    `lazily`, of those the process has imported, and of the others once it
    imports them (whatever pickles a value calls `apply_pending` first).

    Args:
        serialization_context: The context to register them on.
        lazily: Whether to wait for each library to be imported.
    """
    global _pending, _pending_context
    if not lazily:
        # Each registration imports its library, if it's installed.
        for _, register in _ADDONS:
            register(serialization_context)
        return
    _pending_context = serialization_context
    _pending = list(_ADDONS)
    apply_pending()


def apply_pending() -> None:
    """Registers the serializers of the libraries the process has imported
    since: called before pickling a value. Once there's nothing left to
    register (or no library imported yet), it costs a check or two.

    Concurrent calls may both register a library, which is harmless: a
    registration replaces the same entries.
    """
    global _pending
    if not _pending:
        return
    remaining = []
    for module, register in _pending:
        if module in sys.modules:
            register(_pending_context)
        else:
            remaining.append((module, register))
    _pending = remaining
