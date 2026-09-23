from __future__ import absolute_import

import os
from pickle import PicklingError

from ray.cloudpickle.cloudpickle import *  # noqa
from ray.cloudpickle.cloudpickle import load as _pickle_load, loads as _pickle_loads
from ray.cloudpickle.cloudpickle_fast import CloudPickler, dumps, dump  # noqa

_allow_unsafe_unpickling = None


def _exempt_from_guard():
    # Imported lazily: this module loads during ``import ray``, before
    # ``ray.util`` can be imported.
    global _allow_unsafe_unpickling
    if _allow_unsafe_unpickling is None:
        from ray.util.pickle_guard import allow_unsafe_unpickling

        _allow_unsafe_unpickling = allow_unsafe_unpickling
    return _allow_unsafe_unpickling()


# Conform to the convention used by python serialization libraries, which
# expose their Pickler subclass at top-level under the  "Pickler" name.
Pickler = CloudPickler


def loads(data, /, **kwargs):
    """Unpickle bytes Ray itself produced.

    Exempt from the untrusted-unpickling guard (``ray.util.pickle_guard``).
    Never call this on bytes from a file or service; use the stdlib ``pickle``.
    """
    with _exempt_from_guard():
        return _pickle_loads(data, **kwargs)


def load(file, /, **kwargs):
    """File-object counterpart of :func:`loads`; see its note on trust."""
    with _exempt_from_guard():
        return _pickle_load(file, **kwargs)


__version__ = '3.1.2'


def _warn_msg(obj, method, exc):
    return (
        f"{method}({str(obj)}) failed."
        "\nTo check which non-serializable variables are captured "
        "in scope, re-run the ray script with 'RAY_PICKLE_VERBOSE_DEBUG=1'.")


def dump_debug(obj, *args, **kwargs):
    try:
        return dump(obj, *args, **kwargs)
    except (TypeError, PicklingError) as exc:
        if os.environ.get("RAY_PICKLE_VERBOSE_DEBUG"):
            from ray.util.check_serialize import inspect_serializability
            inspect_serializability(obj)
            raise
        else:
            msg = _warn_msg(obj, "ray.cloudpickle.dump", exc)
            raise type(exc)(msg)


def dumps_debug(obj, *args, **kwargs):
    try:
        return dumps(obj, *args, **kwargs)
    except (TypeError, PicklingError) as exc:
        if os.environ.get("RAY_PICKLE_VERBOSE_DEBUG"):
            from ray.util.check_serialize import inspect_serializability
            inspect_serializability(obj)
            raise
        else:
            msg = _warn_msg(obj, "ray.cloudpickle.dumps", exc)
            raise type(exc)(msg)
