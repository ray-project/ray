from __future__ import absolute_import

import os
from pickle import PicklingError

from ray.cloudpickle.cloudpickle import *  # noqa
from ray.cloudpickle.cloudpickle_fast import CloudPickler, dumps, dump  # noqa


# Conform to the convention used by python serialization libraries, which
# expose their Pickler subclass at top-level under the  "Pickler" name.
Pickler = CloudPickler

__version__ = '3.0.0'


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
