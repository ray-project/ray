from __future__ import absolute_import

import os
import logging
from pickle import PicklingError

from ray.cloudpickle.cloudpickle import *  # noqa
from ray.cloudpickle.cloudpickle_fast import CloudPickler, dumps, dump  # noqa

logger = logging.getLogger(__name__)

# Conform to the convention used by python serialization libraries, which
# expose their Pickler subclass at top-level under the  "Pickler" name.
Pickler = CloudPickler

__version__ = "1.6.0"


def _warn_msg(obj, method, exc):
    logger.warning(
        f"{method}({str(obj)}) failed due to: {exc}. "
        "\nTo check which non-serializable variables are captured "
        "in scope, re-run the ray script with 'RAY_PICKLE_VERBOSE_DEBUG=1'.")


def dump_debug(obj, *args, **kwargs):
    try:
        return dump(*args, **kwargs)
    except (TypeError, PicklingError) as exc:
        if os.environ.get("RAY_PICKLE_VERBOSE_DEBUG"):
            from ray.util.check_serialize import inspect_serializability
            inspect_serializability(obj)
            raise
        else:
            _warn_msg("ray.cloudpickle.dump", exc)
            raise


def dumps_debug(obj, *args, **kwargs):
    try:
        return dumps(*args, **kwargs)
    except (TypeError, PicklingError) as exc:
        if os.environ.get("RAY_PICKLE_VERBOSE_DEBUG"):
            from ray.util.check_serialize import inspect_serializability
            inspect_serializability(obj)
            raise
        else:
            _warn_msg("ray.cloudpickle.dumps", exc)
            raise
