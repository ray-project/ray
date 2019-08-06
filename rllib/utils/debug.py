from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pprint
import time

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch

_logged = set()
_disabled = False
_periodic_log = False
_last_logged = 0.0
_printer = pprint.PrettyPrinter(indent=2, width=60)


def log_once(key):
    """Returns True if this is the "first" call for a given key.

    Various logging settings can adjust the definition of "first".

    Example:
        >>> if log_once("some_key"):
        ...     logger.info("Some verbose logging statement")
    """

    global _last_logged

    if _disabled:
        return False
    elif key not in _logged:
        _logged.add(key)
        _last_logged = time.time()
        return True
    elif _periodic_log and time.time() - _last_logged > 60.0:
        _logged.clear()
        _last_logged = time.time()
        return False
    else:
        return False


def disable_log_once_globally():
    """Make log_once() return False in this process."""

    global _disabled
    _disabled = True


def enable_periodic_logging():
    """Make log_once() periodically return True in this process."""

    global _periodic_log
    _periodic_log = True


def summarize(obj):
    """Return a pretty-formatted string for an object.

    This has special handling for pretty-formatting of commonly used data types
    in RLlib, such as SampleBatch, numpy arrays, etc.
    """

    return _printer.pformat(_summarize(obj))


def _summarize(obj):
    if isinstance(obj, dict):
        return {k: _summarize(v) for k, v in obj.items()}
    elif hasattr(obj, "_asdict"):
        return {
            "type": obj.__class__.__name__,
            "data": _summarize(obj._asdict()),
        }
    elif isinstance(obj, list):
        return [_summarize(x) for x in obj]
    elif isinstance(obj, tuple):
        return tuple(_summarize(x) for x in obj)
    elif isinstance(obj, np.ndarray):
        if obj.size == 0:
            return _StringValue("np.ndarray({}, dtype={})".format(
                obj.shape, obj.dtype))
        elif obj.dtype == np.object:
            return _StringValue("np.ndarray({}, dtype={}, head={})".format(
                obj.shape, obj.dtype, _summarize(obj[0])))
        else:
            return _StringValue(
                "np.ndarray({}, dtype={}, min={}, max={}, mean={})".format(
                    obj.shape, obj.dtype, round(float(np.min(obj)), 3),
                    round(float(np.max(obj)), 3), round(
                        float(np.mean(obj)), 3)))
    elif isinstance(obj, MultiAgentBatch):
        return {
            "type": "MultiAgentBatch",
            "policy_batches": _summarize(obj.policy_batches),
            "count": obj.count,
        }
    elif isinstance(obj, SampleBatch):
        return {
            "type": "SampleBatch",
            "data": {k: _summarize(v)
                     for k, v in obj.items()},
        }
    else:
        return obj


class _StringValue(object):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return self.value
