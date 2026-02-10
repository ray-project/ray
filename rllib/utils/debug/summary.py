import pprint
from typing import Any

import numpy as np

from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI

_printer = pprint.PrettyPrinter(indent=2, width=60)


@DeveloperAPI
def summarize(obj: Any) -> Any:
    """Return a pretty-formatted string for an object.

    This has special handling for pretty-formatting of commonly used data types
    in RLlib, such as SampleBatch, numpy arrays, etc.

    Args:
        obj: The object to format.

    Returns:
        The summarized object.
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
            return _StringValue("np.ndarray({}, dtype={})".format(obj.shape, obj.dtype))
        elif obj.dtype == object or obj.dtype.type is np.str_:
            return _StringValue(
                "np.ndarray({}, dtype={}, head={})".format(
                    obj.shape, obj.dtype, _summarize(obj[0])
                )
            )
        else:
            return _StringValue(
                "np.ndarray({}, dtype={}, min={}, max={}, mean={})".format(
                    obj.shape,
                    obj.dtype,
                    round(float(np.min(obj)), 3),
                    round(float(np.max(obj)), 3),
                    round(float(np.mean(obj)), 3),
                )
            )
    elif isinstance(obj, MultiAgentBatch):
        return {
            "type": "MultiAgentBatch",
            "policy_batches": _summarize(obj.policy_batches),
            "count": obj.count,
        }
    elif isinstance(obj, SampleBatch):
        return {
            "type": "SampleBatch",
            "data": {k: _summarize(v) for k, v in obj.items()},
        }
    else:
        return obj


class _StringValue:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return self.value
