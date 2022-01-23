import numpy as np
import os
import pprint
import random
from typing import Any, Mapping, Optional

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch

_printer = pprint.PrettyPrinter(indent=2, width=60)


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
    if isinstance(obj, Mapping):
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
        elif obj.dtype == object or obj.dtype.type is np.str_:
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


class _StringValue:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return self.value


def update_global_seed_if_necessary(framework: Optional[str] = None,
                                    seed: Optional[int] = None) -> None:
    """Seed global modules such as random, numpy, torch, or tf.

    This is useful for debugging and testing.

    Args:
        framework: The framework specifier (may be None).
        seed: An optional int seed. If None, will not do
            anything.
    """
    if seed is None:
        return

    # Python random module.
    random.seed(seed)
    # Numpy.
    np.random.seed(seed)

    # Torch.
    if framework == "torch":
        torch, _ = try_import_torch()
        torch.manual_seed(seed)
        # See https://github.com/pytorch/pytorch/issues/47672.
        cuda_version = torch.version.cuda
        if cuda_version is not None and float(torch.version.cuda) >= 10.2:
            os.environ["CUBLAS_WORKSPACE_CONFIG"] = "4096:8"
        else:
            from distutils.version import LooseVersion

            if LooseVersion(torch.__version__) >= LooseVersion("1.8.0"):
                # Not all Operations support this.
                torch.use_deterministic_algorithms(True)
            else:
                torch.set_deterministic(True)
        # This is only for Convolution no problem.
        torch.backends.cudnn.deterministic = True
    elif framework == "tf2" or framework == "tfe":
        tf1, tf, _ = try_import_tf()
        # Tf2.x.
        if framework == "tf2":
            tf.random.set_seed(seed)
        # Tf-eager.
        elif framework == "tfe":
            tf1.set_random_seed(seed)
