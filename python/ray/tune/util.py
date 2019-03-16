from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import base64
import copy
import numpy as np
import time

import ray

logger = logging.getLogger(__name__)

_pinned_objects = []
PINNED_OBJECT_PREFIX = "ray.tune.PinnedObject:"


def pin_in_object_store(obj):
    """Pin an object in the object store.

    It will be available as long as the pinning process is alive. The pinned
    object can be retrieved by calling get_pinned_object on the identifier
    returned by this call.
    """

    obj_id = ray.put(_to_pinnable(obj))
    _pinned_objects.append(ray.get(obj_id))
    return "{}{}".format(PINNED_OBJECT_PREFIX,
                         base64.b64encode(obj_id.binary()).decode("utf-8"))


def get_pinned_object(pinned_id):
    """Retrieve a pinned object from the object store."""

    from ray import ObjectID

    return _from_pinnable(
        ray.get(
            ObjectID(base64.b64decode(pinned_id[len(PINNED_OBJECT_PREFIX):]))))


class warn_if_slow(object):
    """Prints a warning if a given operation is slower than 100ms.

    Example:
        >>> with warn_if_slow("some_operation"):
        ...    ray.get(something)
    """

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, type, value, traceback):
        now = time.time()
        if now - self.start > 0.1:
            logger.warning("The `{}` operation took {} seconds to complete, ".
                           format(self.name, now - self.start) +
                           "which may be a performance bottleneck.")


def merge_dicts(d1, d2):
    """Returns a new dict that is d1 and d2 deep merged."""
    merged = copy.deepcopy(d1)
    deep_update(merged, d2, True, [])
    return merged


def deep_update(original, new_dict, new_keys_allowed, whitelist):
    """Updates original dict with values from new_dict recursively.
    If new key is introduced in new_dict, then if new_keys_allowed is not
    True, an error will be thrown. Further, for sub-dicts, if the key is
    in the whitelist, then new subkeys can be introduced.

    Args:
        original (dict): Dictionary with default values.
        new_dict (dict): Dictionary with values to be updated
        new_keys_allowed (bool): Whether new keys are allowed.
        whitelist (list): List of keys that correspond to dict values
            where new subkeys can be introduced. This is only at
            the top level.
    """
    for k, value in new_dict.items():
        if k not in original:
            if not new_keys_allowed:
                raise Exception("Unknown config parameter `{}` ".format(k))
        if isinstance(original.get(k), dict):
            if k in whitelist:
                deep_update(original[k], value, True, [])
            else:
                deep_update(original[k], value, new_keys_allowed, [])
        else:
            original[k] = value
    return original


def flatten_dict(dt):
    while any(isinstance(v, dict) for v in dt.values()):
        remove = []
        add = {}
        for key, value in dt.items():
            if isinstance(value, dict):
                for subkey, v in value.items():
                    add[":".join([key, subkey])] = v
                remove.append(key)
        dt.update(add)
        for k in remove:
            del dt[k]
    return dt


def _to_pinnable(obj):
    """Converts obj to a form that can be pinned in object store memory.

    Currently only numpy arrays are pinned in memory, if you have a strong
    reference to the array value.
    """

    return (obj, np.zeros(1))


def _from_pinnable(obj):
    """Retrieve from _to_pinnable format."""

    return obj[0]


if __name__ == "__main__":
    ray.init()
    X = pin_in_object_store("hello")
    print(X)
    result = get_pinned_object(X)
    print(result)
