from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import numpy as np

import ray

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
                         base64.b64encode(obj_id.id()).decode("utf-8"))


def get_pinned_object(pinned_id):
    """Retrieve a pinned object from the object store."""

    from ray.local_scheduler import ObjectID

    return _from_pinnable(
        ray.get(
            ObjectID(base64.b64decode(pinned_id[len(PINNED_OBJECT_PREFIX):]))))


def _to_pinnable(obj):
    """Converts obj to a form that can be pinned in object store memory.

    Currently only numpy arrays are pinned in memory, if you have a strong
    reference to the array value.
    """

    return (obj, np.zeros(1))


def _from_pinnable(obj):
    """Retrieve from _to_pinnable format."""

    return obj[0]


if __name__ == '__main__':
    ray.init()
    X = pin_in_object_store("hello")
    print(X)
    result = get_pinned_object(X)
    print(result)
